use crate::http::ConnectionContext;
use anyhow::{Context, Result};
use axum::{Extension, Router};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

const HEADER_DEADLINE: Duration = Duration::from_secs(5);
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const DEFAULT_MAXIMUM_CONNECTIONS: usize = 1_024;

#[derive(Clone)]
pub struct ListenerOptions {
    pub trusted_proxies: Arc<[IpAddr]>,
    pub maximum_connections: usize,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            trusted_proxies: Arc::from([]),
            maximum_connections: DEFAULT_MAXIMUM_CONNECTIONS,
        }
    }
}

pub struct Listener {
    socket: TcpListener,
    encrypted: Option<TlsAcceptor>,
    options: ListenerOptions,
}

impl Listener {
    pub async fn bind(
        address: SocketAddr,
        encrypted: Option<TlsAcceptor>,
        options: ListenerOptions,
    ) -> Result<Self> {
        anyhow::ensure!(
            options.maximum_connections > 0,
            "maximum connections must be positive"
        );
        let socket = TcpListener::bind(address)
            .await
            .with_context(|| format!("bind HTTP listener at {address}"))?;

        Ok(Self {
            socket,
            encrypted,
            options,
        })
    }

    pub fn local_address(&self) -> Result<SocketAddr> {
        self.socket
            .local_addr()
            .context("read HTTP listener address")
    }

    pub async fn serve(self, router: Router, shutdown: CancellationToken) -> Result<()> {
        let address = self.local_address()?;
        let connections = Arc::new(Semaphore::new(self.options.maximum_connections));
        let mut tasks = JoinSet::new();
        let lifetime = shutdown.child_token();
        let _cancel_on_drop = lifetime.clone().drop_guard();
        let outcome = loop {
            tokio::select! {
                biased;

                _ = lifetime.cancelled() => break Ok(()),
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = completed {
                        break Err(anyhow::anyhow!("HTTP connection task failed: {error}"));
                    }
                }
                accepted = self.socket.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => break Err(error).context("accept HTTP connection"),
                    };
                    let permit = match Arc::clone(&connections).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(%peer, "HTTP connection limit reached");
                            continue;
                        }
                    };

                    socket.set_nodelay(true).context("configure HTTP socket")?;
                    let context = ConnectionContext {
                        peer,
                        encrypted: self.encrypted.is_some(),
                        listener_port: address.port(),
                        trusted_proxies: Arc::clone(&self.options.trusted_proxies),
                    };
                    let router = router.clone();
                    let encrypted = self.encrypted.clone();
                    let shutdown = lifetime.clone();
                    tasks.spawn(async move {
                        let _permit = permit;

                        if let Some(encrypted) = encrypted {
                            let handshake = tokio::select! {
                                _ = shutdown.cancelled() => return,
                                result = tokio::time::timeout(HANDSHAKE_DEADLINE, encrypted.accept(socket)) => result,
                            };

                            match handshake {
                                Ok(Ok(socket)) => serve_connection(socket, router, context, shutdown).await,
                                Ok(Err(error)) => tracing::debug!(%peer, %error, "TLS handshake failed"),
                                Err(_) => tracing::debug!(%peer, "TLS handshake deadline exceeded"),
                            }
                        } else {
                            serve_connection(socket, router, context, shutdown).await;
                        }
                    });
                }
            }
        };

        drop(self.socket);
        lifetime.cancel();

        if tokio::time::timeout(SHUTDOWN_DEADLINE, async {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    tracing::error!(%error, "HTTP connection failed during shutdown");
                }
            }
        })
        .await
        .is_err()
        {
            tracing::warn!("HTTP shutdown deadline exceeded; closing remaining connections");
            tasks.shutdown().await;
        }

        outcome
    }
}

async fn serve_connection<Socket>(
    socket: Socket,
    router: Router,
    context: ConnectionContext,
    shutdown: CancellationToken,
) where
    Socket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_DEADLINE);
    let service = tower::ServiceBuilder::new()
        .layer(Extension(context))
        .service(router);
    let connection =
        builder.serve_connection(TokioIo::new(socket), TowerToHyperService::new(service));
    tokio::pin!(connection);

    let result = tokio::select! {
        result = &mut connection => result,
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };

    if let Err(error) = result {
        tracing::debug!(%error, "HTTP connection ended");
    }
}
