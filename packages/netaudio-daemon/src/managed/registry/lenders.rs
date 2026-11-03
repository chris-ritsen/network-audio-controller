use super::{Registry, Source, State};
use crate::managed::{Error, inventory::InventoryState, sessions::Session};
use std::sync::{Arc, Weak};
use tokio::sync::Semaphore;

impl Registry {
    pub async fn lend(&self, server: &str, session: &Arc<Session>) -> Result<(), Error> {
        let changed = {
            let mut state = self.lock()?;
            let lenders = state.lenders.entry(server.to_owned()).or_default();
            if !lenders.iter().any(|lender| same(lender, session)) {
                lenders.push(Arc::downgrade(session));
            }
            self.reassign_locked(&mut state, vec![server.to_owned()], Some(session))?
        };
        self.refresh_borrowed(changed).await;
        Ok(())
    }

    pub async fn choose_lender(&self, server: &str) -> Result<(), Error> {
        let changed = {
            let mut state = self.lock()?;
            self.reassign_locked(&mut state, vec![server.to_owned()], None)?
        };
        self.refresh_borrowed(changed).await;
        Ok(())
    }

    pub(super) fn reassign_expired(&self) -> Result<Vec<String>, Error> {
        let mut state = self.lock()?;
        let servers = state.servers.keys().cloned().collect();
        self.reassign_locked(&mut state, servers, None)
    }

    pub(super) fn reassign_locked(
        &self,
        state: &mut State,
        servers: Vec<String>,
        renewed: Option<&Arc<Session>>,
    ) -> Result<Vec<String>, Error> {
        let changed: Vec<_> = servers
            .into_iter()
            .filter(|server| reassign(state, server, renewed))
            .collect();
        if !changed.is_empty() {
            self.publish_locked(state)
                .map_err(|_| Error::Conflict("Managed inventory could not be published"))?;
            self.changed.notify_one();
            self.signal_changes.send_replace(());
        }
        Ok(changed)
    }

    pub(super) async fn refresh_borrowed(&self, servers: Vec<String>) {
        for server in servers {
            if let Err(error) = self.refresh_server(&server).await {
                tracing::warn!(%error, server, "managed inventory refresh with a browser's DDM login failed");
            }
        }
    }
}

fn reassign(state: &mut State, server: &str, renewed: Option<&Arc<Session>>) -> bool {
    let State {
        configuration_error,
        lenders,
        servers,
        ..
    } = state;
    let Some(entry) = servers.get_mut(server) else {
        lenders.remove(server);
        return false;
    };
    let url = entry.source.configuration.url.clone();
    let candidates = lenders.entry(server.to_owned()).or_default();
    candidates.retain(|lender| {
        lender.upgrade().is_some_and(|session| {
            session.active()
                && session
                    .login(server)
                    .is_some_and(|login| login.client.serves(url.as_deref()))
        })
    });
    if candidates.is_empty() {
        lenders.remove(server);
    }
    if configuration_error.is_some() || entry.source.configuration.enabled() {
        return false;
    }
    let current = entry.source.lender.as_ref().and_then(Weak::upgrade);
    let chosen = lenders
        .get(server)
        .into_iter()
        .flatten()
        .filter_map(Weak::upgrade)
        .filter_map(|session| session.login(server).map(|login| (session, login)))
        .max_by_key(|(session, login)| {
            (
                login.domains.len(),
                current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, session)),
            )
        });
    let unchanged = match (&chosen, &entry.source.lender) {
        (Some((session, _)), Some(lender)) => {
            same(lender, session) && renewed.is_none_or(|renewed| !Arc::ptr_eq(renewed, session))
        }
        (None, None) => true,
        _ => false,
    };
    if unchanged {
        return false;
    }
    let base = &entry.source;
    let source = match chosen {
        Some((session, login)) => Source {
            client: Some(login.client),
            configuration: base.configuration.clone(),
            credential: Some(login.credential),
            identity: Arc::new(()),
            lender: Some(Arc::downgrade(&session)),
            refresh: Semaphore::new(1),
            resource_paths: base.resource_paths.clone(),
            resource_revision: base.resource_revision,
            setup_error: None,
            transport: Some(login.transport),
        },
        None => Source {
            client: None,
            configuration: base.configuration.clone(),
            credential: None,
            identity: Arc::new(()),
            lender: None,
            refresh: Semaphore::new(1),
            resource_paths: base.resource_paths.clone(),
            resource_revision: base.resource_revision,
            setup_error: None,
            transport: None,
        },
    };
    entry.source = Arc::new(source);
    entry.inventory = InventoryState::default();
    entry.signals.clear();
    entry.controller_status = crate::managed::status::observations::Cache::default();
    true
}

fn same(lender: &Weak<Session>, session: &Arc<Session>) -> bool {
    std::ptr::eq(lender.as_ptr(), Arc::as_ptr(session))
}
