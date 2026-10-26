use anyhow::{Context, Result, ensure};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    WebPkiSupportedAlgorithms, ring, verify_tls12_signature, verify_tls13_signature,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, SignatureScheme};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug)]
struct Fingerprint {
    digest: [u8; 32],
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Fingerprint {
    fn verify_server_cert(
        &self,
        certificate: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, Error> {
        if Sha256::digest(certificate.as_ref()).as_slice() != self.digest {
            return Err(Error::General(
                "Peer certificate does not match its advertised SHA-256 fingerprint".to_owned(),
            ));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

pub fn configuration(fingerprint: &str) -> Result<ClientConfig> {
    let fingerprint = fingerprint.replace(':', "");
    ensure!(
        fingerprint.len() == 64,
        "Peer TLS fingerprint must contain a complete SHA-256 digest"
    );
    let digest: [u8; 32] = hex::decode(fingerprint)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Peer TLS fingerprint has an invalid length"))?;
    let provider = ring::default_provider();
    let verifier = Fingerprint {
        digest,
        algorithms: provider.signature_verification_algorithms,
    };
    Ok(ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .context("Peer TLS protocols are unavailable")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth())
}
