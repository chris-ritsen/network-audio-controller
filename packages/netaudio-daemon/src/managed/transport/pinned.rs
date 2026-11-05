use super::Error;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, verify_tls12_signature, verify_tls13_signature,
    verify_tls13_signature_with_raw_key,
};
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use x509_parser::prelude::{ASN1Time, FromDer, X509Certificate};

#[derive(Debug)]
pub(super) struct Verifier {
    certificates: Vec<CertificateDer<'static>>,
    pin: Option<[u8; 32]>,
    provider: Arc<CryptoProvider>,
    verifier: Arc<WebPkiServerVerifier>,
}

impl Verifier {
    pub(super) fn new(
        certificates: Vec<CertificateDer<'static>>,
        pin: Option<[u8; 32]>,
        roots: RootCertStore,
        provider: Arc<CryptoProvider>,
    ) -> Result<Self, Error> {
        let verifier =
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                .build()
                .map_err(|_| Error::Tls("Managed service TLS configuration failed"))?;
        Ok(Self {
            certificates,
            pin,
            provider,
            verifier,
        })
    }

    fn pinned(&self, certificate: &CertificateDer<'_>) -> bool {
        self.pin
            .is_some_and(|pin| Sha256::digest(certificate.as_ref()).as_slice() == pin)
    }

    fn trusted(&self, certificate: &CertificateDer<'_>) -> bool {
        self.certificates
            .iter()
            .any(|trusted| trusted.as_ref() == certificate.as_ref())
    }
}

fn bad_encoding() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::BadEncoding)
}

fn public_key(certificate: &CertificateDer<'_>) -> Result<Vec<u8>, rustls::Error> {
    let (_, parsed) =
        X509Certificate::from_der(certificate.as_ref()).map_err(|_| bad_encoding())?;
    Ok(parsed.tbs_certificate.subject_pki.raw.to_vec())
}

fn bad_signature() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::BadSignature)
}

fn verify_trusted(
    certificate: &CertificateDer<'_>,
    server_name: &ServerName<'_>,
    now: UnixTime,
) -> Result<ServerCertVerified, rustls::Error> {
    webpki::EndEntityCert::try_from(certificate)
        .map_err(|_| bad_encoding())?
        .verify_is_valid_for_subject_name(server_name)
        .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::NotValidForName))?;
    let (_, parsed) =
        X509Certificate::from_der(certificate.as_ref()).map_err(|_| bad_encoding())?;
    let instant = i64::try_from(now.as_secs())
        .ok()
        .and_then(|seconds| ASN1Time::from_timestamp(seconds).ok())
        .ok_or_else(bad_encoding)?;
    if !parsed.validity().is_valid_at(instant) {
        return Err(rustls::Error::InvalidCertificate(CertificateError::Expired));
    }
    Ok(ServerCertVerified::assertion())
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.pinned(end_entity) {
            return Ok(ServerCertVerified::assertion());
        }
        match self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(_) if self.trusted(end_entity) => verify_trusted(end_entity, server_name, now),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let algorithms = &self.provider.signature_verification_algorithms;
        if !self.pinned(certificate) {
            return verify_tls12_signature(message, certificate, signature, algorithms);
        }
        let key = public_key(certificate)?;
        let key = SubjectPublicKeyInfoDer::from(key.as_slice());
        let entity = webpki::RawPublicKeyEntity::try_from(&key).map_err(|_| bad_encoding())?;
        let candidates = algorithms
            .mapping
            .iter()
            .find(|(scheme, _)| *scheme == signature.scheme)
            .map(|(_, candidates)| *candidates)
            .ok_or_else(bad_signature)?;
        for algorithm in candidates {
            match entity.verify_signature(*algorithm, message, signature.signature()) {
                Ok(()) => return Ok(HandshakeSignatureValid::assertion()),
                Err(webpki::Error::UnsupportedSignatureAlgorithmForPublicKeyContext(_)) => {}
                Err(_) => return Err(bad_signature()),
            }
        }
        Err(bad_signature())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let algorithms = &self.provider.signature_verification_algorithms;
        if !self.pinned(certificate) {
            return verify_tls13_signature(message, certificate, signature, algorithms);
        }
        let key = public_key(certificate)?;
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(key.as_slice()),
            signature,
            algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
