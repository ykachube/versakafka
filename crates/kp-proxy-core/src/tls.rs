//! TLS handling

use crate::{ProxyCoreError, Result};
use rustls::{ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;
use std::fs;
use std::io::BufReader;

pub fn build_tls_config(config: &super::config::TlsConfig) -> Result<Arc<ServerConfig>> {
    let cert_file = fs::File::open(&config.cert_file)
        .map_err(|e| ProxyCoreError::Io(e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain: std::result::Result<Vec<CertificateDer<'static>>, _> = rustls_pemfile::certs(&mut cert_reader).collect();
    let cert_chain = cert_chain
        .map_err(|e| ProxyCoreError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    
    let key_file = fs::File::open(&config.key_file)
        .map_err(|e| ProxyCoreError::Io(e))?;
    let mut key_reader = BufReader::new(key_file);
    let key_der = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| ProxyCoreError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
        .ok_or_else(|| ProxyCoreError::Tls(rustls::Error::General("No private key found".into())))?;
    
    let key = match key_der {
        PrivateKeyDer::Pkcs8(key) => PrivateKeyDer::Pkcs8(key),
        PrivateKeyDer::Pkcs1(key) => PrivateKeyDer::Pkcs1(key),
        PrivateKeyDer::Sec1(key) => PrivateKeyDer::Sec1(key),
        _ => return Err(ProxyCoreError::Tls(rustls::Error::General("Unsupported key format".into()))),
    };
    
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| ProxyCoreError::Tls(e))?;
    
    Ok(Arc::new(server_config))
}