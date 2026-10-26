use anyhow::{Context, Result, ensure};
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

const WEB_ROOT: &str = "../netaudio/src/netaudio/daemon/http/webapp";
const ENTRY_DOCUMENT: &str = "index.html";

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed={WEB_ROOT}");
    println!("cargo:rerun-if-env-changed=NETAUDIO_GIT_REVISION");
    let output = PathBuf::from(env::var_os("OUT_DIR").context("missing build output directory")?);
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").context("missing crate directory")?)
        .join(WEB_ROOT);
    let workspace =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").context("missing crate directory")?)
            .join("../..");
    let mut source_digest = Sha256::new();
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "packages/netaudio-core/Cargo.toml",
        "packages/netaudio-core/src",
        "packages/netaudio-daemon/Cargo.toml",
        "packages/netaudio-daemon/build.rs",
        "packages/netaudio-daemon/src",
        "packages/netaudio-daemon/assets",
    ] {
        let path = workspace.join(path);
        println!("cargo:rerun-if-changed={}", path.display());
        fingerprint(&workspace, &path, &mut source_digest)?;
    }
    let mut files = BTreeMap::new();
    collect(&root, &root, &mut files)?;
    let mut digest = Sha256::new();
    digest.update(include_bytes!("build.rs"));

    for (path, body) in &files {
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update((body.len() as u64).to_be_bytes());
        digest.update(body);
    }

    let generation = format!("{:x}", digest.finalize());
    source_digest.update(generation.as_bytes());
    println!(
        "cargo:rustc-env=NETAUDIO_BUILD_ID={:x}",
        source_digest.finalize()
    );
    let prefix = format!("/_assets/{generation}");
    let mut document = String::from_utf8(
        files
            .remove(ENTRY_DOCUMENT)
            .context("web entry document is absent")?,
    )?;

    for path in files.keys() {
        document = document.replace(&format!("\"/{path}\""), &format!("\"{prefix}/{path}\""));
        document = document.replace(&format!("\"/{path}?"), &format!("\"{prefix}/{path}?"));
    }

    files.insert(ENTRY_DOCUMENT.to_owned(), document.into_bytes());
    let mut generated = format!("pub const GENERATION: &str = {generation:?};\n");
    generated.push_str("static ASSETS: &[EmbeddedAsset] = &[\n");

    for (index, (path, body)) in files.iter().enumerate() {
        let identity = output.join(format!("web-{index}"));
        let compressed = output.join(format!("web-{index}.gz"));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(body)?;
        let encoded = encoder.finish()?;
        fs::write(&identity, body)?;
        fs::write(&compressed, &encoded)?;
        let identity_digest = format!("{:x}", Sha256::digest(body));
        let compressed_digest = format!("{:x}", Sha256::digest(&encoded));
        let content_type = match Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
        {
            Some("html") => "text/html; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            Some("svg") => "image/svg+xml",
            Some("wasm") => "application/wasm",
            _ => unreachable!("unsupported web asset passed collection"),
        };
        writeln!(
            generated,
            "EmbeddedAsset {{ path: {path:?}, content_type: {content_type:?}, \
             identity: include_bytes!({identity:?}), compressed: include_bytes!({compressed:?}), \
             identity_digest: {identity_digest:?}, compressed_digest: {compressed_digest:?} }},"
        )?;
    }

    generated.push_str("];\n");
    fs::write(output.join("web_assets.rs"), generated)?;
    Ok(())
}

fn fingerprint(root: &Path, path: &Path, digest: &mut Sha256) -> Result<()> {
    if path.is_dir() {
        let mut children = fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        children.sort();
        for child in children {
            fingerprint(root, &child, digest)?;
        }
    } else {
        let name = path
            .strip_prefix(root)?
            .to_string_lossy()
            .replace('\\', "/");
        let content = fs::read(path)?;
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update((content.len() as u64).to_be_bytes());
        digest.update(content);
    }
    Ok(())
}

fn collect(root: &Path, directory: &Path, files: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        ensure!(!kind.is_symlink(), "web assets cannot contain symlinks");

        if kind.is_dir() {
            collect(root, &path, files)?;
        } else if kind.is_file()
            && matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("html" | "css" | "js" | "svg" | "wasm")
            )
        {
            let relative = path
                .strip_prefix(root)?
                .components()
                .map(|component| {
                    component
                        .as_os_str()
                        .to_str()
                        .context("web path is not UTF-8")
                })
                .collect::<Result<Vec<_>>>()?
                .join("/");
            files.insert(relative, fs::read(path)?);
        }
    }

    Ok(())
}
