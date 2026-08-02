use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MORAINE_BUILD_GIT_SHA");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let git_sha = env::var("MORAINE_BUILD_GIT_SHA")
        .ok()
        .and_then(normalize_sha)
        .or_else(|| {
            git_output(&manifest_dir, &["rev-parse", "--short=12", "HEAD"]).and_then(normalize_sha)
        });

    if let Some(head_path) = git_output(&manifest_dir, &["rev-parse", "--git-path", "HEAD"]) {
        let head_path = PathBuf::from(head_path);
        let head_path = if head_path.is_absolute() {
            head_path
        } else {
            manifest_dir.join(head_path)
        };
        println!("cargo:rerun-if-changed={}", head_path.display());
    }
    if let Some(head_ref) = git_output(&manifest_dir, &["symbolic-ref", "-q", "HEAD"]) {
        if let Some(ref_path) = git_output(&manifest_dir, &["rev-parse", "--git-path", &head_ref]) {
            let ref_path = PathBuf::from(ref_path);
            let ref_path = if ref_path.is_absolute() {
                ref_path
            } else {
                manifest_dir.join(ref_path)
            };
            println!("cargo:rerun-if-changed={}", ref_path.display());
        }
    }

    let package_version = env::var("CARGO_PKG_VERSION").expect("package version");
    let build_version = match git_sha.as_deref() {
        Some(sha) => format!("{package_version}+g{sha}"),
        None => package_version,
    };
    println!("cargo:rustc-env=MORAINE_BUILD_VERSION={build_version}");
}

fn git_output(manifest_dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(manifest_dir)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn normalize_sha(value: String) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    (7..=40)
        .contains(&value.len())
        .then_some(value)
        .filter(|value| value.bytes().all(|byte| byte.is_ascii_hexdigit()))
}
