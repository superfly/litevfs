use current_platform::CURRENT_PLATFORM;
use duct::cmd;
use std::fs;

use crate::DynError;

fn rust_target_to_npm(target: &str) -> Result<(&'static str, &'static str), DynError> {
    match target {
        "x86_64-unknown-linux-gnu" => Ok(("x64", "linux")),
        "aarch64-unknown-linux-gnu" => Ok(("arm64", "linux")),
        "x86_64-apple-darwin" => Ok(("x64", "darwin")),
        "aarch64-apple-darwin" => Ok(("arm64", "darwin")),
        _ => Err(format!("unknown target {}", target).into()),
    }
}

pub fn build_npm_binary() -> Result<(), DynError> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;

    let (arch, os) = rust_target_to_npm(CURRENT_PLATFORM)?;
    let pkg_dir = metadata
        .target_directory
        .join("npm")
        .join(format!("litevfs-{}-{}", os, arch));
    let lib_dir = pkg_dir.join("lib");
    let version = metadata
        .packages
        .iter()
        .find(|p| p.name == "litevfs")
        .map(|p| p.version.to_string())
        .ok_or("Can't find LiteVFS version")?;

    cmd!("cargo", "build", "--package", "litevfs", "--release").run()?;

    fs::create_dir_all(&pkg_dir)?;
    fs::create_dir_all(&lib_dir)?;

    fs::copy(
        metadata
            .target_directory
            .join("release")
            .join("liblitevfs.so"),
        lib_dir.join("liblitevfs.so"),
    )?;
    let package_json = fs::read_to_string(
        metadata
            .workspace_root
            .join("npm")
            .join("package.json.tmpl"),
    )?;
    let package_json = package_json.replace("{OS}", os);
    let package_json = package_json.replace("{ARCH}", arch);
    let package_json = package_json.replace("{VERSION}", &version);

    fs::write(pkg_dir.join("package.json"), package_json)?;

    Ok(())
}

pub fn build_npm_meta() -> Result<(), DynError> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;

    let pkg_dir = metadata.target_directory.join("npm").join("litevfs");
    let lib_dir = pkg_dir.join("lib");
    let version = metadata
        .packages
        .iter()
        .find(|p| p.name == "litevfs")
        .map(|p| p.version.to_string())
        .ok_or("Can't find LiteVFS version")?;

    fs::create_dir_all(&pkg_dir)?;
    fs::create_dir_all(&lib_dir)?;

    fs::copy(
        metadata
            .workspace_root
            .join("npm")
            .join("litevfs")
            .join("lib")
            .join("index.js"),
        lib_dir.join("index.js"),
    )?;

    let package_json = fs::read_to_string(
        metadata
            .workspace_root
            .join("npm")
            .join("litevfs")
            .join("package.json.tmpl"),
    )?;
    let package_json = package_json.replace("{VERSION}", &version);

    fs::write(pkg_dir.join("package.json"), package_json)?;

    Ok(())
}
