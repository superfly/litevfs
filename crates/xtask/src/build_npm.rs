use duct::cmd;
use std::{env, fs, path::PathBuf};

use crate::DynError;

pub fn build_npm_binary(
    lib: PathBuf,
    cpu: String,
    os: String,
    abi: Option<String>,
) -> Result<(), DynError> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let pkg_dir = env::temp_dir().join(if let Some(ref abi) = abi {
        format!("litevfs-{}-{}-{}", os, cpu, abi)
    } else {
        format!("litevfs-{}-{}", os, cpu)
    });
    let lib_dir = pkg_dir.join("lib");
    let npm_dir = metadata.target_directory.join("npm");

    let version = metadata
        .packages
        .iter()
        .find(|p| p.name == "litevfs")
        .map(|p| p.version.to_string())
        .ok_or("Can't find LiteVFS version")?;

    fs::create_dir_all(&pkg_dir)?;
    fs::create_dir_all(&lib_dir)?;
    fs::create_dir_all(&npm_dir)?;

    fs::copy(&lib, lib_dir.join(lib.file_name().unwrap()))?;
    let package_json = fs::read_to_string(
        metadata
            .workspace_root
            .join("npm")
            .join("package.json.tmpl"),
    )?;
    let package_json = package_json
        .replace("{OS}", &os)
        .replace("{ARCH}", &cpu)
        .replace("{VERSION}", &version);
    let package_json = if let Some(abi) = abi {
        package_json.replace("{ABI}", &format!("-{}", abi))
    } else {
        package_json.replace("{ABI}", "")
    };

    fs::write(pkg_dir.join("package.json"), package_json)?;

    env::set_current_dir(npm_dir)?;

    cmd!("npm", "pack", pkg_dir).run()?;

    Ok(())
}

pub fn build_npm_meta() -> Result<(), DynError> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let pkg_dir = env::temp_dir().join("litevfs-meta");
    let lib_dir = pkg_dir.join("lib");
    let scripts_dir = pkg_dir.join("scripts");
    let npm_dir = metadata.target_directory.join("npm");

    let version = metadata
        .packages
        .iter()
        .find(|p| p.name == "litevfs")
        .map(|p| p.version.to_string())
        .ok_or("Can't find LiteVFS version")?;

    fs::create_dir_all(&pkg_dir)?;
    fs::create_dir_all(&lib_dir)?;
    fs::create_dir_all(&scripts_dir)?;
    fs::create_dir_all(&npm_dir)?;

    for file in fs::read_dir(
        metadata
            .workspace_root
            .join("npm")
            .join("litevfs")
            .join("lib"),
    )? {
        let file = file?;
        fs::copy(&file.path(), lib_dir.join(file.file_name()))?;
    }

    for file in fs::read_dir(
        metadata
            .workspace_root
            .join("npm")
            .join("litevfs")
            .join("scripts"),
    )? {
        let file = file?;
        fs::copy(&file.path(), scripts_dir.join(file.file_name()))?;
    }

    let package_json = fs::read_to_string(
        metadata
            .workspace_root
            .join("npm")
            .join("litevfs")
            .join("package.json.tmpl"),
    )?;
    let package_json = package_json.replace("{VERSION}", &version);

    fs::write(pkg_dir.join("package.json"), package_json)?;

    env::set_current_dir(npm_dir)?;

    cmd!("npm", "pack", pkg_dir).run()?;

    Ok(())
}
