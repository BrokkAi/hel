//! Snapshot and installation of directories attached to remote session targets.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path};

use anyhow::{Context, Result, bail, ensure};
use zip::write::SimpleFileOptions;

const MAX_ENTRIES: usize = 1_000_000;
const MAX_EXPANDED_BYTES: u64 = 50 * 1024 * 1024 * 1024;

pub fn write_resource_archive(source: &Path, archive: &Path) -> Result<()> {
    ensure!(
        source.is_dir(),
        "resource source is not a directory: {}",
        source.display()
    );
    let output = File::create(archive)
        .with_context(|| format!("create resource archive {}", archive.display()))?;
    let mut writer = zip::ZipWriter::new(output);
    let mut entries = 0;
    append_directory(&mut writer, source, source, &mut entries)?;
    writer.finish().context("finish resource ZIP archive")?;
    Ok(())
}

fn append_directory(
    writer: &mut zip::ZipWriter<File>,
    root: &Path,
    directory: &Path,
    entries: &mut usize,
) -> Result<()> {
    let mut children = fs::read_dir(directory)
        .with_context(|| format!("read resource directory {}", directory.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    children.sort_by_key(|entry| entry.file_name());
    if children.is_empty() && directory != root {
        let name = archive_name(root, directory)?;
        writer.add_directory(
            format!("{name}/"),
            SimpleFileOptions::default().unix_permissions(0o755),
        )?;
        *entries += 1;
    }
    for child in children {
        ensure!(
            *entries < MAX_ENTRIES,
            "resource archive has too many entries"
        );
        let path = child.path();
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "resource contains unsupported symbolic link: {}",
            path.display()
        );
        if metadata.is_dir() {
            append_directory(writer, root, &path, entries)?;
            continue;
        }
        ensure!(
            metadata.is_file(),
            "resource entry is not a regular file: {}",
            path.display()
        );
        let name = archive_name(root, &path)?;
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o777
        };
        #[cfg(not(unix))]
        let mode = 0o600;
        writer.start_file(
            name,
            SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated)
                .unix_permissions(mode),
        )?;
        let mut input = File::open(&path)?;
        std::io::copy(&mut input, writer)?;
        *entries += 1;
    }
    Ok(())
}

fn archive_name(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root)?;
    ensure!(
        !relative.as_os_str().is_empty(),
        "resource archive entry is empty"
    );
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            _ => bail!("unsafe resource archive path: {}", relative.display()),
        }
    }
    Ok(parts.join("/"))
}

pub fn install_resource_archive(archive: &Path, destination: &Path) -> Result<()> {
    validate_destination(destination)?;
    fs::create_dir_all(destination)
        .with_context(|| format!("create resource destination {}", destination.display()))?;
    let input = File::open(archive)
        .with_context(|| format!("open resource archive {}", archive.display()))?;
    let mut archive = zip::ZipArchive::new(input).context("open resource ZIP archive")?;
    ensure!(
        archive.len() <= MAX_ENTRIES,
        "resource archive has too many entries"
    );
    let mut expanded = 0_u64;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let relative = entry
            .enclosed_name()
            .ok_or_else(|| anyhow::anyhow!("unsafe resource ZIP entry {}", entry.name()))?;
        ensure!(!relative.as_os_str().is_empty(), "empty resource ZIP entry");
        expanded = expanded
            .checked_add(entry.size())
            .context("resource expanded size overflow")?;
        ensure!(
            expanded <= MAX_EXPANDED_BYTES,
            "resource expanded size exceeds 50 GiB"
        );
        let output = destination.join(&relative);
        ensure_safe_parent(destination, output.parent().unwrap_or(destination))?;
        ensure_not_symlink(&output)?;
        if entry.is_dir() {
            fs::create_dir_all(&output)?;
            continue;
        }
        ensure!(
            entry.unix_mode().unwrap_or(0o600) & 0o170000 != 0o120000,
            "resource ZIP contains unsupported symbolic link: {}",
            entry.name()
        );
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(&output)?;
        std::io::copy(&mut entry, &mut file)?;
        file.flush()?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output, fs::Permissions::from_mode(mode & 0o777))?;
        }
    }
    Ok(())
}

fn ensure_not_symlink(path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            !metadata.file_type().is_symlink(),
            "resource destination is a symbolic link: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_destination(destination: &Path) -> Result<()> {
    ensure!(
        destination.is_absolute(),
        "resource destination must be absolute"
    );
    ensure!(
        destination != Path::new("/"),
        "resource destination cannot be the filesystem root"
    );
    ensure!(
        !destination
            .components()
            .any(|part| part == Component::ParentDir),
        "resource destination cannot contain '..'"
    );
    Ok(())
}

fn ensure_safe_parent(root: &Path, parent: &Path) -> Result<()> {
    let relative = parent.strip_prefix(root)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        if let Ok(metadata) = fs::symlink_metadata(&current) {
            ensure!(
                !metadata.file_type().is_symlink(),
                "resource destination traverses symbolic link: {}",
                current.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_directory_round_trips_through_one_zip() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("many/nested")).unwrap();
        fs::write(source.path().join("many/nested/a.txt"), b"alpha").unwrap();
        fs::create_dir_all(source.path().join("empty")).unwrap();
        let staging = tempfile::tempdir().unwrap();
        let archive = staging.path().join("resource.zip");
        let destination = staging.path().join("installed");

        write_resource_archive(source.path(), &archive).unwrap();
        install_resource_archive(&archive, &destination).unwrap();

        assert_eq!(
            fs::read(destination.join("many/nested/a.txt")).unwrap(),
            b"alpha"
        );
        assert!(destination.join("empty").is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn resource_archives_reject_symbolic_links() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        symlink("outside", source.path().join("link")).unwrap();
        let staging = tempfile::tempdir().unwrap();
        let archive = staging.path().join("resource.zip");
        let error = write_resource_archive(source.path(), &archive).unwrap_err();
        assert!(error.to_string().contains("symbolic link"));
    }
}
