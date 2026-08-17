//! Controller-side support for repositories configured with `local` sources.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::hel_config::ProjectBundle;
use crate::hel_subprocess::run_with_input;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyLocalRepository {
    pub id: String,
    pub path: PathBuf,
    pub summary: String,
}

pub fn dirty_local_repositories(bundle: &ProjectBundle) -> Result<Vec<DirtyLocalRepository>> {
    bundle
        .repositories
        .iter()
        .filter_map(|repository| repository.local.as_ref().map(|path| (repository, path)))
        .filter_map(|(repository, path)| match local_status(path) {
            Ok(Some(summary)) => Some(Ok(DirtyLocalRepository {
                id: repository.id.clone(),
                path: path.clone(),
                summary,
            })),
            Ok(None) => None,
            Err(error) => Some(Err(
                error.context(format!("inspect local repository {:?}", repository.id))
            )),
        })
        .collect()
}

pub fn canonical_repository(path: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .with_context(|| format!("start git in {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "{} is not a Git repository with a readable worktree: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let root = String::from_utf8(output.stdout).context("decode Git repository root")?;
    let root = PathBuf::from(root.trim());
    let root = std::fs::canonicalize(&root)
        .with_context(|| format!("canonicalize local repository {}", root.display()))?;
    let root = main_worktree_root(&root)?;
    reject_git_lfs(&root)?;
    Ok(root)
}

/// Map a repository top level to the top level of its main working tree.
///
/// A linked worktree created by `git worktree add` reports its own top level,
/// but Hel treats it as the same repository as the main working tree.
pub fn main_worktree_root(root: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .current_dir(root)
        .output()
        .with_context(|| format!("start git in {}", root.display()))?;
    if !output.status.success() {
        bail!(
            "could not read the common Git directory for {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let common = String::from_utf8(output.stdout).context("decode common Git directory")?;
    let common = PathBuf::from(common.trim());
    // A bare or otherwise unusual layout has no `.git` parent to fall back to.
    if common.file_name() != Some(std::ffi::OsStr::new(".git")) {
        return Ok(root.to_path_buf());
    }
    let Some(parent) = common.parent().filter(|parent| parent.is_dir()) else {
        return Ok(root.to_path_buf());
    };
    let parent = std::fs::canonicalize(parent)
        .with_context(|| format!("canonicalize main worktree {}", parent.display()))?;
    if parent == root {
        return Ok(root.to_path_buf());
    }
    Ok(parent)
}

fn reject_git_lfs(repository: &Path) -> Result<()> {
    let paths = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(repository)
        .output()
        .context("list tracked files for Git LFS check")?;
    if !paths.status.success() {
        bail!("could not list tracked files for Git LFS check");
    }
    let mut command = Command::new("git");
    command
        .args(["check-attr", "-z", "--stdin", "filter"])
        .current_dir(repository);
    let output =
        run_with_input(&mut command, &paths.stdout).context("run Git LFS attribute check")?;
    if !output.status.success() {
        bail!("could not inspect Git LFS attributes");
    }
    let fields = output.stdout.split(|byte| *byte == 0).collect::<Vec<_>>();
    for record in fields.chunks_exact(3) {
        if record[2] == b"lfs" {
            bail!(
                "local repository {} uses Git LFS for {}; local Git proxy repositories do not support Git LFS",
                repository.display(),
                String::from_utf8_lossy(record[0])
            );
        }
    }
    Ok(())
}

fn local_status(path: &Path) -> Result<Option<String>> {
    let root = canonical_repository(path)?;
    let head = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(&root)
        .output()
        .with_context(|| format!("read HEAD in {}", root.display()))?;
    if !head.status.success() {
        bail!("local repository {} has no commit at HEAD", root.display());
    }
    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .current_dir(&root)
        .output()
        .with_context(|| format!("read Git status in {}", root.display()))?;
    if !output.status.success() {
        bail!(
            "git status failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let status = String::from_utf8(output.stdout).context("decode Git status")?;
    let mut lines = status.lines();
    let Some(first) = lines.next() else {
        return Ok(None);
    };
    let remaining = lines.count();
    let summary = if remaining == 0 {
        first.to_owned()
    } else {
        format!("{first} (and {remaining} more)")
    };
    Ok(Some(summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn git(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn local_status_distinguishes_clean_and_dirty_repositories() {
        let directory = tempfile::tempdir().unwrap();
        git(directory.path(), &["init", "-q", "-b", "main"]);
        git(directory.path(), &["config", "user.name", "Hel Test"]);
        git(
            directory.path(),
            &["config", "user.email", "hel@example.test"],
        );
        fs::write(directory.path().join("tracked"), "clean").unwrap();
        git(directory.path(), &["add", "."]);
        git(directory.path(), &["commit", "-qm", "base"]);
        assert_eq!(local_status(directory.path()).unwrap(), None);
        fs::write(directory.path().join("untracked"), "dirty").unwrap();
        assert!(
            local_status(directory.path())
                .unwrap()
                .unwrap()
                .contains("untracked")
        );
    }

    #[test]
    fn canonical_repository_maps_a_linked_worktree_to_its_main_repository() {
        let directory = tempfile::tempdir().unwrap();
        let main = directory.path().join("main");
        fs::create_dir_all(&main).unwrap();
        git(&main, &["init", "-q", "-b", "main"]);
        git(&main, &["config", "user.name", "Hel Test"]);
        git(&main, &["config", "user.email", "hel@example.test"]);
        fs::write(main.join("tracked"), "clean").unwrap();
        git(&main, &["add", "."]);
        git(&main, &["commit", "-qm", "base"]);
        let worktree = directory.path().join("main2");
        git(
            &main,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "side",
                worktree.to_str().unwrap(),
            ],
        );

        let expected = fs::canonicalize(&main).unwrap();
        assert_eq!(canonical_repository(&main).unwrap(), expected);
        assert_eq!(canonical_repository(&worktree).unwrap(), expected);
    }

    #[test]
    fn local_repository_rejects_git_lfs_attributes() {
        let directory = tempfile::tempdir().unwrap();
        git(directory.path(), &["init", "-q", "-b", "main"]);
        fs::write(
            directory.path().join(".gitattributes"),
            "*.bin filter=lfs\n",
        )
        .unwrap();
        fs::write(directory.path().join("asset.bin"), "pointer").unwrap();
        git(
            directory.path(),
            &[
                "-c",
                "filter.lfs.process=",
                "-c",
                "filter.lfs.clean=cat",
                "-c",
                "filter.lfs.required=false",
                "add",
                ".",
            ],
        );
        let error = canonical_repository(directory.path()).unwrap_err();
        assert!(
            error.to_string().contains("do not support Git LFS"),
            "{error:#}"
        );
    }
}
