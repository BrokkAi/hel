//! Dirty-worktree-aware Git tree snapshots used to attribute changes to one
//! outer user turn or one subagent invocation without touching the real index.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::process::Command;
use tokio::sync::Mutex;

const RECEIPT_LIMIT: usize = 64 * 1024;
pub const REVIEW_PATCH_LIMIT: usize = 128 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryReviewTarget {
    Uncommitted,
    Head,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSnapshot {
    inner: Arc<WorkspaceSnapshotInner>,
}

#[derive(Debug)]
struct WorkspaceSnapshotInner {
    roots: Vec<Mutex<GitTreeSnapshot>>,
    unavailable: Vec<SnapshotNotice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotNotice {
    root: PathBuf,
    message: String,
}

#[derive(Debug)]
struct GitTreeSnapshot {
    repo_root: PathBuf,
    pathspecs: Vec<PathBuf>,
    index_path: PathBuf,
    object_dir: PathBuf,
    alternate_object_dir: PathBuf,
    baseline_tree: String,
    scratch: Arc<tempfile::TempDir>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceDelta {
    changed: bool,
    receipt: String,
    review_patch: Option<String>,
    review_snapshot: Option<ReviewSnapshot>,
}

/// Immutable Git endpoints and human-facing evidence for one captured change
/// interval. The scratch-directory lease keeps the private object database and
/// full patch alive until every review consumer has finished.
#[derive(Debug, Clone)]
pub(crate) struct ReviewSnapshot {
    repo_root: PathBuf,
    object_dir: PathBuf,
    base_tree: String,
    target_tree: String,
    diffstat: String,
    changed_line_count: usize,
    full_patch_path: PathBuf,
    _lease: Arc<tempfile::TempDir>,
}

impl ReviewSnapshot {
    pub(crate) fn repo_root(&self) -> &Path {
        &self.repo_root
    }

    pub(crate) fn object_dir(&self) -> &Path {
        &self.object_dir
    }

    pub(crate) fn base_tree(&self) -> &str {
        &self.base_tree
    }

    pub(crate) fn target_tree(&self) -> &str {
        &self.target_tree
    }

    pub(crate) fn diffstat(&self) -> &str {
        &self.diffstat
    }

    pub(crate) fn changed_line_count(&self) -> usize {
        self.changed_line_count
    }

    pub(crate) async fn full_patch(&self) -> Result<String, String> {
        tokio::fs::read_to_string(&self.full_patch_path)
            .await
            .map_err(|error| format!("could not read captured turn patch: {error}"))
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        repo_root: PathBuf,
        base_tree: &str,
        target_tree: &str,
        diffstat: &str,
        changed_line_count: usize,
        patch: &str,
    ) -> Self {
        let scratch = Arc::new(tempfile::tempdir().expect("review snapshot tempdir"));
        let object_dir = scratch.path().join("objects");
        std::fs::create_dir_all(object_dir.join("info")).expect("snapshot object info dir");
        std::fs::create_dir_all(object_dir.join("pack")).expect("snapshot object pack dir");
        let full_patch_path = scratch.path().join("review.patch");
        std::fs::write(&full_patch_path, patch).expect("snapshot patch");
        Self {
            repo_root,
            object_dir,
            base_tree: base_tree.to_string(),
            target_tree: target_tree.to_string(),
            diffstat: diffstat.to_string(),
            changed_line_count,
            full_patch_path,
            _lease: scratch,
        }
    }
}

impl WorkspaceDelta {
    pub(crate) fn changed(&self) -> bool {
        self.changed
    }

    pub(crate) fn receipt(&self) -> &str {
        &self.receipt
    }

    pub(crate) fn review_patch(&self) -> Option<&str> {
        self.review_patch.as_deref()
    }

    pub(crate) fn review_snapshot(&self) -> Option<&ReviewSnapshot> {
        self.review_snapshot.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn changed_for_test(patch: String) -> Self {
        Self {
            changed: true,
            receipt: String::new(),
            review_patch: Some(patch),
            review_snapshot: None,
        }
    }
}

struct RootDelta {
    receipt: String,
    patch: String,
    review_snapshot: ReviewSnapshot,
}

impl WorkspaceSnapshot {
    pub(crate) async fn capture(workspace_roots: &[PathBuf]) -> Self {
        let mut repositories: BTreeMap<PathBuf, (PathBuf, BTreeSet<PathBuf>)> = BTreeMap::new();
        let mut unavailable = Vec::new();

        for requested_root in workspace_roots {
            let root = match tokio::fs::canonicalize(requested_root).await {
                Ok(root) => root,
                Err(_) => {
                    unavailable.push(SnapshotNotice {
                        root: requested_root.clone(),
                        message: "workspace root is unavailable".to_string(),
                    });
                    continue;
                }
            };
            let Some((repo_root, common_dir)) = discover_repository(&root).await else {
                unavailable.push(SnapshotNotice {
                    root,
                    message: "not a Git worktree".to_string(),
                });
                continue;
            };
            let pathspec = match root.strip_prefix(&repo_root) {
                Ok(path) if path.as_os_str().is_empty() => PathBuf::from("."),
                Ok(path) => path.to_path_buf(),
                Err(_) => {
                    unavailable.push(SnapshotNotice {
                        root,
                        message: "workspace root is outside its Git worktree".to_string(),
                    });
                    continue;
                }
            };
            repositories
                .entry(repo_root)
                .or_insert_with(|| (common_dir, BTreeSet::new()))
                .1
                .insert(pathspec);
        }

        let mut roots = Vec::new();
        for (repo_root, (common_dir, mut pathspecs)) in repositories {
            if pathspecs.contains(Path::new(".")) {
                pathspecs.clear();
                pathspecs.insert(PathBuf::from("."));
            }
            match GitTreeSnapshot::capture(repo_root.clone(), common_dir, pathspecs).await {
                Ok(snapshot) => roots.push(Mutex::new(snapshot)),
                Err(message) => unavailable.push(SnapshotNotice {
                    root: repo_root,
                    message,
                }),
            }
        }

        if workspace_roots.is_empty() {
            unavailable.push(SnapshotNotice {
                root: PathBuf::from("."),
                message: "no workspace roots were supplied".to_string(),
            });
        }

        Self {
            inner: Arc::new(WorkspaceSnapshotInner { roots, unavailable }),
        }
    }

    pub(crate) async fn delta(&self) -> WorkspaceDelta {
        let mut receipt_sections = Vec::new();
        let mut patch_sections = Vec::new();
        let mut review_snapshots = Vec::new();
        let mut review_notices = Vec::new();

        for root in &self.inner.roots {
            let mut root = root.lock().await;
            match root.delta().await {
                Ok(Some(delta)) => {
                    receipt_sections.push(format!(
                        "Repository: {}\n{}",
                        root.repo_root.display(),
                        delta.receipt.trim_end()
                    ));
                    patch_sections.push(format!(
                        "Repository: {}\n{}",
                        root.repo_root.display(),
                        delta.patch.trim_end()
                    ));
                    review_snapshots.push(delta.review_snapshot);
                }
                Ok(None) => {}
                Err(message) => {
                    let notice = format!(
                        "Repository: {}\n  delta unavailable: {message}",
                        root.repo_root.display()
                    );
                    receipt_sections.push(notice.clone());
                    review_notices.push(notice);
                }
            }
        }

        if !self.inner.unavailable.is_empty() {
            let mut section = String::from("Unavailable workspace roots:");
            for notice in &self.inner.unavailable {
                section.push_str(&format!(
                    "\n  - {}: {}",
                    notice.root.display(),
                    notice.message
                ));
            }
            receipt_sections.push(section.clone());
            review_notices.push(section);
        }

        let changed = !patch_sections.is_empty();
        let receipt = if receipt_sections.is_empty() {
            "No workspace changes.".to_string()
        } else {
            bound_text(receipt_sections.join("\n\n"), RECEIPT_LIMIT)
        };
        if changed && !review_notices.is_empty() {
            patch_sections.push(review_notices.join("\n\n"));
        }
        let review_patch =
            changed.then(|| bound_text(patch_sections.join("\n\n"), REVIEW_PATCH_LIMIT));
        WorkspaceDelta {
            changed,
            receipt,
            review_patch,
            review_snapshot: (review_snapshots.len() == 1)
                .then(|| review_snapshots.pop().expect("one review snapshot")),
        }
    }
}

pub(crate) async fn repository_review_patch(
    workspace_root: &Path,
    target: RepositoryReviewTarget,
) -> Result<String, String> {
    let root = tokio::fs::canonicalize(workspace_root)
        .await
        .map_err(|_| "workspace root is unavailable".to_string())?;
    let (repo_root, common_dir) = discover_repository(&root)
        .await
        .ok_or_else(|| "workspace root is not a Git worktree".to_string())?;
    let pathspec = root
        .strip_prefix(&repo_root)
        .ok()
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    match target {
        RepositoryReviewTarget::Uncommitted => {
            let mut snapshot =
                GitTreeSnapshot::capture_head(repo_root.clone(), common_dir, vec![pathspec])
                    .await?;
            snapshot.delta().await.map(|delta| {
                delta.map_or_else(
                    || "No uncommitted changes.".to_string(),
                    |delta| {
                        bound_text(
                            format!(
                                "Repository: {}\n{}",
                                repo_root.display(),
                                delta.patch.trim_end()
                            ),
                            REVIEW_PATCH_LIMIT,
                        )
                    },
                )
            })
        }
        RepositoryReviewTarget::Head => {
            let pathspec = pathspec.to_string_lossy().to_string();
            let output = run_plain_git(
                &repo_root,
                &[
                    "diff-tree",
                    "--root",
                    "-p",
                    "--first-parent",
                    "--no-commit-id",
                    "--no-color",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--find-renames",
                    "HEAD",
                    "--",
                    &pathspec,
                ],
            )
            .await?;
            Ok(if output.trim().is_empty() {
                "HEAD introduces no changes in this workspace root.".to_string()
            } else {
                bound_text(
                    format!("Repository: {}\n{}", repo_root.display(), output.trim_end()),
                    REVIEW_PATCH_LIMIT,
                )
            })
        }
    }
}

impl GitTreeSnapshot {
    async fn capture(
        repo_root: PathBuf,
        common_dir: PathBuf,
        pathspecs: BTreeSet<PathBuf>,
    ) -> Result<Self, String> {
        let scratch = tempfile::Builder::new()
            .prefix("mj-workspace-snapshot-")
            .tempdir()
            .map_err(|_| "could not create temporary snapshot storage".to_string())?;
        let object_dir = scratch.path().join("objects");
        std::fs::create_dir_all(object_dir.join("info"))
            .and_then(|_| std::fs::create_dir_all(object_dir.join("pack")))
            .map_err(|_| "could not initialize temporary Git object storage".to_string())?;
        let alternate_object_dir = common_dir.join("objects");
        let pathspecs = pathspecs.into_iter().collect::<Vec<_>>();

        let scratch = Arc::new(scratch);
        let mut snapshot = Self {
            repo_root,
            pathspecs,
            index_path: scratch.path().join("index"),
            object_dir: scratch.path().join("objects"),
            alternate_object_dir,
            baseline_tree: String::new(),
            scratch,
        };

        let head_tree = run_plain_git(
            &snapshot.repo_root,
            &["rev-parse", "--verify", "HEAD^{tree}"],
        )
        .await
        .ok()
        .and_then(|output| output.lines().next().map(str::trim).map(str::to_string))
        .filter(|tree| !tree.is_empty());
        match head_tree {
            Some(tree) => {
                snapshot
                    .run_scratch_git(["read-tree"], [tree.as_str()])
                    .await?
            }
            None => {
                snapshot
                    .run_scratch_git(["read-tree", "--empty"], std::iter::empty::<&str>())
                    .await?
            }
        }
        snapshot.refresh_index().await?;
        snapshot.baseline_tree = snapshot.write_tree().await?;
        Ok(snapshot)
    }

    async fn capture_head(
        repo_root: PathBuf,
        common_dir: PathBuf,
        pathspecs: Vec<PathBuf>,
    ) -> Result<Self, String> {
        let scratch = tempfile::Builder::new()
            .prefix("mj-workspace-review-")
            .tempdir()
            .map_err(|_| "could not create temporary snapshot storage".to_string())?;
        let object_dir = scratch.path().join("objects");
        std::fs::create_dir_all(object_dir.join("info"))
            .and_then(|_| std::fs::create_dir_all(object_dir.join("pack")))
            .map_err(|_| "could not initialize temporary Git object storage".to_string())?;
        let alternate_object_dir = common_dir.join("objects");
        let scratch = Arc::new(scratch);
        let mut snapshot = Self {
            repo_root,
            pathspecs,
            index_path: scratch.path().join("index"),
            object_dir: scratch.path().join("objects"),
            alternate_object_dir,
            baseline_tree: String::new(),
            scratch,
        };
        let head_tree = run_plain_git(
            &snapshot.repo_root,
            &["rev-parse", "--verify", "HEAD^{tree}"],
        )
        .await
        .ok()
        .and_then(|output| output.lines().next().map(str::trim).map(str::to_string))
        .filter(|tree| !tree.is_empty());
        match head_tree {
            Some(tree) => {
                snapshot
                    .run_scratch_git(["read-tree"], [tree.as_str()])
                    .await?;
                snapshot.baseline_tree = tree;
            }
            None => {
                snapshot
                    .run_scratch_git(["read-tree", "--empty"], std::iter::empty::<&str>())
                    .await?;
                snapshot.baseline_tree = snapshot.write_tree().await?;
            }
        }
        Ok(snapshot)
    }

    async fn delta(&mut self) -> Result<Option<RootDelta>, String> {
        self.refresh_index().await?;
        let after_tree = self.write_tree().await?;
        if after_tree == self.baseline_tree {
            return Ok(None);
        }
        let receipt = self.diff(&after_tree, &["--stat", "--summary"]).await?;
        let patch = self.diff(&after_tree, &[]).await?;
        let full_patch_path = self
            .scratch
            .path()
            .join(format!("review-{after_tree}.patch"));
        tokio::fs::write(&full_patch_path, &patch)
            .await
            .map_err(|error| format!("could not persist captured turn patch: {error}"))?;
        let review_snapshot = ReviewSnapshot {
            repo_root: self.repo_root.clone(),
            object_dir: self.object_dir.clone(),
            base_tree: self.baseline_tree.clone(),
            target_tree: after_tree,
            diffstat: receipt.clone(),
            changed_line_count: changed_line_count(&patch),
            full_patch_path,
            _lease: Arc::clone(&self.scratch),
        };
        Ok(Some(RootDelta {
            receipt,
            patch,
            review_snapshot,
        }))
    }

    async fn refresh_index(&self) -> Result<(), String> {
        let pathspecs = self
            .pathspecs
            .iter()
            .map(|path| path.as_os_str())
            .collect::<Vec<_>>();
        self.run_scratch_git(["add", "-A", "--"], pathspecs).await
    }

    async fn write_tree(&self) -> Result<String, String> {
        let output = self
            .run_scratch_git_output(["write-tree"], std::iter::empty::<&str>())
            .await?;
        let tree = output.trim();
        if tree.is_empty() {
            Err("Git returned an empty tree identifier".to_string())
        } else {
            Ok(tree.to_string())
        }
    }

    async fn diff(&self, after_tree: &str, display_args: &[&str]) -> Result<String, String> {
        let mut args = vec![
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--find-renames",
        ];
        args.extend_from_slice(display_args);
        args.push(&self.baseline_tree);
        args.push(after_tree);
        args.push("--");
        let pathspecs = self
            .pathspecs
            .iter()
            .map(|path| path.as_os_str())
            .collect::<Vec<_>>();
        self.run_scratch_git_output(args, pathspecs).await
    }

    async fn run_scratch_git<I, S, J, T>(&self, args: I, trailing: J) -> Result<(), String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
        J: IntoIterator<Item = T>,
        T: AsRef<std::ffi::OsStr>,
    {
        self.run_scratch_git_output(args, trailing)
            .await
            .map(|_| ())
    }

    async fn run_scratch_git_output<I, S, J, T>(
        &self,
        args: I,
        trailing: J,
    ) -> Result<String, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
        J: IntoIterator<Item = T>,
        T: AsRef<std::ffi::OsStr>,
    {
        let output = Command::new("git")
            .current_dir(&self.repo_root)
            .env("GIT_INDEX_FILE", &self.index_path)
            .env("GIT_OBJECT_DIRECTORY", &self.object_dir)
            .env(
                "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                &self.alternate_object_dir,
            )
            .args(args)
            .args(trailing)
            .output()
            .await
            .map_err(|_| "could not launch Git snapshot command".to_string())?;
        if !output.status.success() {
            return Err(git_failure(&output));
        }
        String::from_utf8(output.stdout)
            .map_err(|_| "Git snapshot output was not UTF-8".to_string())
    }
}

async fn discover_repository(workspace_root: &Path) -> Option<(PathBuf, PathBuf)> {
    let repo_root = run_plain_git(workspace_root, &["rev-parse", "--show-toplevel"])
        .await
        .ok()?;
    let common_dir = run_plain_git(
        workspace_root,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .await
    .ok()?;
    let repo_root = tokio::fs::canonicalize(repo_root.trim()).await.ok()?;
    let common_dir = tokio::fs::canonicalize(common_dir.trim()).await.ok()?;
    Some((repo_root, common_dir))
}

async fn run_plain_git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .args(args)
        .output()
        .await
        .map_err(|_| "could not launch Git".to_string())?;
    if !output.status.success() {
        return Err(git_failure(&output));
    }
    String::from_utf8(output.stdout).map_err(|_| "Git output was not UTF-8".to_string())
}

fn git_failure(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .unwrap_or("Git command failed");
    let detail = detail.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(
        "Git snapshot command failed: {}",
        truncate_chars(&detail, 240)
    )
}

fn bound_text(text: String, limit: usize) -> String {
    if text.len() <= limit {
        return text;
    }
    const MARKER: &str = "\n…[workspace delta truncated]…\n";
    let available = limit.saturating_sub(MARKER.len());
    let head_len = available.saturating_mul(3) / 4;
    let tail_len = available.saturating_sub(head_len);
    let head_end = text.floor_char_boundary(head_len);
    let tail_start = text.ceil_char_boundary(text.len().saturating_sub(tail_len));
    format!("{}{}{}", &text[..head_end], MARKER, &text[tail_start..])
}

fn changed_line_count(diff: &str) -> usize {
    let mut in_hunk = false;
    let mut count = 0usize;
    for line in diff.lines() {
        if line.starts_with("diff --git ") {
            in_hunk = false;
        } else if line.starts_with("@@ ") {
            in_hunk = true;
        } else if in_hunk && (line.starts_with('+') || line.starts_with('-')) {
            count = count.saturating_add(1);
        }
    }
    count
}

fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_string()
    } else {
        text.chars().take(limit).collect::<String>() + "…"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(root: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(root)
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("utf8 git output")
    }

    fn init_repo(root: &Path) {
        git(root, &["init", "-q"]);
        git(root, &["config", "user.email", "mjolnir@example.test"]);
        git(root, &["config", "user.name", "Mjolnir Tests"]);
    }

    fn commit_all(root: &Path) {
        git(root, &["add", "-A"]);
        git(root, &["commit", "-qm", "baseline"]);
    }

    fn object_files(root: &Path) -> BTreeSet<PathBuf> {
        fn visit(root: &Path, dir: &Path, output: &mut BTreeSet<PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("read object directory") {
                let entry = entry.expect("object entry");
                let path = entry.path();
                if path.is_dir() {
                    visit(root, &path, output);
                } else {
                    output.insert(
                        path.strip_prefix(root)
                            .expect("relative object")
                            .to_path_buf(),
                    );
                }
            }
        }

        let objects = root.join(".git").join("objects");
        let mut output = BTreeSet::new();
        visit(&objects, &objects, &mut output);
        output
    }

    #[tokio::test]
    async fn repository_review_targets_cover_uncommitted_and_head_without_touching_index() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_repo(root);
        std::fs::write(root.join("tracked.txt"), "baseline\n").expect("baseline");
        commit_all(root);

        std::fs::write(root.join("tracked.txt"), "changed\n").expect("change tracked");
        git(root, &["add", "tracked.txt"]);
        std::fs::write(root.join("untracked.txt"), "new\n").expect("untracked");
        let status = git(root, &["status", "--porcelain=v1", "--untracked-files=all"]);
        let patch = repository_review_patch(root, RepositoryReviewTarget::Uncommitted)
            .await
            .expect("uncommitted patch");
        assert!(patch.contains("tracked.txt"));
        assert!(patch.contains("untracked.txt"));
        assert_eq!(
            git(root, &["status", "--porcelain=v1", "--untracked-files=all"]),
            status
        );

        commit_all(root);
        let head = repository_review_patch(root, RepositoryReviewTarget::Head)
            .await
            .expect("head patch");
        assert!(head.contains("tracked.txt"));
        assert!(head.contains("untracked.txt"));
    }

    #[tokio::test]
    async fn snapshot_attributes_only_interval_changes_without_touching_git_metadata() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_repo(root);
        std::fs::write(root.join("dirty.txt"), "committed dirty\n").expect("dirty seed");
        std::fs::write(root.join("staged.txt"), "committed staged\n").expect("staged seed");
        std::fs::write(root.join("delete.txt"), "delete me\n").expect("delete seed");
        std::fs::write(root.join("rename-old.txt"), "rename me\n").expect("rename seed");
        std::fs::write(root.join("mode.sh"), "#!/bin/sh\nexit 0\n").expect("mode seed");
        std::fs::write(root.join("baseline-dirty-only.txt"), "committed\n")
            .expect("baseline dirty seed");
        std::fs::write(root.join("baseline-staged-only.txt"), "committed\n")
            .expect("baseline staged seed");
        std::fs::write(root.join("baseline-deleted-only.txt"), "committed\n")
            .expect("baseline deleted seed");
        commit_all(root);

        std::fs::write(root.join("dirty.txt"), "dirty before subagent\n").expect("predirty");
        std::fs::write(root.join("staged.txt"), "staged before subagent\n").expect("prestage");
        git(root, &["add", "staged.txt"]);
        std::fs::write(root.join("untracked.txt"), "untracked before subagent\n")
            .expect("pre-untracked");
        std::fs::write(
            root.join("baseline-dirty-only.txt"),
            "dirty before capture\n",
        )
        .expect("baseline dirty");
        std::fs::write(
            root.join("baseline-staged-only.txt"),
            "staged before capture\n",
        )
        .expect("baseline staged");
        git(root, &["add", "baseline-staged-only.txt"]);
        std::fs::remove_file(root.join("baseline-deleted-only.txt")).expect("baseline deletion");
        std::fs::write(
            root.join("baseline-untracked-only.txt"),
            "untracked before capture\n",
        )
        .expect("baseline untracked");

        let git_dir = root.join(".git");
        let index_before = std::fs::read(git_dir.join("index")).expect("read real index");
        let refs_before = git(root, &["show-ref"]);
        let objects_before = object_files(root);
        let branch_before = git(root, &["symbolic-ref", "HEAD"]);
        let status_before = git(root, &["status", "--porcelain=v1", "--untracked-files=all"]);
        let snapshot = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;
        assert_eq!(
            git(root, &["status", "--porcelain=v1", "--untracked-files=all"]),
            status_before
        );
        assert_eq!(git(root, &["symbolic-ref", "HEAD"]), branch_before);

        std::fs::write(
            root.join("dirty.txt"),
            "dirty before subagent\nchanged during invocation\n",
        )
        .expect("change dirty");
        std::fs::write(
            root.join("staged.txt"),
            "staged before subagent\nchanged during invocation\n",
        )
        .expect("change staged");
        std::fs::write(
            root.join("untracked.txt"),
            "untracked before subagent\nchanged during invocation\n",
        )
        .expect("change untracked");
        std::fs::write(root.join("created.txt"), "created during invocation\n").expect("create");
        std::fs::remove_file(root.join("delete.txt")).expect("delete");
        std::fs::rename(root.join("rename-old.txt"), root.join("rename-new.txt")).expect("rename");
        std::fs::write(root.join("binary.bin"), [0_u8, 1, 2, 0]).expect("binary");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(root.join("mode.sh"))
                .expect("mode metadata")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(root.join("mode.sh"), permissions).expect("chmod");
        }

        let delta = snapshot.delta().await;
        assert!(delta.changed());
        let receipt = delta.receipt();
        for path in [
            "dirty.txt",
            "staged.txt",
            "untracked.txt",
            "created.txt",
            "delete.txt",
            "binary.bin",
        ] {
            assert!(receipt.contains(path), "receipt omitted {path}: {receipt}");
        }
        assert!(receipt.contains("rename-old.txt") || receipt.contains("rename-new.txt"));
        #[cfg(unix)]
        assert!(receipt.contains("mode change 100644 => 100755 mode.sh"));
        for path in [
            "baseline-dirty-only.txt",
            "baseline-staged-only.txt",
            "baseline-deleted-only.txt",
            "baseline-untracked-only.txt",
        ] {
            assert!(
                !receipt.contains(path),
                "pre-capture-only change leaked into receipt: {path}: {receipt}"
            );
        }

        let patch = delta.review_patch().expect("review patch");
        assert!(patch.contains("+changed during invocation"));
        assert!(!patch.contains("-committed dirty"));
        assert!(!patch.contains("-committed staged"));

        assert_eq!(
            std::fs::read(git_dir.join("index")).expect("read real index after"),
            index_before
        );
        assert_eq!(git(root, &["show-ref"]), refs_before);
        assert_eq!(git(root, &["symbolic-ref", "HEAD"]), branch_before);
        assert_eq!(object_files(root), objects_before);
    }

    #[tokio::test]
    async fn review_snapshot_captures_dirty_baseline_reversion_and_outlives_workspace_snapshot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_repo(root);
        std::fs::write(root.join("state.txt"), "head\n").expect("head state");
        commit_all(root);

        // The outer turn begins from a dirty state, then deliberately reverts
        // that state to HEAD. HEAD-to-worktree analysis would see no change,
        // but the captured tree interval must retain it.
        std::fs::write(root.join("state.txt"), "dirty before turn\n").expect("dirty baseline");
        let workspace = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;
        std::fs::write(root.join("state.txt"), "head\n").expect("turn reversion");

        let delta = workspace.delta().await;
        let review = delta
            .review_snapshot()
            .expect("exact review snapshot")
            .clone();
        assert_eq!(review.repo_root(), root);
        assert_ne!(review.base_tree(), review.target_tree());
        assert_eq!(review.changed_line_count(), 2);
        assert!(review.diffstat().contains("state.txt"));
        let patch = review.full_patch().await.expect("full patch");
        assert!(patch.contains("-dirty before turn"));
        assert!(patch.contains("+head"));
        assert!(
            git(root, &["diff", "HEAD"]).trim().is_empty(),
            "the live HEAD diff is intentionally empty in this regression"
        );

        drop(delta);
        drop(workspace);
        assert!(review.object_dir().is_dir());
        assert!(
            review
                .full_patch()
                .await
                .expect("leased full patch")
                .contains("-dirty before turn")
        );
    }

    #[tokio::test]
    async fn review_snapshot_keeps_full_large_patch_and_exact_line_count_beyond_prompt_limit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_repo(root);
        std::fs::write(root.join("large.txt"), "baseline\n").expect("baseline");
        commit_all(root);
        let workspace = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;

        let replacement = (0..200)
            .map(|index| format!("{index:03}-{}\n", "x".repeat(1024)))
            .collect::<String>();
        std::fs::write(root.join("large.txt"), replacement).expect("large turn edit");
        let delta = workspace.delta().await;
        let bounded = delta.review_patch().expect("bounded prompt patch");
        assert!(bounded.contains("workspace delta truncated"));
        assert!(bounded.len() <= REVIEW_PATCH_LIMIT);

        let review = delta.review_snapshot().expect("exact review snapshot");
        let full = review.full_patch().await.expect("full patch");
        assert!(full.len() > REVIEW_PATCH_LIMIT);
        assert_eq!(review.changed_line_count(), 201);
        assert!(review.diffstat().contains("large.txt"));
    }

    #[tokio::test]
    async fn overlapping_outer_and_invocation_snapshots_have_independent_baselines() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_repo(root);
        git(root, &["commit", "--allow-empty", "-qm", "baseline"]);

        let outer = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;
        std::fs::write(root.join("primary.txt"), "changed by the primary agent\n")
            .expect("primary edit");
        let invocation = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;
        std::fs::write(root.join("subagent.txt"), "changed by a subagent\n")
            .expect("subagent edit");

        let invocation_delta = invocation.delta().await;
        assert!(invocation_delta.receipt().contains("subagent.txt"));
        assert!(!invocation_delta.receipt().contains("primary.txt"));

        let outer_delta = outer.delta().await;
        assert!(outer_delta.receipt().contains("primary.txt"));
        assert!(outer_delta.receipt().contains("subagent.txt"));

        let second_invocation = WorkspaceSnapshot::capture(&[root.to_path_buf()]).await;
        std::fs::write(root.join("followup.txt"), "second subagent call\n").expect("followup");
        let second_delta = second_invocation.delta().await;
        assert!(second_delta.receipt().contains("followup.txt"));
        assert!(!second_delta.receipt().contains("primary.txt"));
        assert!(!second_delta.receipt().contains("subagent.txt"));
    }

    #[tokio::test]
    async fn unborn_repository_and_non_git_root_fail_open() {
        let unborn = tempfile::tempdir().expect("unborn");
        init_repo(unborn.path());
        let snapshot = WorkspaceSnapshot::capture(&[unborn.path().to_path_buf()]).await;
        std::fs::write(unborn.path().join("first.txt"), "first\n").expect("first file");
        let delta = snapshot.delta().await;
        assert!(delta.changed());
        assert!(delta.receipt().contains("first.txt"));

        let non_git = tempfile::tempdir().expect("non-git");
        let snapshot = WorkspaceSnapshot::capture(&[non_git.path().to_path_buf()]).await;
        let delta = snapshot.delta().await;
        assert!(!delta.changed());
        assert!(delta.receipt().contains("not a Git worktree"));
    }

    #[test]
    fn bounded_delta_preserves_head_and_tail_with_marker() {
        let text = format!("HEAD{}TAIL", "x".repeat(256));
        let bounded = bound_text(text, 80);
        assert!(bounded.starts_with("HEAD"));
        assert!(bounded.ends_with("TAIL"));
        assert!(bounded.contains("workspace delta truncated"));
        assert!(bounded.len() <= 80);
    }
}
