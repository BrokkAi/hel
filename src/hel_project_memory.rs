//! Project-scoped persistent memory shared across harnesses.
//!
//! A server instance is bound to exactly one project replica. Model-facing
//! paths are virtual absolute paths rooted at that replica; controller and
//! target filesystem paths never cross the MCP boundary.

use std::fs;
use std::io::{BufRead, Write};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MEMORY_INDEX: &str = "MEMORY.md";
pub const MAX_DOCUMENT_BYTES: usize = 100 * 1024;
pub const MAX_VIRTUAL_PATH_BYTES: usize = 1024;
pub const LIST_PAGE_SIZE: usize = 50;
pub const STARTUP_INDEX_LINES: usize = 200;
pub const STARTUP_INDEX_BYTES: usize = 25 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RepositoryMemoryIdentity {
    Github {
        owner: String,
        repository: String,
    },
    Local {
        canonical_root: PathBuf,
    },
    Remote {
        target: String,
        canonical_root: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectMemoryIdentity {
    Repository {
        repository: RepositoryMemoryIdentity,
    },
    Bundle {
        primary: RepositoryMemoryIdentity,
        members: Vec<RepositoryMemoryIdentity>,
    },
}

impl ProjectMemoryIdentity {
    /// Stable, non-secret directory key for controller-side memory storage.
    pub fn key(&self) -> Result<String> {
        let encoded = serde_json::to_vec(self).context("encode project memory identity")?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }

    pub fn bundle(
        primary: RepositoryMemoryIdentity,
        mut members: Vec<RepositoryMemoryIdentity>,
    ) -> Self {
        members.sort_by_key(identity_sort_key);
        members.dedup();
        Self::Bundle { primary, members }
    }
}

fn identity_sort_key(identity: &RepositoryMemoryIdentity) -> String {
    serde_json::to_string(identity).expect("repository memory identity is serializable")
}

#[derive(Debug, Clone)]
pub struct ProjectMemoryStore {
    root: PathBuf,
}

impl ProjectMemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn list(&self, path_prefix: Option<&str>, cursor: Option<&str>) -> MemoryListResult {
        match self.try_list(path_prefix, cursor) {
            Ok(result) => result,
            Err(error) => MemoryListResult::failed(error),
        }
    }

    fn try_list(
        &self,
        path_prefix: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<MemoryListResult> {
        let prefix = path_prefix.unwrap_or("/");
        let relative = validate_virtual_path(prefix, true)?;
        let start = self.root.join(&relative);
        reject_symlink_path(&self.root, &relative, true)?;

        let mut entries = Vec::new();
        if start.is_file() {
            entries.push(memory_entry(&self.root, &start)?);
        } else if start.is_dir() {
            collect_entries(&self.root, &start, &mut entries)?;
        } else if start.exists() {
            bail!("memory path is not a regular file or directory");
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        if let Some(cursor) = cursor {
            validate_virtual_path(cursor, false)?;
            entries.retain(|entry| entry.path.as_str() > cursor);
        }
        let remaining = entries.len().saturating_sub(LIST_PAGE_SIZE);
        entries.truncate(LIST_PAGE_SIZE);
        Ok(MemoryListResult {
            outcome: MemoryListOutcome::Ok,
            entries: Some(entries),
            remaining: Some(remaining),
            reason: None,
            message: None,
        })
    }

    pub fn read(&self, path: &str) -> MemoryReadResult {
        match self.try_read(path) {
            Ok(result) => result,
            Err(error) => MemoryReadResult::failed(path, error),
        }
    }

    fn try_read(&self, path: &str) -> Result<MemoryReadResult> {
        let relative = validate_virtual_path(path, false)?;
        reject_symlink_path(&self.root, &relative, false)?;
        let host_path = self.root.join(relative);
        let metadata = match fs::metadata(&host_path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => bail!("memory path is not a regular file"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(MemoryReadResult::not_found(path));
            }
            Err(error) => return Err(error.into()),
        };
        let bytes = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
        if bytes > MAX_DOCUMENT_BYTES {
            return Ok(MemoryReadResult::refused(
                path,
                "too_large",
                format!("{path:?} is {bytes} bytes, over the {MAX_DOCUMENT_BYTES}-byte read cap"),
            ));
        }
        let content = fs::read_to_string(&host_path)
            .with_context(|| format!("read memory document {}", host_path.display()))?;
        Ok(MemoryReadResult {
            outcome: MemoryReadOutcome::Ok,
            path: path.to_owned(),
            content: Some(content.clone()),
            updated_at: modified_at(&metadata),
            version: Some(content_version(&content)),
            reason: None,
            message: None,
        })
    }

    pub fn write(&self, request: MemoryWriteRequest) -> MemoryWriteResult {
        match self.try_write(request) {
            Ok(result) => result,
            Err(error) => MemoryWriteResult::failed(error),
        }
    }

    fn try_write(&self, request: MemoryWriteRequest) -> Result<MemoryWriteResult> {
        let relative = validate_virtual_path(&request.path, false)?;
        reject_symlink_path(&self.root, &relative, true)?;
        let content = normalize_content(&request.content);
        let bytes = content.len();
        if content.trim().is_empty() {
            return Ok(MemoryWriteResult::refused(
                request.path,
                "empty_content",
                "empty or whitespace-only content is rejected",
            ));
        }
        if bytes > MAX_DOCUMENT_BYTES {
            return Ok(MemoryWriteResult::refused(
                request.path,
                "too_large",
                format!(
                    "content is {bytes} bytes; a memory document is capped at {MAX_DOCUMENT_BYTES} bytes"
                ),
            ));
        }

        let host_path = self.root.join(&relative);
        let existing = match fs::read_to_string(&host_path) {
            Ok(content) => Some(content),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let requested_version = request.if_version.trim();
        match (existing.as_ref(), requested_version) {
            (None, "" | "new") => {}
            (None, _) => {
                return Ok(MemoryWriteResult::missing(request.path));
            }
            (Some(current), "" | "new") => {
                return Ok(MemoryWriteResult::conflict(request.path, current));
            }
            (Some(current), expected) if content_version(current) != expected => {
                return Ok(MemoryWriteResult::conflict(request.path, current));
            }
            (Some(_), _) => {}
        }

        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create memory directory {}", parent.display()))?;
        }
        crate::hel_config::atomic_write(&host_path, content.as_bytes())
            .with_context(|| format!("write memory document {}", host_path.display()))?;
        let op = if existing.is_some() {
            MemoryWriteOperation::Updated
        } else {
            MemoryWriteOperation::Created
        };
        let message = (relative == Path::new(MEMORY_INDEX))
            .then(|| startup_index_warning(&content))
            .flatten();
        Ok(MemoryWriteResult {
            outcome: MemoryWriteOutcome::Ok,
            path: request.path,
            version: Some(content_version(&content)),
            bytes: Some(bytes),
            op: Some(op),
            current_version: None,
            current_content: None,
            reason: None,
            message,
        })
    }

    pub fn startup_index(&self) -> Result<Option<String>> {
        let path = self.root.join(MEMORY_INDEX);
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(truncate_startup_index(&content)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryListOutcome {
    Ok,
    Refused,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryListEntry {
    pub path: String,
    pub bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryListResult {
    pub outcome: MemoryListOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<MemoryListEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl MemoryListResult {
    fn failed(error: anyhow::Error) -> Self {
        Self {
            outcome: MemoryListOutcome::Failed,
            entries: None,
            remaining: None,
            reason: Some("invalid_or_unavailable".into()),
            message: Some(format!("{error:#}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReadOutcome {
    Ok,
    NotFound,
    Refused,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryReadResult {
    pub outcome: MemoryReadOutcome,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl MemoryReadResult {
    fn not_found(path: &str) -> Self {
        Self {
            outcome: MemoryReadOutcome::NotFound,
            path: path.into(),
            content: None,
            updated_at: None,
            version: None,
            reason: Some("not_found".into()),
            message: None,
        }
    }

    fn refused(path: &str, reason: &str, message: String) -> Self {
        Self {
            outcome: MemoryReadOutcome::Refused,
            path: path.into(),
            content: None,
            updated_at: None,
            version: None,
            reason: Some(reason.into()),
            message: Some(message),
        }
    }

    fn failed(path: &str, error: anyhow::Error) -> Self {
        Self {
            outcome: MemoryReadOutcome::Failed,
            path: path.into(),
            content: None,
            updated_at: None,
            version: None,
            reason: Some("invalid_or_unavailable".into()),
            message: Some(format!("{error:#}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryWriteRequest {
    pub path: String,
    pub content: String,
    pub if_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryWriteOutcome {
    Ok,
    Conflict,
    Missing,
    Refused,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryWriteOperation {
    Created,
    Updated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryWriteResult {
    pub outcome: MemoryWriteOutcome,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<MemoryWriteOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl MemoryWriteResult {
    fn conflict(path: String, current: &str) -> Self {
        Self {
            outcome: MemoryWriteOutcome::Conflict,
            path,
            version: None,
            bytes: None,
            op: None,
            current_version: Some(content_version(current)),
            current_content: Some(current.into()),
            reason: Some("conflict".into()),
            message: Some("the document changed; reconcile with currentContent and retry".into()),
        }
    }

    fn missing(path: String) -> Self {
        Self {
            outcome: MemoryWriteOutcome::Missing,
            path,
            version: None,
            bytes: None,
            op: None,
            current_version: None,
            current_content: None,
            reason: Some("not_found".into()),
            message: Some("the document does not exist; pass if_version=new to create it".into()),
        }
    }

    fn refused(path: String, reason: &str, message: impl Into<String>) -> Self {
        Self {
            outcome: MemoryWriteOutcome::Refused,
            path,
            version: None,
            bytes: None,
            op: None,
            current_version: None,
            current_content: None,
            reason: Some(reason.into()),
            message: Some(message.into()),
        }
    }

    fn failed(error: anyhow::Error) -> Self {
        Self {
            outcome: MemoryWriteOutcome::Failed,
            path: String::new(),
            version: None,
            bytes: None,
            op: None,
            current_version: None,
            current_content: None,
            reason: Some("invalid_or_unavailable".into()),
            message: Some(format!("{error:#}")),
        }
    }
}

pub fn truncate_startup_index(content: &str) -> String {
    let mut output = String::new();
    for (index, line) in content.lines().enumerate() {
        if index >= STARTUP_INDEX_LINES {
            break;
        }
        let separator = usize::from(!output.is_empty());
        if output.len() + separator + line.len() > STARTUP_INDEX_BYTES {
            let remaining = STARTUP_INDEX_BYTES.saturating_sub(output.len() + separator);
            let boundary = floor_char_boundary(line, remaining);
            if separator == 1 && boundary > 0 {
                output.push('\n');
            }
            output.push_str(&line[..boundary]);
            break;
        }
        if separator == 1 {
            output.push('\n');
        }
        output.push_str(line);
    }
    output
}

fn startup_index_warning(content: &str) -> Option<String> {
    let lines = content.lines().count();
    (lines > STARTUP_INDEX_LINES || content.len() > STARTUP_INDEX_BYTES).then(|| {
        format!(
            "write succeeded, but new sessions load only the first {STARTUP_INDEX_LINES} lines or {STARTUP_INDEX_BYTES} bytes of {MEMORY_INDEX}"
        )
    })
}

fn floor_char_boundary(text: &str, maximum: usize) -> usize {
    let mut boundary = maximum.min(text.len());
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn content_version(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))[..12].to_owned()
}

fn normalize_content(content: &str) -> String {
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    normalized
        .chars()
        .filter_map(|character| {
            if is_format_character(character) {
                None
            } else if character.is_control() && !matches!(character, '\n' | '\t') {
                Some('\u{fffd}')
            } else {
                Some(character)
            }
        })
        .collect()
}

fn is_format_character(character: char) -> bool {
    matches!(
        character,
        '\u{00ad}'
            | '\u{061c}'
            | '\u{180e}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
    )
}

fn validate_virtual_path(path: &str, root_allowed: bool) -> Result<PathBuf> {
    if path.len() > MAX_VIRTUAL_PATH_BYTES {
        bail!("memory path exceeds {MAX_VIRTUAL_PATH_BYTES} bytes");
    }
    if !path.starts_with('/') || path.contains('\\') || path.chars().any(char::is_control) {
        bail!("memory path must be a safe virtual absolute path");
    }
    if path == "/" {
        return root_allowed
            .then(PathBuf::new)
            .ok_or_else(|| anyhow!("memory document path cannot be the root"));
    }
    let mut relative = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => {
                let text = segment.to_string_lossy();
                if text.starts_with('.')
                    || matches!(text.as_ref(), "skills" | "commands" | "agents" | "hooks")
                {
                    bail!("memory path contains a reserved segment");
                }
                relative.push(segment);
            }
            _ => bail!("memory path contains an unsafe segment"),
        }
    }
    if relative.as_os_str().is_empty() {
        bail!("memory document path cannot be empty");
    }
    Ok(relative)
}

fn reject_symlink_path(root: &Path, relative: &Path, missing_allowed: bool) -> Result<()> {
    let mut current = root.to_path_buf();
    if let Ok(metadata) = fs::symlink_metadata(root)
        && metadata.file_type().is_symlink()
    {
        bail!("memory root cannot be a symlink");
    }
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("memory path cannot traverse a symlink")
            }
            Ok(_) => {}
            Err(error) if missing_allowed && error.kind() == std::io::ErrorKind::NotFound => {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn collect_entries(root: &Path, directory: &Path, output: &mut Vec<MemoryListEntry>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("list memory directory {}", directory.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if entry.file_type()?.is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            collect_entries(root, &entry.path(), output)?;
        } else if metadata.is_file() && !entry.file_name().to_string_lossy().starts_with('.') {
            output.push(memory_entry(root, &entry.path())?);
        }
    }
    Ok(())
}

fn memory_entry(root: &Path, path: &Path) -> Result<MemoryListEntry> {
    let metadata = fs::metadata(path)?;
    let relative = path.strip_prefix(root)?;
    let rendered = format!("/{}", relative.to_string_lossy().replace('\\', "/"));
    Ok(MemoryListEntry {
        path: rendered,
        bytes: usize::try_from(metadata.len()).unwrap_or(usize::MAX),
        updated_at: modified_at(&metadata),
    })
}

fn modified_at(metadata: &fs::Metadata) -> Option<String> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .and_then(|duration| {
            DateTime::<Utc>::from_timestamp(
                i64::try_from(duration.as_secs()).ok()?,
                duration.subsec_nanos(),
            )
        })
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// Serve the project memory tools over MCP's JSON-lines stdio transport.
pub fn run_mcp_stdio(root: &Path) -> Result<()> {
    fs::create_dir_all(root)
        .with_context(|| format!("create project memory root {}", root.display()))?;
    let store = ProjectMemoryStore::new(root);
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    for line in stdin.lock().lines() {
        let line = line.context("read MCP request")?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                write_json_line(
                    &mut output,
                    &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":error.to_string()}}),
                )?;
                continue;
            }
        };
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => json_rpc_result(
                id,
                json!({
                    "protocolVersion": request.pointer("/params/protocolVersion").cloned().unwrap_or_else(|| json!("2025-03-26")),
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "hel-project-memory", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": "Persistent memory for the current Hel project. Use /MEMORY.md as the concise index."
                }),
            ),
            "ping" => json_rpc_result(id, json!({})),
            "tools/list" => json_rpc_result(id, json!({"tools": tool_definitions()})),
            "tools/call" => match call_tool(&store, request.get("params")) {
                Ok((structured, is_error)) => json_rpc_result(
                    id,
                    json!({
                        "content": [{"type":"text", "text": serde_json::to_string_pretty(&structured)?}],
                        "structuredContent": structured,
                        "isError": is_error
                    }),
                ),
                Err(error) => json_rpc_error(id, -32602, format!("{error:#}")),
            },
            _ => json_rpc_error(id, -32601, format!("unknown MCP method {method:?}")),
        };
        write_json_line(&mut output, &response)?;
    }
    Ok(())
}

fn call_tool(store: &ProjectMemoryStore, params: Option<&Value>) -> Result<(Value, bool)> {
    let params = params.context("tools/call is missing params")?;
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .context("tools/call is missing name")?;
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let result = match name {
        "memory_list" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Arguments {
                path_prefix: Option<String>,
                cursor: Option<String>,
            }
            let arguments: Arguments = serde_json::from_value(arguments)?;
            serde_json::to_value(store.list(
                arguments.path_prefix.as_deref(),
                arguments.cursor.as_deref(),
            ))?
        }
        "memory_read" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Arguments {
                path: String,
            }
            let arguments: Arguments = serde_json::from_value(arguments)?;
            serde_json::to_value(store.read(&arguments.path))?
        }
        "memory_write" => {
            let request: MemoryWriteRequest = serde_json::from_value(arguments)?;
            serde_json::to_value(store.write(request))?
        }
        _ => bail!("unknown memory tool {name:?}"),
    };
    let is_error = result
        .get("outcome")
        .and_then(Value::as_str)
        .is_some_and(|outcome| outcome != "ok");
    Ok((result, is_error))
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "memory_list",
            "description": "List persistent memory documents for the current Hel project.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path_prefix": {"type":"string", "description":"Optional virtual directory prefix, such as /roots/api/."},
                    "cursor": {"type":"string", "description":"Last path from the previous page."}
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "memory_read",
            "description": "Read one persistent memory document and its version token.",
            "inputSchema": {
                "type": "object",
                "properties": {"path": {"type":"string"}},
                "required": ["path"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "memory_write",
            "description": "Create or replace one persistent memory document using compare-and-swap.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type":"string"},
                    "content": {"type":"string", "description":"Full UTF-8 replacement content."},
                    "if_version": {"type":"string", "maxLength":64, "description":"Version from memory_read, or new when creating."}
                },
                "required": ["path", "content", "if_version"],
                "additionalProperties": false
            }
        }),
    ]
}

fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

fn json_rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code, "message":message}})
}

fn write_json_line(output: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(
        store: &ProjectMemoryStore,
        path: &str,
        content: &str,
        version: &str,
    ) -> MemoryWriteResult {
        store.write(MemoryWriteRequest {
            path: path.into(),
            content: content.into(),
            if_version: version.into(),
        })
    }

    #[test]
    fn bundle_identity_is_independent_of_member_order() {
        let primary = RepositoryMemoryIdentity::Github {
            owner: "brokkai".into(),
            repository: "hel".into(),
        };
        let worker = RepositoryMemoryIdentity::Github {
            owner: "brokkai".into(),
            repository: "worker".into(),
        };
        assert_eq!(
            ProjectMemoryIdentity::bundle(primary.clone(), vec![worker.clone(), primary.clone()]),
            ProjectMemoryIdentity::bundle(primary.clone(), vec![primary, worker]),
        );
    }

    #[test]
    fn write_uses_compare_and_swap_and_returns_current_content_on_conflict() {
        let directory = tempfile::tempdir().unwrap();
        let store = ProjectMemoryStore::new(directory.path());
        let created = write(&store, "/MEMORY.md", "first\n", "new");
        assert_eq!(created.outcome, MemoryWriteOutcome::Ok);
        let version = created.version.unwrap();

        let updated = write(&store, "/MEMORY.md", "second\n", &version);
        assert_eq!(updated.outcome, MemoryWriteOutcome::Ok);
        let conflict = write(&store, "/MEMORY.md", "stale\n", &version);
        assert_eq!(conflict.outcome, MemoryWriteOutcome::Conflict);
        assert_eq!(conflict.current_content.as_deref(), Some("second\n"));
    }

    #[test]
    fn list_is_sorted_paginated_and_root_scoped() {
        let directory = tempfile::tempdir().unwrap();
        let store = ProjectMemoryStore::new(directory.path());
        for index in (0..55).rev() {
            let path = format!("/topics/{index:02}.md");
            assert_eq!(
                write(&store, &path, "note", "new").outcome,
                MemoryWriteOutcome::Ok
            );
        }
        let first = store.list(Some("/topics"), None);
        let entries = first.entries.unwrap();
        assert_eq!(entries.len(), LIST_PAGE_SIZE);
        assert_eq!(first.remaining, Some(5));
        assert_eq!(entries[0].path, "/topics/00.md");
        let second = store.list(Some("/topics"), Some(&entries.last().unwrap().path));
        assert_eq!(second.entries.unwrap().len(), 5);
    }

    #[test]
    fn unsafe_paths_and_symlinks_are_never_followed() {
        let directory = tempfile::tempdir().unwrap();
        let store = ProjectMemoryStore::new(directory.path());
        assert_eq!(
            write(&store, "/../outside.md", "no", "new").outcome,
            MemoryWriteOutcome::Failed
        );
        assert_eq!(
            write(&store, "/.env", "TOKEN=secret", "new").outcome,
            MemoryWriteOutcome::Failed
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("/tmp", directory.path().join("escape")).unwrap();
            assert_eq!(
                write(&store, "/escape/no.md", "no", "new").outcome,
                MemoryWriteOutcome::Failed
            );
        }
    }

    #[test]
    fn startup_index_honors_both_limits_without_splitting_utf8() {
        let many_lines = (0..250).map(|_| "memory").collect::<Vec<_>>().join("\n");
        assert_eq!(truncate_startup_index(&many_lines).lines().count(), 200);
        let large = "é".repeat(20_000);
        let truncated = truncate_startup_index(&large);
        assert!(truncated.len() <= STARTUP_INDEX_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn memory_document_round_trip_exceeds_pipe_buffer_size() {
        let directory = tempfile::tempdir().unwrap();
        let store = ProjectMemoryStore::new(directory.path());
        let content = "x".repeat(70 * 1024);
        let result = write(&store, "/large.md", &content, "new");
        assert_eq!(result.outcome, MemoryWriteOutcome::Ok);
        assert_eq!(
            store.read("/large.md").content.as_deref(),
            Some(content.as_str())
        );
    }
}
