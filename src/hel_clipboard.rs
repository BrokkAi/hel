//! Small cross-platform clipboard boundary shared by terminal text editors.

use anyhow::{Context, Result};

pub fn read_text() -> Result<String> {
    arboard::Clipboard::new()
        .context("open system clipboard")?
        .get_text()
        .context("read text from system clipboard")
}

/// Writes `text` to the system clipboard.
///
/// Callers must run this off the render loop: opening the platform clipboard
/// blocks.
pub fn write_text(text: &str) -> Result<()> {
    arboard::Clipboard::new()
        .context("open system clipboard")?
        .set_text(text)
        .context("write text to system clipboard")
}
