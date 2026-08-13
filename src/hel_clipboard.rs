//! Small cross-platform clipboard boundary shared by terminal text editors.

use anyhow::{Context, Result};

pub fn read_text() -> Result<String> {
    arboard::Clipboard::new()
        .context("open system clipboard")?
        .get_text()
        .context("read text from system clipboard")
}
