//! Document open/save against a storage backend (issue #518).
//!
//! The CLI's `store-*` commands move raw bytes; this module moves *documents*: it serialises
//! first (pure bytes, never on the typing path) and then talks to any
//! [`StorageBackend`][wordcraft_storage::StorageBackend] — a local folder today, an oCIS
//! collection over WebDAV Bearer auth tomorrow — so a `.docx` round-trips backend → engine
//! [`Session`][crate::Session] → backend with the backend carrying the bytes exactly.
//!
//! Two layers, mirroring [`crate::io`]:
//! - [`open_from_storage`] / [`save_to_storage`] are free functions over any
//!   [`Document`][wordcraft_doc::Document] (headless use: MCP, CLI, tests).
//! - [`Session::open_from_storage`][crate::Session::open_from_storage] /
//!   [`Session::save_to_storage`][crate::Session::save_to_storage] do the same and keep the
//!   session consistent (history reset, caret, password, dirty flag), exactly as `file.open`
//!   and `file.save` do for local files.
//!
//! The write adapts to the backend's [`capabilities`][wordcraft_storage::StorageBackend::capabilities]:
//! backends with atomic staging (local folders) save atomically (stage, journal, rename); a
//! plain `PUT` is already whole-or-nothing, so WebDAV saves without it. Saves are
//! last-writer-wins (no `If-Match`): the session API mirrors `file.save`, not optimistic
//! concurrency — callers that need that use the backend's [`WriteOptions`][wordcraft_storage::WriteOptions]
//! directly.
//!
//! There is intentionally no `file.openFromStorage` command: credentials must not flow through
//! the command registry (params land in Repeat and recorded macros, which is why the session
//! password is excluded from them too). Hosts construct the backend — with its secret — and
//! call this API around it.
//!
//! Not built on `wasm32` (as [`crate::io::save_path_with`]: files go through the browser
//! there, and the WebDAV backend needs sockets).

use wordcraft_doc::Document;
use wordcraft_storage::{FileMeta, StorageBackend};

use crate::Session;
use crate::io::ExportOptions;

/// Read the document stored under `path` on `backend` and parse it as `path`'s format (see
/// [`crate::io::open_bytes_with`]). Returns the document and whether it was encrypted.
/// Storage failures name `path` (backend errors are path-free by contract); parse errors are
/// reported as [`crate::io`] reports them.
pub fn open_from_storage(backend: &dyn StorageBackend, path: &str, password: Option<&str>) -> Result<(Document, bool), String> {
    let bytes = backend.read(path).map_err(|e| format!("{path}: {e}"))?;
    crate::io::open_bytes_with(path, &bytes, password)
}

/// Serialise `doc` as `path`'s format (see [`crate::io::save_bytes_with`]) and store the bytes
/// under `path` on `backend`, creating parent collections as needed. The write is atomic
/// where the backend stages (local folders) and a single whole-or-nothing `PUT` elsewhere.
/// Returns what the backend reports about the stored file (size, ETag).
pub fn save_to_storage(
    backend: &dyn StorageBackend,
    path: &str,
    doc: &Document,
    password: Option<&str>,
    opts: &ExportOptions,
) -> Result<FileMeta, String> {
    let bytes = crate::io::save_bytes_with(path, doc, password, opts)?;
    let write_opts = wordcraft_storage::WriteOptions { if_match: None, atomic: backend.capabilities().atomic_write };
    backend.write(path, &bytes, &write_opts).map_err(|e| format!("{path}: {e}"))
}

/// Formats whose save keeps the whole document, so saving clears the dirty flag — the same
/// list `file.save` uses (other formats are lossy copies: PDF, text, HTML…).
fn is_full_fidelity(path: &str) -> bool {
    let ext = path.rsplit(['/', '\\']).next().unwrap_or(path).rsplit_once('.').map(|(_, ext)| ext.to_ascii_lowercase()).unwrap_or_default();
    matches!(ext.as_str(), "docx" | "docm" | "dotx" | "dotm" | "odt" | "rtf" | "json" | "xml")
}

/// The file-name extension of `path`, lower-case; empty when there is none.
fn ext_of(path: &str) -> String {
    path.rsplit(['/', '\\']).next().unwrap_or(path).rsplit_once('.').map(|(_, ext)| ext.to_ascii_lowercase()).unwrap_or_default()
}

impl Session {
    /// Replace this session's document with the one stored under `path` on `backend` (a
    /// password-protected Word document is decrypted with `password`). History, selection and
    /// layout are reset as `file.open` resets them; the local file path is cleared (this
    /// document lives on the backend now); a document opened with a password is saved with it
    /// again, as `file.open` does. Returns whether the stored file was encrypted.
    pub fn open_from_storage(&mut self, backend: &dyn StorageBackend, path: &str, password: Option<&str>) -> Result<bool, String> {
        let (mut doc, encrypted) = open_from_storage(backend, path, password)?;
        // Plain text and Markdown carry no language: they are in the editing language.
        if let Some(lang) = self.editing_lang.clone()
            && matches!(ext_of(path).as_str(), "txt" | "text" | "md" | "markdown")
        {
            doc.styles.default_chr.lang = Some(lang);
        }
        self.set_document(doc);
        self.path = None;
        if encrypted {
            self.password = password.map(crate::io::Password::new);
        }
        Ok(encrypted)
    }

    /// Save this session's document under `path` on `backend` with the session's password and
    /// export options, as `file.save` saves it to a local file. `file.save` marks the
    /// document saved (`stamp_save`) before writing, so a failed save still leaves the
    /// pre-save undo state behind rather than a half-written file; this does the same. The
    /// dirty flag is cleared for full-fidelity formats only. Returns what the backend reports
    /// about the stored file (size, ETag).
    pub fn save_to_storage(&mut self, backend: &dyn StorageBackend, path: &str, opts: &ExportOptions) -> Result<FileMeta, String> {
        let saved_password = self.password.clone();
        let password = saved_password.as_ref().map(crate::io::Password::as_str);
        self.stamp_save();
        let meta = save_to_storage(backend, path, &self.doc, password, opts)?;
        if is_full_fidelity(path) {
            self.dirty = false;
        }
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wordcraft_doc::StoryRef;
    use wordcraft_storage::local::LocalBackend;

    struct Scratch {
        path: std::path::PathBuf,
    }

    impl Scratch {
        fn create(name: &str) -> Scratch {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("wordcraft-engine-storage-{name}-{}-{id}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Scratch { path }
        }

        fn backend(&self) -> LocalBackend {
            LocalBackend::open(&self.path).unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn text_of(doc: &Document) -> String {
        doc.plain_text(StoryRef::Body)
    }

    #[test]
    fn docx_round_trips_backend_to_session_to_backend() {
        let scratch = Scratch::create("docx");
        let store = scratch.backend();
        let opts = ExportOptions::default();
        let doc = crate::sample::sample_document();
        let before = text_of(&doc);

        // Free functions: the backend carries exactly the serialised bytes.
        let expected = crate::io::save_bytes_with("handbook.docx", &doc, None, &opts).unwrap();
        let meta = save_to_storage(&store, "handbook.docx", &doc, None, &opts).unwrap();
        assert_eq!(meta.size, expected.len() as u64);
        assert_eq!(store.read("handbook.docx").unwrap(), expected);

        // Into a session, back out under another name: the text survives both crossings.
        let mut session = Session::new(crate::sample::sample_document());
        assert!(!session.open_from_storage(&store, "handbook.docx", None).unwrap());
        assert!(session.path.is_none(), "a storage document has no local path");
        assert!(!session.can_undo(), "opening resets history");
        assert!(text_of(&session.doc).contains(before.lines().next().unwrap_or("Open Studio")));
        let meta2 = session.save_to_storage(&store, "handbook-copy.docx", &opts).unwrap();
        assert!(!session.dirty, "a docx save clears the dirty flag");
        assert_eq!(store.read("handbook-copy.docx").unwrap().len() as u64, meta2.size);

        let mut reopened = Session::new(Document::new());
        reopened.open_from_storage(&store, "handbook-copy.docx", None).unwrap();
        assert_eq!(text_of(&reopened.doc), text_of(&session.doc));
    }

    #[test]
    fn lossy_format_save_keeps_the_dirty_flag() {
        let scratch = Scratch::create("lossy");
        let store = scratch.backend();
        let opts = ExportOptions::default();
        let mut session = Session::new(crate::sample::sample_document());
        session.dirty = true;
        session.save_to_storage(&store, "notes.txt", &opts).unwrap();
        assert!(session.dirty, "a text copy is lossy: the document still needs saving");
        assert!(store.read("notes.txt").unwrap().starts_with(b"The Open Studio"));
    }

    #[test]
    fn storage_failures_name_the_path() {
        let scratch = Scratch::create("missing");
        let store = scratch.backend();
        let err = open_from_storage(&store, "gone/memo.docx", None).unwrap_err();
        assert!(err.starts_with("gone/memo.docx: "), "{err}");
        let mut session = Session::new(Document::new());
        let err = session.open_from_storage(&store, "gone/memo.docx", None).unwrap_err();
        assert!(err.starts_with("gone/memo.docx: "), "{err}");
    }

    #[test]
    fn webdav_bearer_backend_builds_without_io() {
        // No sockets here: construction, URL shaping and scheme guards only.
        let bearer = wordcraft_storage::webdav::WebDavBackend::with_bearer("http://ocis:9200/remote.php/dav/files/user/", "t").unwrap();
        assert_eq!(bearer.base_url(), "http://ocis:9200/remote.php/dav/files/user/");
        assert!(wordcraft_storage::webdav::WebDavBackend::with_bearer("https://ocis:9200/dav/", "t").is_err(), "no TLS in this build");
        assert!(wordcraft_storage::webdav::WebDavBackend::with_bearer("http://ocis:9200/dav/", "").is_err(), "empty tokens never authenticate");
    }
}
