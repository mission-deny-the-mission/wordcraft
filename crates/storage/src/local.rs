//! The local-filesystem backend: a folder on disk, a NAS mount, or any synced folder.
//!
//! Writes with [`WriteOptions::atomic`][crate::WriteOptions::atomic] follow the engine's own
//! `safe_save` shape: stage a sibling temporary file (private `0600` while it holds bytes the
//! destination may not have had), `fsync` it, journal the pending commit, rename over the
//! destination, then journal the commit. A crash between the lines is finished or dropped by
//! [`recover`][crate::StorageBackend::recover]. Non-atomic writes go straight to the final name
//! (parents created first). Conditional writes (`if_match`) compare a sidecar `.etag` file the
//! backend maintains per document; a mismatch is [`StorageError::Conflict`].

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::journal::{self, JournalEntry, JournalState};
use crate::{Capabilities, Entry, FileMeta, RecoveryReport, StorageBackend, StorageError, WriteOptions, clean_path};

/// A folder on disk holding documents. `root` must be (or become) a directory.
#[derive(Clone, Debug)]
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Open (creating) the folder at `root`.
    pub fn open(root: &Path) -> Result<LocalBackend, StorageError> {
        if let Ok(meta) = fs::symlink_metadata(root)
            && !meta.is_dir()
        {
            return Err(StorageError::BadPath(format!("{}: not a directory", root.display())));
        }
        fs::create_dir_all(root).map_err(|e| StorageError::Io(format!("{}: {e}", root.display())))?;
        Ok(LocalBackend { root: root.to_path_buf() })
    }

    /// The folder this backend stores in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn journal_path(&self) -> PathBuf {
        self.root.join(journal::JOURNAL_FILE)
    }

    fn append_journal(&self, entry: &JournalEntry) {
        // The journal is best-effort crash evidence, never a reason to fail a save: a lost line
        // only means recovery has one fewer hint. (Errors are ignored deliberately.)
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(self.journal_path()) {
            let _ = f.write_all(journal::line(entry).as_bytes());
        }
    }

    fn read_etag(&self, dest: &Path) -> Option<String> {
        fs::read_to_string(etag_path(dest)).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
    }

    fn write_etag(&self, dest: &Path, meta: &FileMeta) {
        if let Some(tag) = &meta.etag
            && let Some(parent) = dest.parent()
        {
            let _ = fs::create_dir_all(parent);
            let _ = fs::write(etag_path(dest), tag.as_bytes());
        }
    }

    fn meta_of(&self, rel: &str, dest: &Path) -> Result<FileMeta, StorageError> {
        let meta = fs::metadata(dest).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(rel.into())
            } else {
                StorageError::Io(format!("{}: {e}", dest.display()))
            }
        })?;
        if !meta.is_file() {
            return Err(StorageError::NotFound(rel.into()));
        }
        let modified = meta.modified().ok().map(|t| format!("{t:?}"));
        Ok(FileMeta { size: meta.len(), etag: self.read_etag(dest), modified, is_dir: false })
    }

    /// Stage `bytes` beside `dest`, journal the pending write, rename into place, journal the
    /// commit. Returns the staged temp path's file name (for the journal).
    fn atomic_commit(&self, rel: &str, dest: &Path, bytes: &[u8]) -> Result<FileMeta, StorageError> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::Io(format!("{}: {e}", parent.display())))?;
        }
        let keep_mode = match fs::metadata(dest) {
            Ok(m) if m.is_file() => Some(m.permissions()),
            Ok(_) => return Err(StorageError::Conflict(format!("{rel}: a folder is stored there"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(StorageError::Io(format!("{}: {e}", dest.display()))),
        };
        let fname = dest.file_name().and_then(|n| n.to_str()).unwrap_or("doc");
        for _ in 0..128 {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let tmp = dest.with_file_name(format!(".wordcraft-save-{id}-{fname}.tmp"));
            let mut opts = OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                // Stay private until the destination's own mode is applied below.
                opts.mode(0o600);
            }
            let mut f = match opts.open(&tmp) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(StorageError::Io(format!("{}: {e}", tmp.display()))),
            };
            let staged_name = tmp.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
            let entry = JournalEntry { path: rel.into(), staged: staged_name, state: JournalState::Pending, len: bytes.len() as u64 };
            if f.write_all(bytes).is_err() || f.sync_all().is_err() {
                let _ = fs::remove_file(&tmp);
                return Err(StorageError::Io(format!("{}: could not stage the write", dest.display())));
            }
            if let Some(mode) = keep_mode {
                // A replaced file keeps its mode; a new one keeps the umask default.
                if f.set_permissions(mode).is_err() {
                    let _ = fs::remove_file(&tmp);
                    return Err(StorageError::Io(format!("{}: could not stage the write", dest.display())));
                }
            }
            drop(f);
            self.append_journal(&entry);
            if let Err(e) = fs::rename(&tmp, dest) {
                let _ = fs::remove_file(&tmp);
                return Err(StorageError::Io(format!("{}: {e}", dest.display())));
            }
            self.append_journal(&JournalEntry { state: JournalState::Committed, ..entry });
            let tag = etag_of(bytes);
            let meta = FileMeta { size: bytes.len() as u64, etag: Some(tag), modified: None, is_dir: false };
            self.write_etag(dest, &meta);
            return Ok(meta);
        }
        Err(StorageError::Io(format!("{}: no free staging name after 128 tries", dest.display())))
    }
}

/// Sidecar holding a file's ETag: `<name>.wc-etag` beside it (hidden from [`list`][StorageBackend::list]).
fn etag_path(dest: &Path) -> PathBuf {
    let name = dest.file_name().and_then(|n| n.to_str()).unwrap_or("doc");
    dest.with_file_name(format!("{name}.wc-etag"))
}

/// Content tag: a short hex of a wrapping hash (FNV-1a, 64-bit). It only needs to change when
/// the bytes change *through this backend*; it is not a cryptographic digest.
fn etag_of(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

impl StorageBackend for LocalBackend {
    fn read(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let rel = clean_path(path)?;
        let dest = self.root.join(&rel);
        fs::read(&dest).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(rel)
            } else {
                StorageError::Io(format!("{}: {e}", dest.display()))
            }
        })
    }

    fn write(&self, path: &str, bytes: &[u8], opts: &WriteOptions) -> Result<FileMeta, StorageError> {
        let rel = clean_path(path)?;
        if bytes.len() as u64 > (2 << 30) {
            return Err(StorageError::Unsupported(format!("{rel}: larger than 2 GB")));
        }
        let dest = self.root.join(&rel);
        if let Some(want) = &opts.if_match {
            match self.read_etag(&dest) {
                Some(have) if &have == want => {}
                _ if !dest.exists() => return Err(StorageError::Conflict(format!("{rel}: no stored version matches `{want}`"))),
                _ => return Err(StorageError::Conflict(format!("{rel}: stored version changed (stale If-Match)"))),
            }
        }
        if opts.atomic {
            return self.atomic_commit(&rel, &dest, bytes);
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::Io(format!("{}: {e}", parent.display())))?;
        }
        let mut f = File::create(&dest).map_err(|e| StorageError::Io(format!("{}: {e}", dest.display())))?;
        f.write_all(bytes).map_err(|e| StorageError::Io(format!("{}: {e}", dest.display())))?;
        let meta = FileMeta { size: bytes.len() as u64, etag: Some(etag_of(bytes)), modified: None, is_dir: false };
        self.write_etag(&dest, &meta);
        Ok(meta)
    }

    fn stat(&self, path: &str) -> Result<FileMeta, StorageError> {
        let rel = clean_path(path)?;
        let dest = self.root.join(&rel);
        self.meta_of(&rel, &dest)
    }

    fn list(&self, prefix: &str) -> Result<Vec<Entry>, StorageError> {
        let dir = if prefix.is_empty() { self.root.clone() } else { self.root.join(clean_path(prefix)?) };
        let rd = fs::read_dir(&dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(prefix.into())
            } else {
                StorageError::Io(format!("{}: {e}", dir.display()))
            }
        })?;
        let mut out = Vec::new();
        for ent in rd {
            let ent = ent.map_err(|e| StorageError::Io(e.to_string()))?;
            let name = ent.file_name().to_string_lossy().into_owned();
            if name.starts_with(".wordcraft-save-") || name.ends_with(".tmp") || name.ends_with(".wc-etag") || name == journal::JOURNAL_FILE {
                continue;
            }
            let ft = ent.file_type().map_err(|e| StorageError::Io(e.to_string()))?;
            let rel = if prefix.is_empty() { name.clone() } else { format!("{}/{}", prefix.trim_end_matches('/'), name) };
            if ft.is_dir() {
                out.push(Entry { path: rel, meta: FileMeta { size: 0, etag: None, modified: None, is_dir: true } });
            } else if ft.is_file() {
                let size = ent.metadata().map(|m| m.len()).unwrap_or(0);
                out.push(Entry { path: rel, meta: FileMeta { size, etag: self.read_etag(&ent.path()), modified: None, is_dir: false } });
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    fn remove(&self, path: &str) -> Result<(), StorageError> {
        let rel = clean_path(path)?;
        let dest = self.root.join(&rel);
        match fs::remove_file(&dest) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StorageError::Io(format!("{}: {e}", dest.display()))),
        }
        let _ = fs::remove_file(etag_path(&dest));
        Ok(())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { atomic_write: true, conditional_write: true, list: true, etag: true, persistent: true }
    }

    fn recover(&self) -> Result<RecoveryReport, StorageError> {
        let mut report = RecoveryReport::default();
        let bytes = fs::read(self.journal_path()).unwrap_or_default();
        let plan = journal::RecoveryPlan::plan(&journal::read(&bytes));
        for pending in &plan.complete {
            let Ok(rel) = clean_path(&pending.path) else {
                report.dropped += 1;
                continue;
            };
            let staged = self.root.join(&pending.staged);
            let dest = self.root.join(&rel);
            let staged_bytes = fs::read(&staged).ok();
            match staged_bytes {
                Some(b) if b.len() as u64 == pending.len && dest.exists() => {
                    // Final file survived alongside the staged one: the rename may or may not
                    // have happened, but the document is intact either way.
                    let _ = fs::remove_file(&staged);
                    report.completed += 1;
                }
                Some(b) if b.len() as u64 == pending.len => {
                    if fs::rename(&staged, &dest).is_ok() {
                        let meta = FileMeta { size: b.len() as u64, etag: Some(etag_of(&b)), modified: None, is_dir: false };
                        self.write_etag(&dest, &meta);
                        report.completed += 1;
                    } else {
                        report.dropped += 1;
                    }
                }
                _ => {
                    let _ = fs::remove_file(&staged);
                    report.dropped += 1;
                }
            }
            self.append_journal(&JournalEntry { state: JournalState::Committed, ..pending.clone() });
        }
        report.completed += plan.done.len();
        report.removed_temp = remove_orphan_temps(&self.root);
        // The log has served its purpose; a fresh one starts now.
        let _ = fs::remove_file(self.journal_path());
        Ok(report)
    }
}

/// Delete staging files no journal entry names. Returns how many were removed.
fn remove_orphan_temps(root: &Path) -> usize {
    let mut removed = 0;
    let mut stack = vec![root.to_path_buf()];
    let mut seen = 0;
    while let Some(dir) = stack.pop() {
        seen += 1;
        if seen > 4096 {
            break;
        }
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for ent in rd.flatten() {
            let name = ent.file_name().to_string_lossy().into_owned();
            let Ok(ft) = ent.file_type() else { continue };
            if ft.is_dir() {
                stack.push(ent.path());
            } else if ft.is_file() && name.starts_with(".wordcraft-save-") && fs::remove_file(ent.path()).is_ok() {
                removed += 1;
            }
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> (TempDir, LocalBackend) {
        let dir = TempDir::create();
        let b = LocalBackend::open(dir.path()).unwrap();
        (dir, b)
    }

    /// A unique scratch folder under the system temp dir.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl TempDir {
        fn create() -> TempDir {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("wordcraft-storage-test-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }
    }

    #[test]
    fn atomic_roundtrip_and_etag_match() {
        let (_tmp, b) = backend();
        let meta = b.write("memo.docx", b"PK-data", &WriteOptions { atomic: true, ..Default::default() }).unwrap();
        assert_eq!(b.read("memo.docx").unwrap(), b"PK-data");
        // `stat` agrees on content identity (`modified` is time-dependent, so compared separately).
        let st = b.stat("memo.docx").unwrap();
        assert_eq!((st.size, st.etag, st.is_dir), (meta.size, meta.etag.clone(), false));
        assert!(st.modified.is_some());
        // Stale tag loses.
        let err = b.write("memo.docx", b"new", &WriteOptions { if_match: Some("deadbeef".into()), ..Default::default() }).unwrap_err();
        assert!(matches!(err, StorageError::Conflict(_)));
        // Fresh tag wins.
        let tag = meta.etag.unwrap();
        b.write("memo.docx", b"new", &WriteOptions { if_match: Some(tag), atomic: true }).unwrap();
        assert_eq!(b.read("memo.docx").unwrap(), b"new");
    }

    #[test]
    fn no_half_file_visible() {
        let (_tmp, b) = backend();
        b.write("a/b/c.txt", b"hello", &WriteOptions { atomic: true, ..Default::default() }).unwrap();
        let leftovers: Vec<_> = fs::read_dir(b.root()).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().starts_with(".wordcraft-save-")).collect();
        assert!(leftovers.is_empty());
        assert_eq!(b.list("a/b").unwrap().len(), 1);
    }

    #[test]
    fn recover_finishes_interrupted_commit() {
        let (_tmp, b) = backend();
        // Fake a crash: staged bytes + pending line, no commit line, no final file.
        let staged = ".wordcraft-save-0-crash.txt.tmp";
        fs::write(b.root().join(staged), b"saved?").unwrap();
        b.append_journal(&JournalEntry { path: "crash.txt".into(), staged: staged.into(), state: JournalState::Pending, len: 6 });
        let report = b.recover().unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(b.read("crash.txt").unwrap(), b"saved?");
    }

    #[test]
    fn remove_is_idempotent() {
        let (_tmp, b) = backend();
        b.remove("missing.docx").unwrap();
        b.write("x.docx", b"d", &WriteOptions::default()).unwrap();
        b.remove("x.docx").unwrap();
        assert!(matches!(b.stat("x.docx").unwrap_err(), StorageError::NotFound(_)));
    }
}
