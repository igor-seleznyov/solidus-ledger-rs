use std::sync::Arc;
use mio::{Events, Interest, Poll, Token, Waker};
use std::sync::mpsc::{Sender, Receiver};
use libc::IN_CLOEXEC;
#[cfg(target_os = "linux")]
use mio::unix::SourceFd;
use crate::consts::FILE_PAGE_SIZE;
use crate::signature_verification_cache::SignatureVerificationCache;
use crate::signature_verifier::SignatureVerifyResult;

const INOTIFY_TOKEN: Token = Token(0);
const CHANNEL_TOKEN: Token = Token(1);

pub enum FileWatcherMessage{
    WatchFile { ls_path: String },
}

#[derive(Clone)]
pub struct FileWatcherSender {
    inner: Sender<FileWatcherMessage>,
    waker: Arc<Waker>,
}

impl FileWatcherSender {
    pub fn new(
        inner: Sender<FileWatcherMessage>,
        waker: Arc<Waker>,
    ) -> Self {
        Self { inner, waker }
    }
    
    pub fn send(&self, msg: FileWatcherMessage) -> std::io::Result<()> {
        self.inner.send(msg).map_err(
            |_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "File watcher channel closed",
                )
            }
        )?;
        self.waker.wake()?;
        Ok(())
    }
}

pub struct FileWatcher{
    id: usize,
    rx: Receiver<FileWatcherMessage>,
    poll: Poll,
    watch_directory: String,
    signature_cache: SignatureVerificationCache,
}

impl FileWatcher {
    pub fn new(
        id: usize,
        rx: Receiver<FileWatcherMessage>,
        watch_directory: String,
    ) -> std::io::Result<(Self, Arc<Waker>)> {
        let poll = Poll::new()?;
        let waker = Arc::new(
            Waker::new(
                poll.registry(),
                CHANNEL_TOKEN,
            )?
        );

        let watcher = Self {
            id,
            rx,
            poll,
            watch_directory,
            signature_cache: SignatureVerificationCache::new(),
        };

        Ok((watcher, waker))
    }

    pub fn run(&mut self) {
        println!(
            "[file-watcher {}] started, watching: {}",
            self.id,
            self.watch_directory
        );

        match self.run_platform() {
            Ok(()) => {
                println!("[file-watcher {}] stopped", self.id);
            }
            Err(error) => {
                eprintln!(
                    "[file-watcher {}] FATAL: file watcher failed: {}. Initiating shutdown.",
                    self.id, error,
                );
                std::process::exit(1);
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl FileWatcher{
    fn run_platform(&mut self) -> std::io::Result<()> {
        let inotify_fd = unsafe {
            libc::inotify_init1(libc::IN_NONBLOCK | IN_CLOEXEC)
        };
        if inotify_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let c_dir = std::ffi::CString::new(self.watch_directory.as_str())
            .map_err(
                |_| std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid watching directory path",
                )
            )?;

        let watch_mask = libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_DELETE;
        let wd = unsafe {
            libc::inotify_add_watch(
                inotify_fd,
                c_dir.as_ptr(),
                watch_mask,
            )
        };
        if wd < 0 {
            unsafe { libc::close(inotify_fd); }
            return Err(std::io::Error::last_os_error());
        }

        self.poll.registry().register(
            &mut SourceFd(&inotify_fd),
            INOTIFY_TOKEN,
            Interest::READABLE,
        )?;

        println!(
            "[file-watcher {}] inotify watching directory: {}",
            self.id, self.watch_directory,
        );

        let mut events = Events::with_capacity(16);
        let mut event_buf = [0u8; FILE_PAGE_SIZE];

        loop {
            self.poll.poll(&mut events, None)?;

            for event in events.iter() {
                match event.token() {
                    INOTIFY_TOKEN => {
                        loop {
                            let bytes_read = unsafe {
                                libc::read(
                                    inotify_fd,
                                    event_buf.as_mut_ptr() as *mut libc::c_void,
                                    event_buf.len(),
                                )
                            };

                            if bytes_read <= 0 {
                                let err = std::io::Error::last_os_error();
                                if err.kind() == std::io::ErrorKind::WouldBlock {
                                    break;
                                }
                                return Err(err);
                            }

                            self.process_inotify_events(
                                &event_buf[..bytes_read as usize],
                            )
                        }
                    }
                    CHANNEL_TOKEN => {
                        loop {
                            match self.rx.try_recv() {
                                Ok(msg) => self.handle_message(msg),
                                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    println!(
                                        "[file-watcher {}] channel closed, exiting",
                                        self.id,
                                    );
                                    self.cleanup_inotify(inotify_fd, wd);
                                    return Ok(());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn cleanup_inotify(&self, inotify_fd: libc::c_int, wd: libc::c_int) {
        unsafe {
            libc::inotify_rm_watch(inotify_fd, wd);
            libc::close(inotify_fd);
        }
    }

    fn process_inotify_events(&mut self, buf: &[u8]) {
        let mut offset = 0;

        while offset + std::mem::size_of::<libc::inotify_event>() <= buf.len() {
            let event = unsafe {
                &*(
                    buf[offset..].as_ptr() as *const libc::inotify_event
                )
            };

            let name_len = event.len as usize;
            let event_size = std::mem::size_of::<libc::inotify_event>() + name_len;

            if event.len > 0 {
                let name_start = offset + std::mem::size_of::<libc::inotify_event>();
                let name_bytes = &buf[name_start..name_start + name_len];
                let name_end = name_bytes.iter().position(|&byte| byte == 0).unwrap_or(name_len);
                let filename = match std::str::from_utf8(&name_bytes[..name_end]) {
                    Ok(filename) => filename,
                    Err(_) => {
                        eprintln!("[file-watcher] WARNING: non-UTF8 filename in inotify event, skipping");
                        offset += event_size;
                        continue;
                    }
                };

                if filename.contains(".ls") {
                    let mask = event.mask;
                    let event_type = if mask & libc::IN_MODIFY != 0 {
                        "MODIFY"
                    } else if mask & libc::IN_ATTRIB != 0 {
                        "ATTRIB"
                    } else if mask & libc::IN_DELETE != 0 {
                        "DELETE"
                    } else {
                        "OTHER"
                    };

                    eprintln!(
                        "[file-watcher {}] ALARM: {} detected on rotated file: {}/{}",
                        self.id, event_type, self.watch_directory, filename,
                    );

                    let full_path = format!("{}/{}", self.watch_directory, filename);
                    if filename.ends_with(".sign") {
                        self.signature_cache.invalidate(&full_path);
                    } else if filename.ends_with(".ls") && !filename.contains(".ls.") {
                        let sign_path = format!("{}.sign", full_path);
                        self.signature_cache.invalidate(&sign_path);
                    }
                }
            }

            offset += event_size;
        }
    }

    fn handle_message(&mut self, msg: FileWatcherMessage) {
        match msg {
            FileWatcherMessage::WatchFile { ls_path } => {
                println!(
                    "[file-watcher {}] registered rotated file: {}",
                    self.id, ls_path,
                );

                let sign_path = format!("{}.sign", ls_path);
                if std::path::Path::new(&sign_path).exists() {
                    let result = self.signature_cache.verify_or_cached(&sign_path);
                    match result {
                        SignatureVerifyResult::Ok { records_count } => {
                            println!(
                                "[file-watcher {}] signature verified: {} ({} records)",
                                self.id, sign_path, records_count,
                            );
                        }
                        SignatureVerifyResult::SignFileNotFound => {}
                        other => {
                            eprintln!(
                                "[file-watcher {}] ALARM: signature verification failed for {}: {:?}",
                                self.id, sign_path, other,
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl FileWatcher {
    fn run_platform(&mut self) -> std::io::Result<()> {
        println!(
            "[file-watcher {}] macOS: kqueue + Waker for channel, FSEvents TODO",
            self.id,
        );

        let mut events = Events::with_capacity(16);

        loop {
            self.poll.poll(&mut events, None)?;

            for event in events.iter() {
                if event.token() == CHANNEL_TOKEN {
                    loop {
                        match self.rx.try_recv() {
                            Ok(msg) => self.handle_message(msg),
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                println!("[file-watcher {}] channel closed, exiting", self.id);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    fn handle_message(&mut self, msg: FileWatcherMessage) {
        match msg {
            FileWatcherMessage::WatchFile { ls_path } => {
                println!(
                    "[file-watcher {}] registered rotated file: {}",
                    self.id, ls_path,
                );

                let sign_path = format!("{}.sign", ls_path);
                if std::path::Path::new(&sign_path).exists() {
                    let result = self.signature_cache.verify_or_cached(&sign_path);
                    match result {
                        SignatureVerifyResult::Ok { records_count } => {
                            println!(
                                "[file-watcher {}] signature verified: {} ({} records)",
                                self.id, sign_path, records_count,
                            );
                        }
                        SignatureVerifyResult::SignFileNotFound => {}
                        other => {
                            eprintln!(
                                "[file-watcher {}] ALARM: signature verification failed for {}: {:?}",
                                self.id, sign_path, other,
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl FileWatcher {
    fn run_platform(&mut self) -> std::io::Result<()> {
        Err(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "File watching not supported on this platform",
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_file_watcher() {
        let (_tx, rx) = std::sync::mpsc::channel();
        let result = FileWatcher::new(0, rx, "/tmp".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn sender_send_and_wake() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (_watcher, waker) = FileWatcher::new(0, rx, "/tmp".to_string()).unwrap();

        let sender = FileWatcherSender::new(tx, waker);

        let result = sender.send(FileWatcherMessage::WatchFile {
            ls_path: "/data/ls/test.ls".to_string(),
        });
        assert!(result.is_ok());
    }

    #[test]
    fn sender_clone_works() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let (_watcher, waker) = FileWatcher::new(0, _rx, "/tmp".to_string()).unwrap();

        let sender1 = FileWatcherSender::new(tx, waker);
        let sender2 = sender1.clone();

        assert!(sender1.send(FileWatcherMessage::WatchFile {
            ls_path: "/test1.ls".to_string(),
        }).is_ok());
        assert!(sender2.send(FileWatcherMessage::WatchFile {
            ls_path: "/test2.ls".to_string(),
        }).is_ok());
    }
}