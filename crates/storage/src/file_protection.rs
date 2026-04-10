use std::ffi::CString;

pub fn set_immutable(path: &str) -> std::io::Result<bool> {
    let c_path = CString::new(path).map_err(
        |error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid path {}", path)
            )
        }
    )?;

    if !std::path::Path::new(path).exists() {
        return Ok(false);
    }

    set_immutable_platform(&c_path)
}

pub fn remove_immutable(path: &str) -> std::io::Result<bool> {
    let c_path = CString::new(path).map_err(
        |_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid path {}", path)
            )
        }
    )?;

    if !std::path::Path::new(path).exists() {
        return Ok(false);
    }

    remove_immutable_platform(&c_path)
}

#[cfg(target_os = "linux")]
const FS_IMMUTABLE_FL: libc::c_int = 0x00000010;

#[cfg(target_os = "linux")]
const FS_IOC_GETFLAGS: libc::c_ulong = 0x80086601;

#[cfg(target_os = "linux")]
const FS_IOC_SETFLAGS: libc::c_ulong = 0x40086602;

#[cfg(target_os = "linux")]
fn set_immutable_platform(c_path: &CString) -> std::io::Result<bool> {
    let fd = unsafe {
        libc::open(c_path.as_ptr(), libc::O_RDONLY)
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }

    unsafe {
        let mut flags: libc::c_long = 0;

        if libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) != 0 {
            libc::close(fd);
            return Err(std::io::Error::last_os_error());
        }

        flags |= FS_IMMUTABLE_FL as libc::c_long;

        if libc::ioctl(fd, FS_IOC_SETFLAGS, &flags) != 0 {
            libc::close(fd);
            return Err(std::io::Error::last_os_error());
        }

        libc::close(fd);
    }

    Ok(true)
}

#[cfg(target_os = "linux")]
fn remove_immutable_platform(c_path: &CString) -> std::io::Result<bool> {
    let fd = unsafe {
        libc::open(c_path.as_ptr(), libc::O_RDONLY)
    };

    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }

    unsafe {
        let mut flags: libc::c_long = 0;

        if libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) != 0 {
            libc::close(fd);
            return Err(std::io::Error::last_os_error());
        }

        flags &= !(FS_IMMUTABLE_FL as libc::c_long);

        if libc::ioctl(fd, FS_IOC_SETFLAGS, &flags) != 0 {
            libc::close(fd);
            return Err(std::io::Error::last_os_error());
        }

        libc::close(fd);
    }

    Ok(true)
}

#[cfg(target_os = "macos")]
fn set_immutable_platform(c_path: &CString) -> std::io::Result<bool> {
    const UF_IMMUTABLE: libc::c_uint = 0x00000002;

    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe {
        libc::stat(c_path.as_ptr(), &mut stat)
    } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    let new_flags = stat.st_flags | UF_IMMUTABLE;
    if unsafe {
        libc::chflags(c_path.as_ptr(), new_flags)
    } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(true)
}

#[cfg(target_os = "macos")]
fn remove_immutable_platform(c_path: &CString) -> std::io::Result<bool> {
    const UF_IMMUTABLE: libc::c_uint = 0x00000002;

    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe {
        libc::stat(c_path.as_ptr(), &mut stat)
    } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    let new_flags = stat.st_flags & !UF_IMMUTABLE;
    if unsafe {
        libc::chflags(c_path.as_ptr(), new_flags)
    } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(true)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn set_immutable_platform(_c_path: &CString) -> std::io::Result<bool> {
    Err(
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Immutable file attribute not supported on this platform",
        )
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn remove_immutable_platform(_c_path: &CString) -> std::io::Result<bool> {
    Err(
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Immutable file attribute not supported on this platform",
        )
    )
}

pub fn protect_rotated_files(
    ls_path: &str,
    signing_enabled: bool,
    metadata_enabled: bool,
    immutable_enabled: bool,
) -> std::io::Result<()> {
    if !immutable_enabled {
        return Ok(());
    }

    let files = collect_rotated_file_paths(ls_path, signing_enabled, metadata_enabled);

    for path in &files {
        match set_immutable(path) {
            Ok(true) => {
                println!("[file-protection] immutable set: {}", path);
            }
            Ok(false) => {
            }
            Err(error) => {
                eprintln!(
                    "[file-protection] FATAL: failed to set immutable on {}: {}. \
                    Requires CAP_LINUX_IMMUTABLE (Linux) or file ownership (macOS). \
                    Set storage.file-protection.immutable-enabled: false to disable.",
                    path,
                    error
                );
                return Err(error);
            }
        }
    }

    Ok(())
}

fn collect_rotated_file_paths(
    ls_path: &str,
    signing_enabled: bool,
    metadata_enabled: bool,
) -> Vec<String> {
    let mut files = vec![
        ls_path.to_string(),
        format!("{}.checkpoint", ls_path),
        format!("{}.posting-accounts", ls_path),
        format!("{}.ordinal", ls_path),
        format!("{}.timestamp", ls_path),
    ];

    if signing_enabled {
        files.push(
            format!("{}.sign", ls_path)
        );
    }

    if metadata_enabled {
        files.push(
            format!("{}.posting-metadata", ls_path)
        );
    }

    files
}

pub fn unprotect_rotated_files(
    ls_path: &str,
    signing_enabled: bool,
    metadata_enabled: bool,
) {
    let files = collect_rotated_file_paths(ls_path, signing_enabled, metadata_enabled);
    for path in &files {
        let _ = remove_immutable(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-fileprot-{}-{}",
            name,
            std::process::id(),
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let _ = remove_immutable(&entry.path().to_string_lossy());
            }
        }
        std::fs::remove_dir_all(dir).ok();
    }

    fn create_test_file(path: &str) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(b"test data").unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn set_immutable_nonexistent_file() {
        let result = set_immutable("/tmp/nonexistent-immutable-test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    #[ignore]
    fn set_and_remove_immutable() {
        let dir = make_test_dir("set-remove");
        let path = format!("{}/test.ls", dir);
        create_test_file(&path);

        let result = set_immutable(&path);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        let write_result = std::fs::write(&path, b"modified");
        assert!(write_result.is_err());

        let result = remove_immutable(&path);
        assert!(result.is_ok());

        let write_result = std::fs::write(&path, b"modified");
        assert!(write_result.is_ok());

        cleanup(&dir);
    }

    #[test]
    #[ignore]
    fn protect_rotated_files_all() {
        let dir = make_test_dir("protect-all");
        let ls_path = format!("{}/test.ls", dir);

        create_test_file(&ls_path);
        create_test_file(&format!("{}.checkpoint", ls_path));
        create_test_file(&format!("{}.posting-accounts", ls_path));
        create_test_file(&format!("{}.ordinal", ls_path));
        create_test_file(&format!("{}.timestamp", ls_path));
        create_test_file(&format!("{}.sign", ls_path));
        create_test_file(&format!("{}.posting-metadata", ls_path));

        protect_rotated_files(&ls_path, true, true, true);

        assert!(std::fs::write(&ls_path, b"x").is_err());
        assert!(std::fs::write(&format!("{}.checkpoint", ls_path), b"x").is_err());
        assert!(std::fs::write(&format!("{}.sign", ls_path), b"x").is_err());

        unprotect_rotated_files(&ls_path, true, true);

        cleanup(&dir);
    }

    #[test]
    fn protect_missing_files_no_panic() {
        protect_rotated_files("/tmp/nonexistent-ls", false, false, true);
    }

    #[test]
    fn collect_paths_no_signing_no_metadata() {
        let paths = collect_rotated_file_paths("/data/ls/file.ls", false, false);
        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&"/data/ls/file.ls".to_string()));
        assert!(paths.contains(&"/data/ls/file.ls.checkpoint".to_string()));
        assert!(paths.contains(&"/data/ls/file.ls.posting-accounts".to_string()));
        assert!(paths.contains(&"/data/ls/file.ls.ordinal".to_string()));
        assert!(paths.contains(&"/data/ls/file.ls.timestamp".to_string()));
    }

    #[test]
    fn collect_paths_with_signing_and_metadata() {
        let paths = collect_rotated_file_paths("/data/ls/file.ls", true, true);
        assert_eq!(paths.len(), 7);
        assert!(paths.contains(&"/data/ls/file.ls.sign".to_string()));
        assert!(paths.contains(&"/data/ls/file.ls.posting-metadata".to_string()));
    }
}