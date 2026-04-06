use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use crate::manifest_header::{ManifestHeader, MANIFEST_HEADER_MAGIC, MANIFEST_FORMAT_VERSION};
use crate::manifest_entry::{ManifestEntry, MANIFEST_STATUS_CURRENT, MANIFEST_STATUS_ROTATED};

pub struct Manifest {
    file: File,
    header: ManifestHeader,
    path: String,
}

impl Manifest {
    pub fn create(directory: &str, shard_id: usize) -> Self {
        let path = format!("{}/{}.manifest", directory, shard_id);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect(&format!("failed to create manifest file: {}", path));

        let header = ManifestHeader::new(shard_id as u16);
        let header_bytes = unsafe { header.as_bytes() };

        file.write_all(header_bytes)
            .expect("Failed to write manifest header");
        file.sync_all()
            .expect("Failed to sync manifest file");

        Self { file, header, path }
    }

    pub fn open(directory: &str, shard_id: usize) -> Self {
        let path = format!("{}/{}.manifest", directory, shard_id);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect(&format!("failed to open manifest file: {}", path));

        let mut header = ManifestHeader::zeroed();
        file.read_exact(
            unsafe { header.as_bytes_mut() }
        ).expect("Failed to read manifest header");

        assert_eq!(header.magic, MANIFEST_HEADER_MAGIC, "Invalid manifest magic");
        assert_eq!(header.format_version, MANIFEST_FORMAT_VERSION, "Unsupported manifest version");
        assert!(unsafe { header.verify_checksum() }, "Manifest header checksum mismatch");

        Self { file, header, path }
    }

    pub fn exists(directory: &str, shard_id: usize) -> bool {
        let path = format!("{}/{}.manifest", directory, shard_id);
        std::path::Path::new(&path).exists()
    }

    pub fn append_current_entry(&mut self, entry: &mut ManifestEntry) {
        entry.status = MANIFEST_STATUS_CURRENT;
        unsafe {
            entry.compute_checksum();
        }

        let entry_offset = ManifestHeader::SIZE as u64
            + self.header.entries_count as u64 * ManifestEntry::SIZE as u64;

        let entry_bytes = unsafe { entry.as_bytes() };

        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek manifest entry position");
        self.file.write_all(entry_bytes)
            .expect("Failed to write manifest entry");

        self.header.current_entry_index = self.header.entries_count;
        self.header.entries_count += 1;
        self.header.last_updated_at_ns = Self::now_ns();
        unsafe { self.header.compute_checksum(); }

        let header_bytes = unsafe { self.header.as_bytes() };

        self.file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to header position");

        self.file.write_all(header_bytes)
            .expect("Failed to write manifest header");

        self.file.sync_all().expect("Failed to sync manifest file");
    }

    pub fn finalize_entry(
        &mut self,
        entry_index: u32,
        gsn_max: u64,
        timestamp_max_ns: u64,
    ) {
        assert!(
            (entry_index as usize) < self.header.entries_count as usize,
            "Entry index {} out of range (count={})",
            entry_index, self.header.entries_count
        );

        let entry_offset = ManifestHeader::SIZE as u64
            + entry_index as u64 * ManifestEntry::SIZE as u64;

        let mut entry = ManifestEntry::zeroed();

        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek to manifest entry for update");

        self.file.read_exact(
            unsafe { entry.as_bytes_mut() }
        ).expect("Failed to read manifest entry for update");

        entry.status = MANIFEST_STATUS_ROTATED;
        entry.gsn_max = gsn_max;
        entry.timestamp_max_ns = timestamp_max_ns;

        unsafe { entry.compute_checksum(); }

        let entry_bytes = unsafe { entry.as_bytes() };

        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek to entry for write-back");
        self.file.write_all(entry_bytes)
            .expect("Failed to write update entry");

        self.header.last_updated_at_ns = Self::now_ns();
        unsafe {
            self.header.compute_checksum();
        }

        let header_bytes = unsafe { self.header.as_bytes() };

        self.file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to header for update");

        self.file.write_all(header_bytes)
            .expect("Failed to write updated header");

        self.file.sync_all()
            .expect("Failed to fsync manifest after finalize");
    }
    
    pub fn update_entry_min_values(
        &mut self,
        entry_index: u32,
        gsn_min: u64,
        timestamp_min_ns: u64,
    ) {
        assert!(
            (entry_index as usize) < self.header.entries_count as usize,
            "Entry index {} out of range (count={})",
            entry_index, self.header.entries_count
        );
        
        let entry_offset = ManifestHeader::SIZE as u64
            + entry_index as u64 * ManifestEntry::SIZE as u64;
        
        let mut entry = ManifestEntry::zeroed();
        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek to manifest entry for min values update");
        self.file.read_exact(unsafe { entry.as_bytes_mut() })
            .expect("Failed to read manifest entry for min values update");
        
        entry.gsn_min = gsn_min;
        entry.timestamp_min_ns = timestamp_min_ns;
        unsafe { entry.compute_checksum(); }
        
        let entry_bytes = unsafe { entry.as_bytes() };
        
        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek to entry for min values write-back");
        self.file.write_all(entry_bytes)
            .expect("Failed to write updated entry min values");
        
        self.header.last_updated_at_ns = Self::now_ns();
        unsafe {
            self.header.compute_checksum();
        }
        
        let header_bytes = unsafe { self.header.as_bytes() };
        self.file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to header for min values update");
        self.file.write_all(header_bytes)
            .expect("Failed to write updated header after min values");
        
        self.file.sync_all()
            .expect("Failed to fsync manifest after min values update");
    }

    pub fn read_entry(&mut self, entry_index: u32) -> ManifestEntry {
        assert!(
            (entry_index as usize) < self.header.entries_count as usize,
            "Entry index {} out of range (count={})",
            entry_index, self.header.entries_count
        );

        let entry_offset = ManifestHeader::SIZE as u64
            + entry_index as u64 * ManifestEntry::SIZE as u64;

        let mut entry = ManifestEntry::zeroed();

        self.file.seek(SeekFrom::Start(entry_offset))
            .expect("Failed to seek to entry");
        self.file.read_exact(
            unsafe { entry.as_bytes_mut() }
        ).expect("Failed to read entry");

        assert!(
            unsafe { entry.verify_checksum() },
            "ManifestEntry checksum mismatch at index {}",
            entry_index,
        );

        entry
    }

    pub fn read_current_entry(&mut self) -> ManifestEntry {
        self.read_entry(self.header.current_entry_index)
    }

    pub fn current_entry_index(&self) -> u32 {
        self.header.current_entry_index
    }

    pub fn entries_count(&self) -> u32 {
        self.header.entries_count
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest_entry::ManifestEntry;

    fn make_temp_dir(test_name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-manifest-{}-{}",
            test_name,
            std::process::id()
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    fn make_entry(file_seq: u64, filename: &str) -> ManifestEntry {
        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = file_seq;
        entry.signing_enabled = 0;
        entry.metadata_enabled = 0;
        entry.record_size = 0;
        entry.rules_checksum = 0;
        entry.set_filename(filename);
        entry
    }

    #[test]
    fn create_and_open() {
        let dir = make_temp_dir("create-open");

        let manifest = Manifest::create(&dir, 0);
        assert_eq!(manifest.entries_count(), 0);

        drop(manifest);

        let manifest = Manifest::open(&dir, 0);
        assert_eq!(manifest.entries_count(), 0);

        cleanup(&dir);
    }

    #[test]
    fn exists_check() {
        let dir = make_temp_dir("exists");

        assert!(!Manifest::exists(&dir, 0));
        let _manifest = Manifest::create(&dir, 0);
        assert!(Manifest::exists(&dir, 0));

        cleanup(&dir);
    }

    #[test]
    fn append_and_read_entry() {
        let dir = make_temp_dir("append-read");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry = make_entry(0, "ls_20260403-120000-000-0-0.ls");
        manifest.append_current_entry(&mut entry);

        assert_eq!(manifest.entries_count(), 1);
        assert_eq!(manifest.current_entry_index(), 0);

        let read_back = manifest.read_current_entry();
        assert_eq!(read_back.file_seq, 0);
        assert_eq!(read_back.filename_str(), "ls_20260403-120000-000-0-0.ls");
        assert!(unsafe { read_back.verify_checksum() });

        cleanup(&dir);
    }

    #[test]
    fn append_multiple_entries() {
        let dir = make_temp_dir("append-multi");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry0 = make_entry(0, "ls_first-0-0.ls");
        manifest.append_current_entry(&mut entry0);

        let mut entry1 = make_entry(1, "ls_second-0-1.ls");
        manifest.append_current_entry(&mut entry1);

        assert_eq!(manifest.entries_count(), 2);
        assert_eq!(manifest.current_entry_index(), 1);

        let e0 = manifest.read_entry(0);
        assert_eq!(e0.file_seq, 0);
        assert_eq!(e0.filename_str(), "ls_first-0-0.ls");

        let e1 = manifest.read_entry(1);
        assert_eq!(e1.file_seq, 1);
        assert_eq!(e1.filename_str(), "ls_second-0-1.ls");

        cleanup(&dir);
    }

    #[test]
    fn finalize_entry_sets_rotated() {
        let dir = make_temp_dir("finalize");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry = make_entry(0, "ls_test-0-0.ls");
        manifest.append_current_entry(&mut entry);

        manifest.finalize_entry(0, 1000, 1700000000_000_000_000);

        let finalized = manifest.read_entry(0);
        assert_eq!(finalized.status, MANIFEST_STATUS_ROTATED);
        assert_eq!(finalized.gsn_max, 1000);
        assert_eq!(finalized.timestamp_max_ns, 1700000000_000_000_000);
        assert!(unsafe { finalized.verify_checksum() });

        cleanup(&dir);
    }

    #[test]
    fn rotation_flow() {
        let dir = make_temp_dir("rotation-flow");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry0 = make_entry(0, "ls_first-0-0.ls");
        entry0.gsn_min = 1;
        manifest.append_current_entry(&mut entry0);

        manifest.finalize_entry(0, 500, 1700000000_000_000_000);

        let mut entry1 = make_entry(1, "ls_second-0-1.ls");
        entry1.gsn_min = 501;
        manifest.append_current_entry(&mut entry1);

        assert_eq!(manifest.entries_count(), 2);
        assert_eq!(manifest.current_entry_index(), 1);

        let e0 = manifest.read_entry(0);
        assert_eq!(e0.status, MANIFEST_STATUS_ROTATED);
        assert_eq!(e0.gsn_max, 500);

        let e1 = manifest.read_current_entry();
        assert_eq!(e1.status, MANIFEST_STATUS_CURRENT);
        assert_eq!(e1.file_seq, 1);

        cleanup(&dir);
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = make_temp_dir("persistence");

        {
            let mut manifest = Manifest::create(&dir, 0);
            let mut entry = make_entry(0, "ls_persist-0-0.ls");
            entry.gsn_min = 42;
            manifest.append_current_entry(&mut entry);
        }

        let mut manifest = Manifest::open(&dir, 0);
        assert_eq!(manifest.entries_count(), 1);

        let entry = manifest.read_current_entry();
        assert_eq!(entry.file_seq, 0);
        assert_eq!(entry.gsn_min, 42);
        assert_eq!(entry.filename_str(), "ls_persist-0-0.ls");

        cleanup(&dir);
    }

    #[test]
    #[should_panic(expected = "Entry index")]
    fn read_entry_out_of_range() {
        let dir = make_temp_dir("out-of-range");
        let mut manifest = Manifest::create(&dir, 0);
        manifest.read_entry(0);
        cleanup(&dir);
    }

    #[test]
    fn update_entry_min_values() {
        let dir = make_temp_dir("update-min");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry = make_entry(0, "ls_test-0-0.ls");
        manifest.append_current_entry(&mut entry);

        let e = manifest.read_entry(0);
        assert_eq!(e.gsn_min, 0);
        assert_eq!(e.timestamp_min_ns, 0);

        manifest.update_entry_min_values(0, 42, 1700000000_000_000_000);

        let e = manifest.read_entry(0);
        assert_eq!(e.gsn_min, 42);
        assert_eq!(e.timestamp_min_ns, 1700000000_000_000_000);
        assert!(unsafe { e.verify_checksum() });

        cleanup(&dir);
    }

    #[test]
    fn update_entry_min_values_persists_after_reopen() {
        let dir = make_temp_dir("update-min-persist");

        {
            let mut manifest = Manifest::create(&dir, 0);
            let mut entry = make_entry(0, "ls_test-0-0.ls");
            manifest.append_current_entry(&mut entry);
            manifest.update_entry_min_values(0, 100, 1700000000_000_000_000);
        }

        let mut manifest = Manifest::open(&dir, 0);
        let e = manifest.read_current_entry();
        assert_eq!(e.gsn_min, 100);
        assert_eq!(e.timestamp_min_ns, 1700000000_000_000_000);

        cleanup(&dir);
    }

    #[test]
    fn full_lifecycle_min_then_finalize() {
        let dir = make_temp_dir("min-then-finalize");
        let mut manifest = Manifest::create(&dir, 0);

        let mut entry = make_entry(0, "ls_first-0-0.ls");
        manifest.append_current_entry(&mut entry);

        manifest.update_entry_min_values(0, 1, 1700000000_000_000_000);

        manifest.finalize_entry(0, 500, 1700000500_000_000_000);

        let e = manifest.read_entry(0);
        assert_eq!(e.status, MANIFEST_STATUS_ROTATED);
        assert_eq!(e.gsn_min, 1);
        assert_eq!(e.gsn_max, 500);
        assert_eq!(e.timestamp_min_ns, 1700000000_000_000_000);
        assert_eq!(e.timestamp_max_ns, 1700000500_000_000_000);
        assert!(unsafe { e.verify_checksum() });

        cleanup(&dir);
    }
}