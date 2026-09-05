use std::collections::HashMap;
use std::error::Error;
use std::fs;
use serde::Deserialize;
use uuid::Uuid;
use protocol::consts::MAX_PAYLOAD_SIZE;
use protocol::request::BatchRequestHeader;
use protocol::transfer::TRANSFER_BASE_SIZE;
use crate::parse::deserialize_instance_id_hex;
use crate::time_window_config::TimeWindowConfig;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server: ServerConfig,
    pub workers: WorkersConfig,
    pub pipeline: PipelineConfig,
    pub partitions: PartitionsConfig,
    pub protocol: ProtocolConfig,
    pub batch_accept: BatchAcceptConfig,
    pub decision_maker: DecisionMakerConfig,
    pub storage: StorageConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WorkersConfig {
    pub count: usize,
    pub tcp_rb_capacity: usize,

    /// How many times a Worker hands the core to the scheduler, and comes
    /// back to a still-full Incoming Ring Buffer, before it answers the
    /// client BUSY.
    ///
    /// This is the only wait in the system that gives up. Every producer
    /// behind the ingress holds work the ledger has already taken
    /// responsibility for and waits as long as it must; the Worker is the
    /// one place where nothing has been accepted yet and a client is still
    /// on the line to be told so.
    ///
    /// The bound counts scheduler round-trips rather than elapsed time so
    /// that the same batch meets the same answer on every run. Its
    /// wall-clock length is whatever the machine makes it, and is a thing
    /// to measure on the target rather than to promise here.
    pub pipeline_wait_max_yields: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PipelineConfig {
    pub count: usize,
    pub incoming_rb_capacity: usize,
    pub incoming_rb_batch_size: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionsConfig {
    pub count: usize,
    pub initial_accounts_count: usize,
    pub partition_rb_capacity: usize,
    pub partition_rb_batch_size: usize,
    #[serde(default)]
    pub accounts_assignment_overrides_path: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ProtocolConfig {
    pub metadata_size: usize,

    /// How many payload bytes the server will accumulate for one
    /// message before refusing it.
    ///
    /// Judged on the thirteen-byte message header, before any payload
    /// byte is buffered, and it governs every message type rather than
    /// batches alone. Deliberately NOT derived from
    /// `batch-accept.max-transfers-per-batch`: that bound is about how
    /// much work a complete message asks for, this one about how many
    /// bytes an incomplete one may pin. Startup refuses a
    /// configuration in which the two could collide.
    pub max_message_payload_bytes: usize,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct BatchAcceptConfig {
    pub all_or_nothing: bool,
    pub partial_reject_by_transfer_sequence_id: bool,

    /// The largest number of transfers one batch may declare.
    ///
    /// A client-supplied count is otherwise a `u16`, which admits
    /// 65 535 transfers against an ingress ring of a few thousand
    /// slots — a batch the ring can never hold, sent by anyone who can
    /// open a connection. The bound is what turns that from a way to
    /// stop the process into an answer the client receives.
    ///
    /// Startup refuses a value of zero, and refuses a value whose
    /// product with the worker count exceeds the ingress ring's
    /// capacity: every Worker is a producer, so that product is the
    /// burst the ring must absorb, and a configuration that cannot
    /// absorb it makes refusal the steady state rather than the
    /// exception.
    pub max_transfers_per_batch: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DecisionMakerConfig {
    pub count: usize,
    pub transfer_hash_table_capacity: usize,
    pub coordinator_rb_capacity: usize,
    pub coordinator_rb_batch_size: usize,
    pub flush_done_rb_capacity: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    pub flush_timeout_ms: u64,
    pub flush_max_buffer_posting_records: usize,
    pub current_files_directory: String,
    pub previous_files_directory: String,
    pub max_ls_file_size_mb: usize,
    pub signing_enabled: bool,
    pub posting_metadata: PostingMetadata,
    #[serde(default = "default_checkpoint_prealloc_multiplier")]
    pub checkpoint_prealloc_multiplier: usize,
    pub in_flight_min_heap: StorageInFlightMinHeap,
    pub file_protection: StorageFileProtection,
}

fn default_checkpoint_prealloc_multiplier() -> usize { 4 }

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PostingMetadata {
    pub enabled: bool,
    pub record_size: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageInFlightMinHeap {
    pub initial_capacity:  usize,
    pub max_resize_count: u16,
    pub growth_factor: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageFileProtection {
    pub immutable_enabled: bool,
    pub watch_enabled: bool,

    #[serde(default = "default_recheck_interval_seconds")]
    pub recheck_interval_seconds: u64,
    #[serde(default = "default_recheck_batch_size")]
    pub recheck_batch_size: usize,
    #[serde(default = "default_recheck_depth_files")]
    pub recheck_depth_files: usize,
    #[serde(default = "default_recheck_depth_days")]
    pub recheck_depth_days: usize,
    #[serde(default)]
    pub recheck_allowed_time_windows: Vec<(u32, u32)>,

    #[serde(default = "default_startup_verify_recent_count")]
    pub startup_verify_recent_count: usize,

    #[serde(default = "default_tampering_log_max_size_mb")]
    pub tampering_log_max_size_mb: usize,

    /// Unique identifier for this server instance.
    ///
    /// Checked against every TamperingLog segment header on startup.
    /// A mismatch means the segment belongs to a different instance;
    /// the segment is skipped to prevent cross-instance log replay (H2).
    ///
    /// Value: 32 hex characters = 16 bytes (UUID v4/v7 footprint).
    /// Absent or all-zero → check disabled (backward compat with
    /// deployments that have not set an instance identity).
    ///
    /// See ADR-017 §Amendment 2026-05-09 for rationale.
    #[serde(default, deserialize_with = "deserialize_instance_id_hex")]
    pub instance_id: Option<[u8; 16]>
}

fn default_recheck_depth_files() -> usize { 1000 }
fn default_recheck_interval_seconds() -> u64 { 3600 }
fn default_recheck_batch_size() -> usize { 10 }
fn default_recheck_depth_days() -> usize { 30 }
fn default_startup_verify_recent_count() -> usize { 0 }
fn default_tampering_log_max_size_mb() -> usize { 64 }

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.workers.count == 0 {
            return Err("workers.count must be > 0".into());
        }
        if !self.workers.tcp_rb_capacity.is_power_of_two() {
            return Err("workers.tcp-rb-capacity must be a power of two".into());
        }
        if self.workers.tcp_rb_capacity == 0 {
            return Err("workers.tcp-rb-capacity must be > 0".into());
        }
        if self.workers.pipeline_wait_max_yields == 0 {
            return Err("workers.pipeline-wait-max-yields must be > 0".into());
        }
        if self.decision_maker.count == 0 {
            return Err("decision-maker.count must be > 0".into());
        }
        if !self.decision_maker.transfer_hash_table_capacity.is_power_of_two() {
            return Err("decision-maker.transfer-hash-table-capacity must be a power of two".into());
        }
        if self.decision_maker.transfer_hash_table_capacity < 16 {
            return Err("decision-maker.transfer-hash-table-capacity must be at least 16".into());
        }
        if !self.decision_maker.coordinator_rb_capacity.is_power_of_two() {
            return Err("decision-maker.coordinator-rb-capacity must be a power of two".into());
        }
        if self.decision_maker.coordinator_rb_batch_size < 2 {
            return Err("decision-maker.coordinator-rb-batch-size must be >= 2".into());
        }
        if self.decision_maker.coordinator_rb_batch_size > self.decision_maker.coordinator_rb_capacity {
            return Err("decision-maker.coordinator-rb-batch-size must be <= coordinator-rb-capacity".into());
        }
        if !self.decision_maker.flush_done_rb_capacity.is_power_of_two() {
            return Err("decision-maker.flush-done-rb-capacity must be a power of two".into());
        }
        if self.pipeline.count == 0 {
            return Err("pipeline.count must be > 0".into());
        }
        if self.partitions.count == 0 {
            return Err("partitions.count must be > 0".into());
        }
        if !self.partitions.initial_accounts_count.is_power_of_two() {
            return Err("partitions.initial-accounts-count must be a power of two".into());
        }
        if self.partitions.initial_accounts_count < 16 {
            return Err("partitions.initial-accounts-count must be at least 16".into());
        }
        if !self.partitions.partition_rb_capacity.is_power_of_two() {
            return Err("partitions.partitions.partition-rb-capacity must be a power of two".into());
        }
        if self.partitions.partition_rb_capacity == 0 {
            return Err("partitions.partitions.partition-rb-capacity must be > 0".into());
        }
        if self.partitions.partition_rb_batch_size == 0 {
            return Err("partitions.partition-rb-batch-size must be > 0".into());
        }
        if self.partitions.partition_rb_batch_size > self.partitions.partition_rb_capacity {
            return Err("partitions.partition-rb-batch-size must be <= partition-rb-capacity".into());
        }
        if !self.pipeline.incoming_rb_capacity.is_power_of_two() {
            return Err("pipeline.incoming-rb-capacity must be a power of two".into());
        }
        if self.pipeline.incoming_rb_batch_size == 0 {
            return Err("pipeline.incoming-rb-batch-size must be > 0".into());
        }
        if self.pipeline.incoming_rb_batch_size > self.pipeline.incoming_rb_capacity {
            return Err("pipeline.incoming-rb-batch-size must be <= incoming-rb-capacity".into());
        }

        let incoming_rb_capacity = self.pipeline.incoming_rb_capacity;
        let worker_count = self.workers.count;
        let max_transfers_per_batch = self.batch_accept.max_transfers_per_batch;
        if max_transfers_per_batch == 0 {
            return Err(format!(
                "batch-accept.max-transfers-per-batch must be > 0 \
                 (workers.count is {worker_count}, \
                 pipeline.incoming-rb-capacity is {incoming_rb_capacity})"
            ).into());
        }
        let simultaneous_burst =
            max_transfers_per_batch.saturating_mul(worker_count);
        if simultaneous_burst > incoming_rb_capacity {
            return Err(format!(
                "batch-accept.max-transfers-per-batch is {max_transfers_per_batch} \
                 and {worker_count} workers can each claim that much at once, \
                 which is {simultaneous_burst} against \
                 pipeline.incoming-rb-capacity {incoming_rb_capacity}; \
                 refusal would be the steady state rather than the exception"
            ).into());
        }

        let max_message_payload_bytes = self.protocol.max_message_payload_bytes;
        let largest_legitimate_batch_payload = BatchRequestHeader::SIZE.saturating_add(
            max_transfers_per_batch
                .saturating_mul(TRANSFER_BASE_SIZE + self.protocol.metadata_size),
        );
        if max_message_payload_bytes <= largest_legitimate_batch_payload {
            return Err(format!(
                "protocol.max-message-payload-bytes is {max_message_payload_bytes}, \
                 which is not above the {largest_legitimate_batch_payload} bytes the \
                 largest legitimate batch occupies at \
                 batch-accept.max-transfers-per-batch {max_transfers_per_batch}; \
                 an over-count batch would be killed on the message header instead \
                 of receiving the refusal the ingress is built to send"
            ).into());
        }
        if max_message_payload_bytes > MAX_PAYLOAD_SIZE as usize {
            return Err(format!(
                "protocol.max-message-payload-bytes is {max_message_payload_bytes}, \
                 above the protocol's absolute frame ceiling {MAX_PAYLOAD_SIZE}"
            ).into());
        }
        if self.storage.flush_timeout_ms == 0 {
            return Err("storage.flush-timeout-ms must be > 0".into());
        }
        if self.storage.flush_max_buffer_posting_records == 0 {
            return Err("storage.flush-max-buffer-posting-records must be > 0".into());
        }
        if self.storage.current_files_directory.is_empty() {
            return Err("storage.current-files-directory must not be empty".into());
        }
        if self.storage.previous_files_directory.is_empty() {
            return Err("storage.previous-files-directory must not be empty".into());
        }
        if self.storage.max_ls_file_size_mb == 0 {
            return Err("storage.max-ls-file-size-mb must be > 0".into());
        }
        if self.storage.posting_metadata.enabled && self.storage.posting_metadata.record_size == 0 {
            return Err("posting-metadata.record_size must be > 0 if it is enabled".into());
        }
        if self.storage.posting_metadata.enabled && !self.storage.posting_metadata.record_size.is_power_of_two() {
            return Err("posting-metadata.record_size must be a power of two".into());
        }
        if self.storage.checkpoint_prealloc_multiplier < 1 {
            return Err("storage.checkpoint_prealloc_multiplier must be equals or greater than 1".into());
        }
        if self.storage.in_flight_min_heap.initial_capacity == 0 {
            return Err("storage.in-flight-min-heap.initial-capacity must be > 0".into());
        }
        if !self.storage.in_flight_min_heap.initial_capacity.is_power_of_two() {
            return Err("storage.in-flight-min-heap.initial-capacity must be a power of two".into());
        }
        if self.storage.file_protection.recheck_interval_seconds == 0 {
            return Err("storage.file-protection.recheck-interval-seconds must be > 0".into());
        }
        if self.storage.file_protection.recheck_batch_size == 0 {
            return Err("storage.file-protection.recheck-batch-size must be > 0".into());
        }
        if self.storage.file_protection.tampering_log_max_size_mb == 0 {
            return Err("storage.file-protection.tampering-log.max-size-mb must be > 0".into());
        }
        for (start, end) in &self.storage.file_protection.recheck_allowed_time_windows {
            if start > end {
                return Err("storage.file-protection.recheck-allowed_time_windows must be smaller than end".into());
            }
        }
        if let Some(ref path) = self.partitions.accounts_assignment_overrides_path {
            if path.is_empty() {
                return Err("partitions.accounts-assignment-overrides-path must not be empty if specified".into());
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionAccountAssignmentConfig {
    partitions_accounts_assignment_overrides: HashMap<Uuid, usize>,
}

impl PartitionAccountAssignmentConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn Error>> {
        let content = fs::read_to_string(path)?;
        let config: PartitionAccountAssignmentConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn empty() -> Self {
        Self {
            partitions_accounts_assignment_overrides: HashMap::new(),
        }
    }

    pub fn convert_to_u8_key_map(&self) -> HashMap<[u8; 16], usize> {
        let mut converted: HashMap<[u8; 16], usize> = HashMap::with_capacity(
            self.partitions_accounts_assignment_overrides.len()
        );
        for (key, value) in &self.partitions_accounts_assignment_overrides {
            converted.insert(*key.as_bytes(), *value);
        }
        converted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_config(name: &str, content: &str) -> String {
        let path = format!("/tmp/solidus-test-{}.yaml", name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    const VALID_YAML: &str = "\
server:
  bind-address: \"127.0.0.1\"
  port: 9100
workers:
  count: 4
  tcp-rb-capacity: 1024
  pipeline-wait-max-yields: 64
pipeline:
  count: 1
  incoming-rb-capacity: 1024
  incoming-rb-batch-size: 64
partitions:
  count: 16
  initial-accounts-count: 65536
  partition-rb-capacity: 4096
  partition-rb-batch-size: 64
protocol:
  metadata-size: 0
  max-message-payload-bytes: 1048576
batch-accept:
  all-or-nothing: true
  partial-reject-by-transfer-sequence-id: false
  max-transfers-per-batch: 256
decision-maker:
  count: 1
  transfer-hash-table-capacity: 16384
  coordinator-rb-capacity: 65536
  coordinator-rb-batch-size: 128
  flush-done-rb-capacity: 4096
storage:
  flush-timeout-ms: 2
  flush-max-buffer-posting-records: 512
  current-files-directory: \"data/ls\"
  previous-files-directory: \"data/ls\"
  max-ls-file-size-mb: 256
  signing-enabled: true
  posting-metadata:
    enabled: true
    record-size: 256
  checkpoint-prealloc-multiplier: 4
  in-flight-min-heap:
    initial-capacity: 1024
    max-resize-count: 4
    growth-factor: 2
  file-protection:
    immutable-enabled: true
    watch-enabled: true
 ";

    #[test]
    fn load_valid_config() {
        let path = write_temp_config("valid", VALID_YAML);
        let config = Config::load(&path).expect("should load");
        assert_eq!(config.workers.count, 4);
        assert_eq!(config.workers.pipeline_wait_max_yields, 64);
        assert_eq!(config.pipeline.count, 1);
        assert_eq!(config.pipeline.incoming_rb_capacity, 1024);
        assert_eq!(config.pipeline.incoming_rb_batch_size, 64);
        assert_eq!(config.partitions.count, 16);
        assert_eq!(config.partitions.initial_accounts_count, 65536);
        assert_eq!(config.partitions.partition_rb_capacity, 4096);
        assert_eq!(config.partitions.partition_rb_batch_size, 64);
        assert_eq!(config.protocol.metadata_size, 0);
        assert_eq!(config.server.bind_address, "127.0.0.1");
        assert_eq!(config.server.port, 9100);
        assert_eq!(config.decision_maker.count, 1);
        assert_eq!(config.decision_maker.transfer_hash_table_capacity, 16384);
        assert_eq!(config.decision_maker.coordinator_rb_capacity, 65536);
        assert_eq!(config.decision_maker.coordinator_rb_batch_size, 128);
        assert_eq!(config.decision_maker.flush_done_rb_capacity, 4096);
        assert_eq!(config.storage.flush_timeout_ms, 2);
        assert_eq!(config.storage.flush_max_buffer_posting_records, 512);
        assert_eq!(config.storage.current_files_directory, "data/ls");
        assert_eq!(config.storage.previous_files_directory, "data/ls");
        assert_eq!(config.storage.max_ls_file_size_mb, 256);
        assert!(config.batch_accept.all_or_nothing);
        assert!(!config.batch_accept.partial_reject_by_transfer_sequence_id);
        assert_eq!(config.batch_accept.max_transfers_per_batch, 256);
        assert_eq!(config.protocol.max_message_payload_bytes, 1_048_576);
        assert!(config.storage.posting_metadata.enabled);
        assert_eq!(config.storage.posting_metadata.record_size, 256);
        assert_eq!(config.storage.checkpoint_prealloc_multiplier, 4);
        assert_eq!(config.storage.in_flight_min_heap.initial_capacity, 1024);
        assert_eq!(config.storage.in_flight_min_heap.max_resize_count, 4);
        assert_eq!(config.storage.in_flight_min_heap.growth_factor, 2);
        assert!(config.storage.file_protection.immutable_enabled);
        assert!(config.storage.file_protection.watch_enabled);
    }

    #[test]
    fn load_config_with_overrides_path() {
        let yaml = VALID_YAML.replace(
            "partition-rb-batch-size: 64",
            "partition-rb-batch-size: 64\n  accounts-assignment-overrides-path: \"overrides.yaml\"",
        );
        let path = write_temp_config("with-overrides", &yaml);
        let config = Config::load(&path).expect("should load");
        assert_eq!(
        config.partitions.accounts_assignment_overrides_path,
        Some("overrides.yaml".to_string()),
    );
    }

    #[test]
    fn config_without_overrides_path_is_none() {
        let path = write_temp_config("no-overrides", VALID_YAML);
        let config = Config::load(&path).expect("should load");
        assert!(config.partitions.accounts_assignment_overrides_path.is_none());
    }

    #[test]
    fn load_missing_file() {
        let result = Config::load("/tmp/nonexistent-solidus-config.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn validate_zero_workers() {
        let yaml = VALID_YAML.replace("count: 4", "count: 0");
        let path = write_temp_config("zero-workers", &yaml);
        let result = Config::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn validate_zero_pipeline_wait_max_yields() {
        let yaml = VALID_YAML.replace(
            "pipeline-wait-max-yields: 64",
            "pipeline-wait-max-yields: 0",
        );
        let config: Config = serde_yaml::from_str(&yaml).expect("yaml must parse");
        assert!(
            config.validate().is_err(),
            "an allowance of zero would refuse every batch on the first full ring",
        );
    }

    #[test]
    fn validate_zero_max_transfers_per_batch() {
        let yaml = VALID_YAML.replace(
            "max-transfers-per-batch: 256",
            "max-transfers-per-batch: 0",
        );
        let config: Config = serde_yaml::from_str(&yaml).expect("yaml must parse");
        let error = config.validate().expect_err("a bound of zero must refuse to start");
        let message = error.to_string();
        assert!(
            message.contains("must be > 0"),
            "the message must state the rule: {message}",
        );
        assert!(
            message.contains("workers.count is 4"),
            "the message must name the worker count: {message}",
        );
        assert!(
            message.contains("1024"),
            "the message must name the ring capacity it is judged against: {message}",
        );
    }

    #[test]
    fn validate_max_transfers_per_batch_above_the_worker_aggregate_share() {
        let yaml = VALID_YAML.replace(
            "max-transfers-per-batch: 256",
            "max-transfers-per-batch: 512",
        );
        let config: Config = serde_yaml::from_str(&yaml).expect("yaml must parse");
        let error = config
            .validate()
            .expect_err("four workers claiming 512 each cannot fit a 1024-slot ring");
        let message = error.to_string();
        assert!(
            message.contains("512"),
            "the message must name the configured bound: {message}",
        );
        assert!(
            message.contains("2048"),
            "the message must name the simultaneous burst: {message}",
        );
        assert!(
            message.contains("1024"),
            "the message must name the ring capacity: {message}",
        );
    }

    #[test]
    fn validate_max_transfers_per_batch_at_the_worker_aggregate_share_is_allowed() {
        let config: Config = serde_yaml::from_str(VALID_YAML).expect("yaml must parse");
        assert_eq!(config.batch_accept.max_transfers_per_batch, 256);
        assert_eq!(config.workers.count, 4);
        assert_eq!(config.pipeline.incoming_rb_capacity, 1024);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_message_payload_budget_below_the_largest_legitimate_batch() {
        let yaml = VALID_YAML.replace(
            "max-message-payload-bytes: 1048576",
            "max-message-payload-bytes: 20000",
        );
        let config: Config = serde_yaml::from_str(&yaml).expect("yaml must parse");
        let error = config
            .validate()
            .expect_err("a budget under the largest legitimate batch must refuse to start");
        let message = error.to_string();
        assert!(message.contains("20000"), "the message must name the budget: {message}");
        assert!(
            message.contains("28690"),
            "the message must name the largest legitimate batch payload: {message}",
        );
    }

    #[test]
    fn validate_message_payload_budget_above_the_absolute_ceiling() {
        let yaml = VALID_YAML.replace(
            "max-message-payload-bytes: 1048576",
            "max-message-payload-bytes: 33554432",
        );
        let config: Config = serde_yaml::from_str(&yaml).expect("yaml must parse");
        let error = config
            .validate()
            .expect_err("a budget above the protocol ceiling must refuse to start");
        assert!(error.to_string().contains("33554432"));
    }

    #[test]
    fn validate_message_payload_budget_leaves_room_for_the_batch_bound() {
        let config: Config = serde_yaml::from_str(VALID_YAML).expect("yaml must parse");
        let largest_legitimate_batch_payload = 18 + 256 * 112;
        assert_eq!(largest_legitimate_batch_payload, 28_690);
        assert!(config.protocol.max_message_payload_bytes > largest_legitimate_batch_payload);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_capacity_not_power_of_two() {
        let yaml = VALID_YAML.replace("incoming-rb-capacity: 1024", "incoming_rb_capacity: 1000");
        let path = write_temp_config("bad-capacity", &yaml);
        let result = Config::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn validate_batch_exceeds_capacity() {
        let yaml = VALID_YAML.replace("incoming-rb-batch-size: 64", "incoming_rb_batch: 2048");
        let path = write_temp_config("batch-exceeds", &yaml);
        let result = Config::load(&path);
        assert!(result.is_err());
    }
}