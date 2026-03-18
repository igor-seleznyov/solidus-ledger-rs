use std::collections::HashMap;
use std::error::Error;
use std::fs;
use serde::Deserialize;
use uuid::Uuid;

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
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct BatchAcceptConfig {
    pub all_or_nothing: bool,
    pub partial_reject_by_transfer_sequence_id: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DecisionMakerConfig {
    pub count: usize,
    pub transfer_hash_table_capacity: usize,
    pub coordinator_rb_capacity: usize,
    pub coordinator_rb_batch_size: usize,
}

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
        if self.decision_maker.coordinator_rb_batch_size == 0 {
            return Err("decision-maker.coordinator-rb-batch-size must be > 0".into());
        }
        if self.decision_maker.coordinator_rb_batch_size > self.decision_maker.coordinator_rb_capacity {
            return Err("decision-maker.coordinator-rb-batch-size must be <= coordinator-rb-capacity".into());
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
batch-accept:
  all-or-nothing: true
  partial-reject-by-transfer-sequence-id: false
decision-maker:
  count: 1
  transfer-hash-table-capacity: 16384
  coordinator-rb-capacity: 65536
  coordinator-rb-batch-size: 128
 ";

    #[test]
    fn load_valid_config() {
        let path = write_temp_config("valid", VALID_YAML);
        let config = Config::load(&path).expect("should load");
        assert_eq!(config.workers.count, 4);
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
        assert_eq!(config.decision_maker.coordinator_rb_batch_size, 128);        assert!(config.batch_accept.all_or_nothing);
        assert!(!config.batch_accept.partial_reject_by_transfer_sequence_id);
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