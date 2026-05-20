use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct TimeWindowConfig {
    pub start: String,
    pub end: String,
}
