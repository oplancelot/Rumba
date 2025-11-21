use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Main configuration structure for the Rumba backup tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub target: TargetConfig,
    #[serde(default)]
    pub backup: BackupConfig,
}

/// SMB source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// SMB share URL, e.g., "\\\\server\\share\\path"
    pub url: String,
    /// Username for SMB authentication
    pub username: String,
    /// Password (can be plain text or base64 encoded with "base64:" prefix)
    pub password: String,
}

/// Backup target configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    /// Output mode: "rustltfs" (pipe to rustltfs process) or "tar" (write to tar file)
    #[serde(default = "default_output_mode")]
    pub output_mode: String,
    
    /// Path to rustltfs binary (only used when output_mode = "rustltfs")
    #[serde(default = "default_rustltfs_path")]
    pub rustltfs_path: String,
    
    /// Path to the tape device or file for backup (used differently based on output_mode)
    /// - rustltfs mode: device path passed to rustltfs (e.g., "/dev/nst0")
    /// - tar mode: local tar file path (e.g., "tape_drive.tar")
    #[serde(default = "default_tape_path")]
    pub tape_path: String,
    
    /// Path to the metadata database
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

/// Backup behavior configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Number of parallel scanning threads
    #[serde(default = "default_parallel_threads")]
    pub parallel_threads: usize,
    /// Compression level for zstd (0-22)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

// Default values
fn default_output_mode() -> String {
    "rustltfs".to_string()
}

fn default_rustltfs_path() -> String {
    "rustltfs".to_string()
}

fn default_tape_path() -> String {
    "tape_drive.tar".to_string()
}

fn default_db_path() -> String {
    "backup_meta.redb".to_string()
}

fn default_parallel_threads() -> usize {
    num_cpus::get()
}

fn default_compression_level() -> i32 {
    3
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            parallel_threads: default_parallel_threads(),
            compression_level: default_compression_level(),
        }
    }
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        
        let mut config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse TOML config: {}", path.display()))?;
        
        // Decode password if it's base64 encoded
        config.source.password = decode_password(&config.source.password)?;
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }
    
    /// Validate configuration values
    fn validate(&self) -> Result<()> {
        if self.source.url.is_empty() {
            bail!("Source URL cannot be empty");
        }
        
        if self.source.username.is_empty() {
            bail!("Source username cannot be empty");
        }
        
        if self.source.password.is_empty() {
            bail!("Source password cannot be empty");
        }
        
        // Validate output mode
        if self.target.output_mode != "rustltfs" && self.target.output_mode != "tar" {
            bail!("Output mode must be either 'rustltfs' or 'tar', got: {}", self.target.output_mode);
        }
        
        if self.backup.compression_level < 0 || self.backup.compression_level > 22 {
            bail!("Compression level must be between 0 and 22");
        }
        
        if self.backup.parallel_threads == 0 {
            bail!("Parallel threads must be at least 1");
        }
        
        Ok(())
    }
    
    /// Get the backup root path from the SMB URL
    pub fn get_backup_root(&self) -> Result<PathBuf> {
        // For Windows UNC paths, we can use them directly
        // For now, we'll just convert the URL to a PathBuf
        // In a real implementation, this would mount the SMB share
        Ok(PathBuf::from(&self.source.url))
    }
}

/// Encode a password to base64 for storage in config file
pub fn encode_password(password: &str) -> String {
    use base64::{Engine as _, engine::general_purpose};
    format!("base64:{}", general_purpose::STANDARD.encode(password))
}

/// Decode a password from config file
/// If password has "base64:" prefix, decode it; otherwise return as-is
pub fn decode_password(password: &str) -> Result<String> {
    use base64::{Engine as _, engine::general_purpose};
    if let Some(encoded) = password.strip_prefix("base64:") {
        let decoded = general_purpose::STANDARD.decode(encoded)
            .context("Failed to decode base64 password")?;
        String::from_utf8(decoded)
            .context("Password is not valid UTF-8 after decoding")
    } else {
        Ok(password.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_encoding() {
        let password = "N@hegogiqp1";
        let encoded = encode_password(password);
        assert!(encoded.starts_with("base64:"));
        
        let decoded = decode_password(&encoded).unwrap();
        assert_eq!(decoded, password);
    }

    #[test]
    fn test_password_plain() {
        let password = "plain_password";
        let decoded = decode_password(password).unwrap();
        assert_eq!(decoded, password);
    }

    #[test]
    fn test_config_validation() {
        let config = Config {
            source: SourceConfig {
                url: "\\\\server\\share".to_string(),
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            target: TargetConfig {
                tape_path: "tape.tar".to_string(),
                db_path: "db.redb".to_string(),
            },
            backup: BackupConfig {
                parallel_threads: 4,
                compression_level: 3,
            },
        };
        
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_url() {
        let config = Config {
            source: SourceConfig {
                url: "".to_string(),
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            target: TargetConfig {
                tape_path: "tape.tar".to_string(),
                db_path: "db.redb".to_string(),
            },
            backup: BackupConfig::default(),
        };
        
        assert!(config.validate().is_err());
    }
}
