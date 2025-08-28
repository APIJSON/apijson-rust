//! 配置加载模块
//! 
//! 提供从YAML文件加载多数据源配置的功能

use std::fs;
use std::path::Path;
use serde_yaml;
use crate::app::datasource::config::DataSourcesConfig;

/// 配置加载错误类型
#[derive(Debug)]
pub enum ConfigLoadError {
    /// 文件读取错误
    FileReadError(std::io::Error),
    /// YAML解析错误
    YamlParseError(serde_yaml::Error),
    /// 配置验证错误
    ValidationError(String),
}

impl std::fmt::Display for ConfigLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigLoadError::FileReadError(e) => write!(f, "文件读取错误: {}", e),
            ConfigLoadError::YamlParseError(e) => write!(f, "YAML解析错误: {}", e),
            ConfigLoadError::ValidationError(e) => write!(f, "配置验证错误: {}", e),
        }
    }
}

impl std::error::Error for ConfigLoadError {}

/// 配置加载器
pub struct ConfigLoader;

impl ConfigLoader {
    /// 从YAML文件加载数据源配置
    /// 
    /// # 参数
    /// * `file_path` - YAML配置文件路径
    /// 
    /// # 返回值
    /// 成功时返回 DataSourcesConfig，失败时返回 ConfigLoadError
    /// 
    /// # 示例
    /// ```
    /// use panda_base::app::datasource::loader::ConfigLoader;
    /// 
    /// let config = ConfigLoader::load_from_file("application.yaml")?;
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(file_path: P) -> Result<DataSourcesConfig, ConfigLoadError> {
        // 读取文件内容
        let content = fs::read_to_string(file_path)
            .map_err(ConfigLoadError::FileReadError)?;
        
        Self::load_from_string(&content)
    }
    
    /// 从YAML字符串加载数据源配置
    /// 
    /// # 参数
    /// * `yaml_content` - YAML格式的配置内容
    /// 
    /// # 返回值
    /// 成功时返回 DataSourcesConfig，失败时返回 ConfigLoadError
    pub fn load_from_string(yaml_content: &str) -> Result<DataSourcesConfig, ConfigLoadError> {
        // 解析YAML内容
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(yaml_content)
            .map_err(ConfigLoadError::YamlParseError)?;
        
        // 直接反序列化整个YAML为DataSourcesConfig
        let config: DataSourcesConfig = serde_yaml::from_value(yaml_value)
            .map_err(ConfigLoadError::YamlParseError)?;
        
        // 验证配置
        Self::validate_config(&config)?;
        
        Ok(config)
    }
    
    /// 验证配置的有效性
    /// 
    /// # 参数
    /// * `config` - 要验证的配置
    /// 
    /// # 返回值
    /// 验证通过返回 Ok(())，否则返回 ConfigLoadError
    fn validate_config(config: &DataSourcesConfig) -> Result<(), ConfigLoadError> {
        if config.datasource.is_empty() {
            return Err(ConfigLoadError::ValidationError(
                "至少需要配置一个数据源".to_string()
            ));
        }
        
        // 检查是否有默认数据源
        let default_count = config.datasource.iter()
            .filter(|ds| ds.default)
            .count();
        
        if default_count == 0 {
            return Err(ConfigLoadError::ValidationError(
                "必须指定一个默认数据源".to_string()
            ));
        }
        
        if default_count > 1 {
            return Err(ConfigLoadError::ValidationError(
                "只能有一个默认数据源".to_string()
            ));
        }
        
        // 检查数据源名称是否唯一
        let mut names = std::collections::HashSet::new();
        for ds in &config.datasource {
            if !names.insert(&ds.name) {
                return Err(ConfigLoadError::ValidationError(
                    format!("数据源名称重复: {}", ds.name)
                ));
            }
        }
        
        // 检查每个数据源的数据库列表是否为空
        for ds in &config.datasource {
            if ds.database.is_empty() {
                return Err(ConfigLoadError::ValidationError(
                    format!("数据源 '{}' 的数据库列表不能为空", ds.name)
                ));
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::datasource::config::{DataSourceKind, DataSourceConfig};
    
    #[test]
    fn test_load_from_string_missing_datasources() {
        let yaml_content = r#"
other_config:
  value: test
"#;
        
        let result = ConfigLoader::load_from_string(yaml_content);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::YamlParseError(_)) = result {
            // 现在缺少datasources字段会导致YAML解析错误
        } else {
            panic!("Expected YamlParseError");
        }
    }
    
    #[test]
    fn test_validate_config_empty_datasources() {
        let config = DataSourcesConfig {
            datasource: vec![],
        };
        
        let result = ConfigLoader::validate_config(&config);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::ValidationError(msg)) = result {
            assert!(msg.contains("至少需要配置一个数据源"));
        } else {
            panic!("Expected ValidationError");
        }
    }
    
    #[test]
    fn test_validate_config_no_default() {
        let config = DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "ds1".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://localhost:3306".to_string(),
                    database: vec!["db1".to_string()],
                    default: false,
                },
            ],
        };
        
        let result = ConfigLoader::validate_config(&config);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::ValidationError(msg)) = result {
            assert!(msg.contains("必须指定一个默认数据源"));
        } else {
            panic!("Expected ValidationError");
        }
    }
    
    #[test]
    fn test_validate_config_multiple_defaults() {
        let config = DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "ds1".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://localhost:3306".to_string(),
                    database: vec!["db1".to_string()],
                    default: true,
                },
                DataSourceConfig {
                    name: "ds2".to_string(),
                    kind: DataSourceKind::Postgres,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "postgres://localhost:5432".to_string(),
                    database: vec!["db2".to_string()],
                    default: true,
                },
            ],
        };
        
        let result = ConfigLoader::validate_config(&config);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::ValidationError(msg)) = result {
            assert!(msg.contains("只能有一个默认数据源"));
        } else {
            panic!("Expected ValidationError");
        }
    }
    
    #[test]
    fn test_validate_config_duplicate_names() {
        let config = DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "ds1".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://localhost:3306".to_string(),
                    database: vec!["db1".to_string()],
                    default: true,
                },
                DataSourceConfig {
                    name: "ds1".to_string(),
                    kind: DataSourceKind::Postgres,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "postgres://localhost:5432".to_string(),
                    database: vec!["db2".to_string()],
                    default: false,
                },
            ],
        };
        
        let result = ConfigLoader::validate_config(&config);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::ValidationError(msg)) = result {
            assert!(msg.contains("数据源名称重复"));
        } else {
            panic!("Expected ValidationError");
        }
    }
    
    #[test]
    fn test_validate_config_empty_database_list() {
        let config = DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "ds1".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://localhost:3306".to_string(),
                    database: vec![], // 空的数据库列表
                    default: true,
                },
            ],
        };
        
        let result = ConfigLoader::validate_config(&config);
        assert!(result.is_err());
        
        if let Err(ConfigLoadError::ValidationError(msg)) = result {
            assert!(msg.contains("数据库列表不能为空"));
        } else {
            panic!("Expected ValidationError");
        }
    }
}