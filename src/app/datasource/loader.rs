//! 配置加载模块
//! 
//! 提供从YAML文件加载多数据源配置的功能

use std::fs;
use std::path::Path;
use serde_yaml;
use crate::app::datasource::config::DataSourcesConfig;
// 新增：用于环境变量占位符替换和缺失时告警
use regex::Regex;
use log::warn;

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
        // 先进行占位符替换（支持 ${VAR} 和 ${VAR:-default} / ${VAR-default}）
        let expanded = Self::expand_env(yaml_content);

        // 解析YAML内容
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(&expanded)
            .map_err(ConfigLoadError::YamlParseError)?;
        
        // 直接反序列化整个YAML为DataSourcesConfig
        let config: DataSourcesConfig = serde_yaml::from_value(yaml_value)
            .map_err(ConfigLoadError::YamlParseError)?;
        
        // 验证配置
        Self::validate_config(&config)?;
        
        Ok(config)
    }

    /// 将字符串中的环境变量占位符展开
    /// - 支持 ${VAR}
    /// - 支持 ${VAR:-default} 以及 ${VAR-default}
    /// - 当环境变量不存在且无默认值时，保留占位符原样并发出告警
    fn expand_env(input: &str) -> String {
        // 匹配 ${VARNAME} 或 ${VARNAME:-default}/${VARNAME-default}
        let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::?-([^}]*))?\}").expect("invalid regex");
        re.replace_all(input, |caps: &regex::Captures| {
            let var = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let default = caps.get(2).map(|m| m.as_str());
            match std::env::var(var) {
                Ok(val) => val,
                Err(_) => {
                    if let Some(d) = default {
                        d.to_string()
                    } else {
                        // 未提供默认值时，保留原始占位符，避免破坏YAML内容
                        warn!("配置中引用的环境变量未设置: ${}，请在环境或 .env 中提供", var);
                        caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
                    }
                }
            }
        }).into_owned()
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