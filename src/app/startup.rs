//! 应用启动模块
//! 
//! 负责应用的初始化流程，包括多数据源的配置加载和初始化

use std::sync::Arc;
use log::{info, error};
use crate::app::datasource::{
    loader::{ConfigLoader, ConfigLoadError},
    manager::DataSourceManager,
};

/// 应用启动错误类型
#[derive(Debug)]
pub enum StartupError {
    /// 配置加载错误
    ConfigError(ConfigLoadError),
    /// 数据源初始化错误
    DataSourceError(String),
    /// 其他错误
    Other(String),
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupError::ConfigError(e) => write!(f, "配置错误: {}", e),
            StartupError::DataSourceError(e) => write!(f, "数据源错误: {}", e),
            StartupError::Other(e) => write!(f, "启动错误: {}", e),
        }
    }
}

impl std::error::Error for StartupError {}

impl From<ConfigLoadError> for StartupError {
    fn from(err: ConfigLoadError) -> Self {
        StartupError::ConfigError(err)
    }
}

/// 应用启动器
pub struct AppStartup;

impl AppStartup {
    /// 初始化应用
    /// 
    /// # 参数
    /// * `config_path` - 配置文件路径，如果为None则使用默认路径"application.yaml"
    /// 
    /// # 返回值
    /// 成功时返回初始化完成的DataSourceManager，失败时返回StartupError
    pub async fn initialize(config_path: Option<&str>) -> Result<Arc<DataSourceManager>, StartupError> {
        info!("开始初始化应用...");
        
        // 1. 加载配置
        let config_file = config_path.unwrap_or("application.yaml");
        info!("加载配置文件: {}", config_file);
        
        let config = ConfigLoader::load_from_file(config_file)?;
        info!("配置加载成功，共配置了 {} 个数据源", config.datasource.len());
        
        // 2. 创建数据源管理器
        let manager = Arc::new(DataSourceManager::new(config));
        
        // 3. 初始化所有数据源连接
        info!("开始初始化数据源连接...");
        manager.initialize().await
            .map_err(|e| StartupError::DataSourceError(e.to_string()))?;
        
        info!("数据源初始化完成");
        
        // 4. 扫描数据库和表信息
        info!("开始扫描数据库和表信息...");
        Self::scan_database_metadata(&manager).await?;
        
        info!("应用初始化完成");
        Ok(manager)
    }
    
    /// 扫描数据库元数据
    /// 
    /// # 参数
    /// * `manager` - 数据源管理器
    /// 
    /// # 返回值
    /// 成功时返回 Ok(())，失败时返回 StartupError
    async fn scan_database_metadata(manager: &DataSourceManager) -> Result<(), StartupError> {
        let datasource_names = manager.get_datasource_names();
        
        for datasource_name in datasource_names {
            info!("扫描数据源: {}", datasource_name);
            
            let database_names = manager.get_database_names(&datasource_name);
            
            for database_name in database_names {
                info!("  扫描数据库: {}.{}", &datasource_name, &database_name);
                
                // 获取数据库连接
                if let Some(connection) = manager.get_connection(&datasource_name, &database_name) {
                    match connection {
                        crate::app::datasource::manager::DatabaseConnection::Mysql(_conn) => {
                            info!("    MySQL数据库 {}.{} 连接成功", &datasource_name, &database_name);
                        },
                        crate::app::datasource::manager::DatabaseConnection::Postgres(_conn) => {
                            info!("    PostgreSQL数据库 {}.{} 连接成功", &datasource_name, &database_name);
                        },
                    }
                } else {
                    error!("无法获取数据库连接: {}.{}", &datasource_name, &database_name);
                }
            }
        }
        
        Ok(())
    }
    
    /// 获取应用统计信息
    /// 
    /// # 参数
    /// * `manager` - 数据源管理器
    /// 
    /// # 返回值
    /// 返回包含统计信息的字符串
    pub fn get_statistics(manager: &DataSourceManager) -> String {
        let datasource_names = manager.get_datasource_names();
        let mut stats = Vec::new();
        
        stats.push(format!("数据源总数: {}", datasource_names.len()));
        
        let mut total_databases = 0;
        for datasource_name in &datasource_names {
            let db_names = manager.get_database_names(datasource_name);
             if !db_names.is_empty() {
                total_databases += db_names.len();
                stats.push(format!("  {}: {} 个数据库", datasource_name, db_names.len()));
            }
        }
        
        stats.insert(1, format!("数据库总数: {}", total_databases));
        
        // 获取缓存统计信息
        let all_datasource_names = crate::app::datasource::metadata::get_all_datasource_names();
        if !all_datasource_names.is_empty() {
            stats.push(format!("缓存中的数据源: {}", all_datasource_names.len()));
        }
        
        stats.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_initialize_with_invalid_config() {
        // 测试无效配置文件
        let result = AppStartup::initialize(Some("non_existent_file.yaml")).await;
        assert!(result.is_err());
        
        if let Err(StartupError::ConfigError(_)) = result {
            // 预期的错误类型
        } else {
            panic!("Expected ConfigError");
        }
    }
    
    #[test]
    fn test_startup_error_display() {
        let config_error = StartupError::ConfigError(
            ConfigLoadError::ValidationError("测试错误".to_string())
        );
        assert!(config_error.to_string().contains("配置错误"));
        
        let datasource_error = StartupError::DataSourceError("连接失败".to_string());
        assert!(datasource_error.to_string().contains("数据源错误"));
        
        let other_error = StartupError::Other("其他错误".to_string());
        assert!(other_error.to_string().contains("启动错误"));
    }
    
    #[test]
    fn test_get_statistics() {
        // 这个测试需要一个有效的DataSourceManager实例
        // 由于依赖外部资源，这里只测试函数不会panic
        use crate::app::datasource::config::{DataSourcesConfig, DataSourceConfig, DataSourceKind};
        
        let config = DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "test_ds".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://localhost:3306".to_string(),
                    database: vec!["test_db".to_string()],
                    default: true,
                },
            ],
        };
        
        let manager = DataSourceManager::new(config);
        let stats = AppStartup::get_statistics(&manager);
        
        assert!(stats.contains("数据源总数"));
        assert!(stats.contains("数据库总数"));
    }
}