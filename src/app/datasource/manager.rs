use std::collections::HashMap;
use std::sync::Arc;
use crate::app::datasource::config::{DataSourceKind, DataSourcesConfig};
use crate::app::datasource::mysql::DBConn;
use crate::app::datasource::postgres::PgConn;

/// 数据库连接枚举
#[derive(Debug, Clone)]
pub enum DatabaseConnection {
    /// MySQL 连接
    Mysql(DBConn),
    /// PostgreSQL 连接
    Postgres(PgConn),
}

impl DatabaseConnection {
    /// 查询单条记录
    pub async fn query_one(&self, sql: &str, params: Vec<String>) -> Result<Option<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                conn.query_one(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            DatabaseConnection::Postgres(conn) => {
                conn.query_one(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }

    /// 查询多条记录
    pub async fn query_list(&self, sql: &str, params: Vec<String>) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                conn.query_list(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            DatabaseConnection::Postgres(conn) => {
                conn.query_list(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }

    /// 执行更新操作
    pub async fn update(&self, sql: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                conn.update(sql).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            DatabaseConnection::Postgres(conn) => {
                conn.update(sql).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }

    /// 加载数据库表信息到缓存
    pub async fn load_db_table(&mut self, schema: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                conn.load_db_table(schema).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            DatabaseConnection::Postgres(conn) => {
                conn.load_db_table(schema).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }

    /// 查询记录数
    pub async fn count(&self, sql: &str, params: Vec<String>) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                conn.count(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            DatabaseConnection::Postgres(conn) => {
                conn.count(sql, params).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }
}

/// 数据源管理器
/// 
/// 负责管理多个数据源的连接，提供统一的数据库操作接口
#[derive(Debug, Clone)]
pub struct DataSourceManager {
    /// 数据源配置
    config: DataSourcesConfig,
    /// 数据源名称到连接的映射
    /// Key: 数据源名称
    /// Value: 数据库名称到连接的映射
    connections: Arc<std::sync::RwLock<HashMap<String, HashMap<String, DatabaseConnection>>>>,
    /// 默认数据源名称
    default_datasource: Option<String>,
}

impl DataSourceManager {
    /// 创建新的数据源管理器
    /// 
    /// # 参数
    /// * `config` - 数据源配置
    /// 
    /// # 返回值
    /// 返回数据源管理器实例
    pub fn new(config: DataSourcesConfig) -> Self {
        let default_datasource = config.get_default_datasource().map(|ds| ds.name.clone());
        
        Self {
            config,
            connections: Arc::new(std::sync::RwLock::new(HashMap::new())),
            default_datasource,
        }
    }

    /// 初始化所有数据源连接
    /// 
    /// 遍历配置中的所有数据源，为每个数据源的每个数据库建立连接
    /// 
    /// # 返回值
    /// * `Ok(())` - 初始化成功
    /// * `Err(Box<dyn std::error::Error + Send + Sync>)` - 初始化失败
    pub async fn initialize(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut connections = self.connections.write().unwrap();
        
        for datasource in &self.config.datasource {
            let mut ds_connections = HashMap::new();
            
            for database in &datasource.database {
                let connection_url = datasource.build_connection_url(database);
                
                let db_connection = match datasource.kind {
                    DataSourceKind::Mysql => {
                        let mut conn = DBConn::new(&connection_url, &datasource.name).await
                            .map_err(|e| format!("Failed to connect to MySQL database {}: {}", database, e))?;
                        // 加载表信息到缓存
                        if let Err(e) = conn.load_db_table(database).await {
                            log::warn!("Failed to load table metadata for MySQL database {}: {}", database, e);
                        }
                        DatabaseConnection::Mysql(conn)
                    }
                    DataSourceKind::Postgres => {
                        let mut conn = PgConn::new(&connection_url, &datasource.name).await
                            .map_err(|e| format!("Failed to connect to PostgreSQL database {}: {}", database, e))?;
                        // 加载表信息到缓存
                        if let Err(e) = conn.load_db_table(database).await {
                            log::warn!("Failed to load table metadata for PostgreSQL database {}: {}", database, e);
                        }
                        DatabaseConnection::Postgres(conn)
                    }
                };
                
                ds_connections.insert(database.clone(), db_connection);
                log::info!("Connected to database: {} in datasource: {}", database, datasource.name);
            }
            
            connections.insert(datasource.name.clone(), ds_connections);
            log::info!("Datasource {} initialized with {} databases", datasource.name, datasource.database.len());
        }
        
        Ok(())
    }

    /// 获取指定数据源和数据库的连接
    /// 
    /// # 参数
    /// * `datasource_name` - 数据源名称
    /// * `database_name` - 数据库名称
    /// 
    /// # 返回值
    /// 返回数据库连接的克隆
    pub fn get_connection(&self, datasource_name: &str, database_name: &str) -> Option<DatabaseConnection> {
        let connections = self.connections.read().unwrap();
        connections.get(datasource_name)
            .and_then(|ds_conns| ds_conns.get(database_name))
            .cloned()
    }

    /// 获取默认数据源的指定数据库连接
    /// 
    /// # 参数
    /// * `database_name` - 数据库名称
    /// 
    /// # 返回值
    /// 返回默认数据源中指定数据库的连接
    pub fn get_default_connection(&self, database_name: &str) -> Option<DatabaseConnection> {
        if let Some(default_ds) = &self.default_datasource {
            self.get_connection(default_ds, database_name)
        } else {
            None
        }
    }

    /// 获取所有数据源名称
    /// 
    /// # 返回值
    /// 返回所有数据源名称的向量
    pub fn get_datasource_names(&self) -> Vec<String> {
        self.config.get_datasource_names()
    }

    /// 获取指定数据源的所有数据库名称
    /// 
    /// # 参数
    /// * `datasource_name` - 数据源名称
    /// 
    /// # 返回值
    /// 返回指定数据源的所有数据库名称
    pub fn get_database_names(&self, datasource_name: &str) -> Vec<String> {
        if let Some(datasource) = self.config.get_datasource_by_name(datasource_name) {
            datasource.database.clone()
        } else {
            Vec::new()
        }
    }

    /// 获取所有数据源和数据库的映射
    /// 
    /// # 返回值
    /// 返回数据源名称到数据库名称列表的映射
    pub fn get_all_datasource_databases(&self) -> HashMap<String, Vec<String>> {
        self.config.datasource.iter()
            .map(|ds| (ds.name.clone(), ds.database.clone()))
            .collect()
    }

    /// 根据数据源名称和数据库名称查询单条记录
    /// 
    /// # 参数
    /// * `datasource_name` - 数据源名称
    /// * `database_name` - 数据库名称
    /// * `sql` - SQL 查询语句
    /// * `params` - 查询参数
    /// 
    /// # 返回值
    /// 返回查询结果
    pub async fn query_one(
        &self,
        datasource_name: &str,
        database_name: &str,
        sql: &str,
        params: Vec<String>,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(connection) = self.get_connection(datasource_name, database_name) {
            connection.query_one(sql, params).await
        } else {
            Err(format!("Connection not found for datasource: {}, database: {}", datasource_name, database_name).into())
        }
    }

    /// 根据数据源名称和数据库名称查询多条记录
    /// 
    /// # 参数
    /// * `datasource_name` - 数据源名称
    /// * `database_name` - 数据库名称
    /// * `sql` - SQL 查询语句
    /// * `params` - 查询参数
    /// 
    /// # 返回值
    /// 返回查询结果列表
    pub async fn query_list(
        &self,
        datasource_name: &str,
        database_name: &str,
        sql: &str,
        params: Vec<String>,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(connection) = self.get_connection(datasource_name, database_name) {
            connection.query_list(sql, params).await
        } else {
            Err(format!("Connection not found for datasource: {}, database: {}", datasource_name, database_name).into())
        }
    }

    /// 获取数据源配置
    /// 
    /// # 参数
    /// * `datasource_name` - 数据源名称
    /// 
    /// # 返回值
    /// 返回数据源配置的引用
    pub fn get_datasource_config(&self, datasource_name: &str) -> Option<&crate::app::datasource::config::DataSourceConfig> {
        self.config.get_datasource_by_name(datasource_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::datasource::config::{DataSourceConfig, DataSourceKind};

    fn create_test_config() -> DataSourcesConfig {
        DataSourcesConfig {
            datasource: vec![
                DataSourceConfig {
                    name: "ds_mysql".to_string(),
                    kind: DataSourceKind::Mysql,
                    username: "root".to_string(),
                    password: "123456".to_string(),
                    url: "mysql://user:password@localhost:3306".to_string(),
                    database: vec!["db1".to_string(), "db2".to_string()],
                    default: true,
                },
                DataSourceConfig {
                    name: "ds_pg".to_string(),
                    kind: DataSourceKind::Postgres,
                    username: "postgres".to_string(),
                    password: "123456".to_string(),
                    url: "postgres://user:password@localhost:5432".to_string(),
                    database: vec!["db3".to_string(), "db4".to_string()],
                    default: false,
                },
            ],
        }
    }

    #[test]
    fn test_datasource_manager_creation() {
        let config = create_test_config();
        let manager = DataSourceManager::new(config);
        
        assert_eq!(manager.default_datasource, Some("ds_mysql".to_string()));
        assert_eq!(manager.get_datasource_names(), vec!["ds_mysql", "ds_pg"]);
    }

    #[test]
    fn test_get_database_names() {
        let config = create_test_config();
        let manager = DataSourceManager::new(config);
        
        let mysql_dbs = manager.get_database_names("ds_mysql");
        assert_eq!(mysql_dbs, vec!["db1", "db2"]);
        
        let pg_dbs = manager.get_database_names("ds_pg");
        assert_eq!(pg_dbs, vec!["db3", "db4"]);
        
        let unknown_dbs = manager.get_database_names("unknown");
        assert!(unknown_dbs.is_empty());
    }

    #[test]
    fn test_get_all_datasource_databases() {
        let config = create_test_config();
        let manager = DataSourceManager::new(config);
        
        let all_dbs = manager.get_all_datasource_databases();
        assert_eq!(all_dbs.len(), 2);
        assert_eq!(all_dbs.get("ds_mysql"), Some(&vec!["db1".to_string(), "db2".to_string()]));
        assert_eq!(all_dbs.get("ds_pg"), Some(&vec!["db3".to_string(), "db4".to_string()]));
    }
}