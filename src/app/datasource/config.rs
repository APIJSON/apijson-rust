use serde::{Deserialize, Serialize};

/// 数据源类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataSourceKind {
    /// MySQL 数据库
    Mysql,
    /// PostgreSQL 数据库
    Postgres,
}

/// 单个数据源配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// 数据源名称
    pub name: String,
    /// 数据源类型
    pub kind: DataSourceKind,
    /// 用户名
    pub username: String,
    /// 密码
    pub password: String,
    /// 连接URL
    pub url: String,
    /// 数据库列表
    pub database: Vec<String>,
    /// 是否为默认数据源
    #[serde(default)]
    pub default: bool,
}

/// 多数据源配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourcesConfig {
    /// 数据源列表
    pub datasource: Vec<DataSourceConfig>,
}

impl DataSourceConfig {
    /// 构建完整的数据库连接URL
    /// 
    /// # 参数
    /// * `database` - 数据库名称
    /// 
    /// # 返回值
    /// 返回包含数据库名称的完整连接URL
    pub fn build_connection_url(&self, database: &str) -> String {
        match self.kind {
            DataSourceKind::Mysql => {
                // MySQL URL 格式: mysql://username:password@host:port/database
                if self.url.contains("://") {
                    // 如果URL已经包含协议，替换用户名密码和数据库
                    let url_parts: Vec<&str> = self.url.split("://").collect();
                    if url_parts.len() == 2 {
                        let protocol = url_parts[0];
                        let host_part = url_parts[1];
                        // 提取主机和端口部分（去掉可能存在的用户名密码）
                        let host_port = if host_part.contains("@") {
                            host_part.split("@").nth(1).unwrap_or(host_part)
                        } else {
                            host_part
                        };
                        // 去掉可能存在的数据库名
                        let host_port = host_port.split("/").next().unwrap_or(host_port);
                        format!("{}://{}:{}@{}/{}", protocol, self.username, self.password, host_port, database)
                    } else {
                        format!("{}/{}", self.url, database)
                    }
                } else {
                    format!("mysql://{}:{}@{}/{}", self.username, self.password, self.url, database)
                }
            }
            DataSourceKind::Postgres => {
                // PostgreSQL URL 格式: postgres://username:password@host:port/database
                if self.url.contains("://") {
                    let url_parts: Vec<&str> = self.url.split("://").collect();
                    if url_parts.len() == 2 {
                        let protocol = url_parts[0];
                        let host_part = url_parts[1];
                        let host_port = if host_part.contains("@") {
                            host_part.split("@").nth(1).unwrap_or(host_part)
                        } else {
                            host_part
                        };
                        let host_port = host_port.split("/").next().unwrap_or(host_port);
                        format!("{}://{}:{}@{}/{}", protocol, self.username, self.password, host_port, database)
                    } else {
                        format!("{}/{}", self.url, database)
                    }
                } else {
                    format!("postgres://{}:{}@{}/{}", self.username, self.password, self.url, database)
                }
            }
        }
    }
}

impl DataSourcesConfig {
    /// 获取默认数据源
    /// 
    /// # 返回值
    /// 返回第一个标记为默认的数据源，如果没有则返回第一个数据源
    pub fn get_default_datasource(&self) -> Option<&DataSourceConfig> {
        // 首先查找标记为默认的数据源
        self.datasource.iter().find(|ds| ds.default)
            // 如果没有找到默认数据源，返回第一个数据源
            .or_else(|| self.datasource.first())
    }
    
    /// 根据名称获取数据源
    /// 
    /// # 参数
    /// * `name` - 数据源名称
    /// 
    /// # 返回值
    /// 返回匹配名称的数据源配置
    pub fn get_datasource_by_name(&self, name: &str) -> Option<&DataSourceConfig> {
        self.datasource.iter().find(|ds| ds.name == name)
    }
    
    /// 获取所有数据源名称
    /// 
    /// # 返回值
    /// 返回所有数据源名称的向量
    pub fn get_datasource_names(&self) -> Vec<String> {
        self.datasource.iter().map(|ds| ds.name.clone()).collect()
    }
}