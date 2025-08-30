use std::fmt::Debug;

pub trait SqlDialect: Debug + Send + Sync {
    #[allow(dead_code)]
    fn get_name(&self) -> &str;
    fn escape_identifier(&self, identifier: &str) -> String;
    fn build_columns(&self, columns_str: &str) -> Vec<String>;
    fn build_limit(&self, limit: usize, offset: usize) -> String;
}

#[derive(Debug, Clone)]
pub struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    #[allow(dead_code)]
    fn get_name(&self) -> &str {
        "mysql"
    }
    
    fn escape_identifier(&self, identifier: &str) -> String {
        format!("`{}`", identifier)
    }
    
    fn build_columns(&self, columns_str: &str) -> Vec<String> {
        columns_str
            .split(',')
            .map(|s| {
                let trimmed = s.trim();
                if trimmed.contains(" as ") || trimmed.contains(" AS ") {
                    // 处理别名
                    trimmed.to_string()
                } else {
                    // 普通字段，添加反引号
                    format!("`{}`", trimmed)
                }
            })
            .collect()
    }
    
    fn build_limit(&self, limit: usize, offset: usize) -> String {
        if offset > 0 {
            format!("LIMIT {} OFFSET {}", limit, offset)
        } else {
            format!("LIMIT {}", limit)
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgreSqlDialect;

impl SqlDialect for PostgreSqlDialect {
    #[allow(dead_code)]
    fn get_name(&self) -> &str {
        "postgres"
    }
    
    fn escape_identifier(&self, identifier: &str) -> String {
        format!("\"{}\"", identifier)
    }
    
    fn build_columns(&self, columns_str: &str) -> Vec<String> {
        columns_str
            .split(',')
            .map(|s| {
                let trimmed = s.trim();
                if trimmed.contains(" as ") || trimmed.contains(" AS ") {
                    // 处理别名
                    trimmed.to_string()
                } else {
                    // 普通字段，添加双引号
                    format!("\"{}\"", trimmed)
                }
            })
            .collect()
    }
    
    fn build_limit(&self, limit: usize, offset: usize) -> String {
        if offset > 0 {
            format!("LIMIT {} OFFSET {}", limit, offset)
        } else {
            format!("LIMIT {}", limit)
        }
    }
}