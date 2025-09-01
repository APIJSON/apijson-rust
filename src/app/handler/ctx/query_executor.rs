use indexmap::IndexMap;
use std::ptr::null;
use crate::app::datasource::mysql::DBConn;
use crate::app::datasource::metadata::get_table;
use crate::app::handler::ctx::dialect::{SqlDialect, MySqlDialect, PostgreSqlDialect};
use crate::app::datasource::config::DataSourceKind;

pub const DEFAULT_MAX_COUNT: usize = 10;

#[derive(Debug, Clone)]
pub struct QueryExecutor {
    schema: String,
    table: String,
    columns: Vec<String>,
    where_clauses: Vec<String>,
    params: Vec<serde_json::Value>,
    order: Option<String>,
    page: i32,
    limit: i32,
    dialect: DataSourceKind,
}

impl QueryExecutor {
    pub fn new(datasource_kind: DataSourceKind) -> Self {
        QueryExecutor {
            schema: String::new(),
            table: String::new(),
            columns: vec![],
            where_clauses: vec![],
            params: vec![],
            order: None,
            page: 0,
            limit: 1,
            dialect: datasource_kind,
        }
    }
    
    fn get_dialect(&self) -> Box<dyn SqlDialect> {
        match self.dialect {
            DataSourceKind::Mysql => Box::new(MySqlDialect),
            DataSourceKind::Postgres => Box::new(PostgreSqlDialect),
        }
    }
    
    #[allow(dead_code)]
    pub fn get_params(&self) -> Vec<serde_json::Value> {
        self.params.clone()
    }
    
    pub fn get_string_params(&self) -> Vec<String> {
        self.params.iter().map(|v| {
            match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "NULL".to_string(),
                _ => v.to_string(),
            }
        }).collect()
    }

    #[allow(dead_code)]
    pub async fn exec(&self, db: &DBConn) -> Result<Vec<IndexMap<String, serde_json::Value>>, sqlx::Error> {
        let sql = self.to_sql();
        log::info!("sql.exec: {}, params: {}", sql, serde_json::to_string(&self.params).unwrap());
        let params: Vec<String> = self.params.iter()
            .map(|v| match v {
                serde_json::Value::Null => "NULL".to_string(),
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Array(_) | serde_json::Value::Object(_) =>
                    serde_json::to_string(v).unwrap_or_else(|_| "NULL".to_string()),
                _ => v.to_string(),
            })
            .collect();
        db.query_list(&sql, params).await
    }

    pub fn to_sql(&self) -> String {
        let dialect = self.get_dialect();
        let mut sql = String::new();
        
        // SELECT子句
        sql.push_str("SELECT ");
        if self.columns.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.columns.join(", "));
        }
        
        // FROM子句

        let escaped_table = dialect.escape_identifier(&self.table);
        if (self.schema.is_empty()) { // FIXME 必须有 schema，否则查询总是为空
            sql.push_str(&format!(" FROM {}", escaped_table));
        } else {
            let escaped_schema = dialect.escape_identifier(&self.schema);
            sql.push_str(&format!(" FROM {}.{}", escaped_schema, escaped_table));
        }
        
        // WHERE子句
        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clauses.join(" AND "));
        }
        
        // ORDER BY子句
        if let Some(order) = &self.order {
            sql.push_str(&format!(" ORDER BY {}", order));
        }
        
        // LIMIT和OFFSET子句
        if self.limit > 0 {
            let offset = (self.limit * self.page) as usize;
            sql.push_str(&format!(" {}", dialect.build_limit(self.limit as usize, offset)));
        }
        
        sql
    }
    
    pub fn parse_table(&mut self, table_key: &str) -> Result<String, String> {
        let table_key = if table_key.ends_with("[]") { &table_key[..table_key.len()-2] } else { table_key };
        let keys = table_key.split("-").collect::<Vec<&str>>();
        let l = keys.len();
        let table = if l < 1 {""} else {keys[0]};
        // match get_table(&schema, table) {
        //     Some(table) => {
        //         self.table = table.name.clone();
        //         self.schema = table.schema.clone();
        //         Ok(())
        //     },
        //     None => Err(format!("table: {} not exists", table_key))
        // }
        if (table.is_empty()) {
            Err(format!("Table is empty"))?
        }
        self.table = table.to_string().clone();
        self.schema = "sys".to_string();
        Ok(table.to_string())
    }

    pub fn parse_condition(&mut self, field: &str, value: &serde_json::Value) {
        // 处理特殊参数
        if field.starts_with('@') {
            match &field[1..] {
                "order" => {
                    if let serde_json::Value::String(order) = value {
                        self.order = Some(order.to_string());
                    }
                }
                "column" => {
                    if let serde_json::Value::String(cols) = value {
                        // 使用dialect的build_columns方法处理字段选择和别名
                        let dialect = self.get_dialect();
                        let columns = dialect.build_columns(cols);
                        self.columns = columns;
                    }
                }
                _ => {}
            }
            return;
        }
        
        // 处理各种查询条件
        if field.ends_with('$') {
            // 模糊查询
            let actual_field = &field[..field.len() - 1];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} LIKE ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with('?') {
             // 正则匹配
             let actual_field = &field[..field.len() - 1];
             let dialect = self.get_dialect();
             let escaped_field = dialect.escape_identifier(actual_field);
             let regex_op = match self.dialect {
                 DataSourceKind::Mysql => "REGEXP",
                 DataSourceKind::Postgres => "~",
             };
             self.where_clauses.push(format!("{} {} ?", escaped_field, regex_op));
             self.params.push(value.to_owned());
        } else if field.ends_with("{}") {
            // IN查询
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            match value {
                serde_json::Value::Array(values) => {
                    if !values.is_empty() {
                        let placeholders = vec!["?"; values.len()].join(",");
                        self.where_clauses.push(format!("{} IN ({})", escaped_field, placeholders));
                        self.params.extend(values.to_owned());
                    }
                }
                _ => {
                    self.where_clauses.push(format!("{} = ?", escaped_field));
                    self.params.push(value.to_owned());
                }
            }
        } else if field.ends_with("<>") {
            // NOT IN查询
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            match value {
                serde_json::Value::Array(values) => {
                    if !values.is_empty() {
                        let placeholders = vec!["?"; values.len()].join(",");
                        self.where_clauses.push(format!("{} NOT IN ({})", escaped_field, placeholders));
                        self.params.extend(values.to_owned());
                    }
                }
                _ => {
                    self.where_clauses.push(format!("{} != ?", escaped_field));
                    self.params.push(value.to_owned());
                }
            }
        } else {
            // 普通等值查询
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(field);
            match value {
                serde_json::Value::Array(values) => {
                    if !values.is_empty() {
                        let placeholders = vec!["?"; values.len()].join(",");
                        self.where_clauses.push(format!("{} IN ({})", escaped_field, placeholders));
                        self.params.extend(values.to_owned());
                    }
                }
                _ => {
                    self.where_clauses.push(format!("{} = ?", escaped_field));
                    self.params.push(value.to_owned());
                }
            }
        }
    }

    pub fn page_size(&mut self, page: serde_json::Value, count: serde_json::Value) {
        self.page = Self::parse_num(&page, 0);
        self.limit = Self::parse_num(&count, 10);
    }

    fn parse_num(value: &serde_json::Value, default_val: i32) -> i32 {
        match value {
            serde_json::Value::Number(n) => n.as_f64()
                .map(|f| f as i32)
                .unwrap_or(default_val),
            _ => default_val,
        }
    }

    pub fn add_column(&mut self, column: &str) {
        // *代替，必然包含所有字段
        if self.columns.is_empty() { return; }
        // 包含当前字段，跳过
        if self.columns.iter().any(|c| c.eq(column)) { return; }
        self.columns.push(column.to_string());
    }
}