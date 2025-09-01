use indexmap::IndexMap;
use std::ptr::null;
use serde_json::Value;
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
    params: Vec<Value>,
    group: Option<String>,
    having: Option<String>,
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
            group: None,
            having: None,
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
    pub fn get_params(&self) -> Vec<Value> {
        self.params.clone()
    }
    
    pub fn get_string_params(&self) -> Vec<String> {
        self.params.iter().map(|v| {
            match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => v.to_string(),
            }
        }).collect()
    }

    #[allow(dead_code)]
    pub async fn exec(&self, db: &DBConn) -> Result<Vec<IndexMap<String, Value>>, sqlx::Error> {
        let sql = self.to_sql();
        log::info!("sql.exec: {}, params: {}", sql, serde_json::to_string(&self.params).unwrap());
        let params: Vec<String> = self.params.iter()
            .map(|v| match v {
                Value::Null => "NULL".to_string(),
                Value::String(s) => s.clone(),
                Value::Array(_) | Value::Object(_) =>
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

    pub fn parse_condition(&mut self, field: &str, value: &Value) {
        // 处理特殊参数
        if field.starts_with('@') {
            match &field[1..] {
                "column" => {
                    if let Value::String(cols) = value {
                        // 使用dialect的build_columns方法处理字段选择和别名
                        let dialect = self.get_dialect();
                        let columns = dialect.build_columns(cols.replace(";", ", ").as_str());
                        self.columns = columns;
                    }
                }
                "group" => {
                    if let Value::String(group) = value {
                        self.group = Some(group.to_string());
                    }
                }
                "having" => {
                    if let Value::String(having) = value {
                        self.having = Some(having.to_string().replace(";", ", "));
                    }
                }
                "order" => {
                    if let Value::String(order) = value {
                        self.order = Some(order.to_string().replace("+", " ASC ").replace("-", " DESC "));
                    }
                }
                _ => {}
            }
            return;
        }
        
        // 处理各种查询条件 https://github.com/Tencent/APIJSON/blob/master/Document.md#3.2
        if field.ends_with('$') {
            // 模糊搜索 https://github.com/Tencent/APIJSON/blob/master/APIJSONORM/src/main/java/apijson/orm/AbstractSQLConfig.java#L4114-L4232
            let actual_field = &field[..field.len() - 1];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} LIKE ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with('~') {
             // 正则匹配 https://github.com/Tencent/APIJSON/blob/master/APIJSONORM/src/main/java/apijson/orm/AbstractSQLConfig.java#L4236-L4323
             let actual_field = &field[..field.len() - 1];
             let dialect = self.get_dialect();
             let escaped_field = dialect.escape_identifier(actual_field);
             let regex_op = match self.dialect { // FIXME 根据不同数据库类型及版本来适配，MySQL 8.0+ 用 regexp_like()
                 DataSourceKind::Mysql => if field.ends_with("*~") { "REGEXP" } else { "REGEXP BINARY" },
                 DataSourceKind::Postgres => if field.ends_with("*~") { "*~" } else {  "~" },
             };
             self.where_clauses.push(format!("{} {} ?", escaped_field, regex_op));
             self.params.push(value.to_owned());
        } else if field.ends_with("{}") {
            // IN查询
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            match value {
                Value::Array(values) => {
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
        } else if field.ends_with("%") {
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} BETWEEN ? AND ?", escaped_field));

            let vals = value.as_str().unwrap().split(',').collect::<Vec<&str>>();
            assert_eq!(vals.len(), 2);
            self.params.push(Value::from(vals[0]));
            self.params.push(Value::from(vals[1]));
        } else if field.ends_with("<>") {
            // json contains 查询 https://github.com/Tencent/APIJSON/blob/master/APIJSONORM/src/main/java/apijson/orm/AbstractSQLConfig.java#L4561-L4656
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            match value {
                Value::Array(values) => {
                    if !values.is_empty() {
                        let placeholders = vec!["?"; values.len()].join(","); // FIXME 根据不同数据库类型及版本来适配
                        self.where_clauses.push(format!("json_contains({}, {}, '$')", escaped_field, placeholders));
                        self.params.extend(values.to_owned());
                    }
                }
                _ => {
                    self.where_clauses.push(format!("{} != ?", escaped_field));
                    self.params.push(value.to_owned());
                }
            }
        } else if field.ends_with(">=") {
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} >= ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with("<=") {
            let actual_field = &field[..field.len() - 2];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} <= ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with(">") {
            let actual_field = &field[..field.len() - 1];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} > ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with("<") {
            let actual_field = &field[..field.len() - 1];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} < ?", escaped_field));
            self.params.push(value.to_owned());
        } else if field.ends_with("!") {
            let actual_field = &field[..field.len() - 1];
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(actual_field);
            self.where_clauses.push(format!("{} != ?", escaped_field));
            self.params.push(value.to_owned());
        } else {
            // 普通等值查询
            let dialect = self.get_dialect();
            let escaped_field = dialect.escape_identifier(field);
            match value {
                Value::Object(values) => {
                    assert!(false, "还未实现子查询！！！！！");
                    // if !values.is_empty() {
                    //     let placeholders = vec!["?"; values.len()].join(",");
                    //     self.where_clauses.push(format!("{} IN ({})", escaped_field, placeholders));
                    //     self.params.extend(values.to_owned());
                    // }
                }
                _ => {
                    self.where_clauses.push(format!("{} = ?", escaped_field));
                    self.params.push(value.to_owned());
                }
            }
        }
    }

    pub fn page_size(&mut self, page: Value, count: Value) {
        self.page = Self::parse_num(&page, 0);
        self.limit = Self::parse_num(&count, 10);
    }

    fn parse_num(value: &Value, default_val: i32) -> i32 {
        match value {
            Value::Number(n) => n.as_f64()
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