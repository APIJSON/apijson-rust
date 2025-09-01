use crate::app::datasource::config::DataSourceKind;
use serde_json::{Value, Map};

/// SQL方言特性
pub trait SqlDialect: Send + Sync {
    /// 构建INSERT语句
    fn build_insert_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        values: &[String],
    ) -> String;

    /// 构建UPDATE语句
    fn build_update_sql(
        &self,
        schema: &str,
        table: &str,
        set_clauses: &[String],
        where_clause: &str,
    ) -> String;

    /// 构建DELETE语句
    fn build_delete_sql(
        &self,
        schema: &str,
        table: &str,
        where_clause: &str,
    ) -> String;

    /// 构建SELECT语句
    fn build_select_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        where_clause: Option<&str>,
        order_by: Option<&str>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String;

    /// 转义标识符（表名、字段名等）
    fn escape_identifier(&self, identifier: &str) -> String;

    /// 转义字符串值
    fn escape_string_value(&self, value: &str) -> String;

    /// 格式化值（根据类型）
    fn format_value(&self, value: &Value) -> String;

    /// 构建WHERE子句
    fn build_wheres(&self, conditions: &Map<String, Value>) -> String;

    /// 构建SELECT字段列表，支持字段选择和别名
    /// 
    /// # 参数
    /// * `column_spec` - 字段规范，格式："field1,field2:alias2,field3" 或 "*" 表示所有字段
    /// 
    /// # 返回值
    /// 返回格式化的字段列表字符串
    fn build_columns(&self, column_spec: &str) -> Vec<String>;
}

/// MySQL方言实现
pub struct MySqlDialect;

impl MySqlDialect {
    /// 解析单个条件
    fn parse_condition(&self, key: &str, value: &Value) -> String {
        // 解析逻辑运算符
        let (field_name, logic_op, operator) = self.parse_key(key);
        
        match operator.as_str() {
            "{}" => {
                // 对于{}操作符，需要判断值的类型来决定是IN条件还是范围条件
                match value {
                    Value::Array(_) => self.build_in_condition(&field_name, value, &logic_op),
                    Value::String(s) if s.contains(',') && (s.contains('<') || s.contains('>') || s.contains('=')) => {
                        self.build_range_condition(&field_name, value, &logic_op)
                    },
                    _ => self.build_in_condition(&field_name, value, &logic_op),
                }
            },
            "<>" => self.build_contains_condition(&field_name, value),
            "$" => self.build_like_condition(&field_name, value),
            "?" => self.build_regexp_condition(&field_name, value),
            "~" => self.build_regexp_condition(&field_name, value),
            "%" => self.build_between_condition(&field_name, value),
            _ => self.build_range_condition(&field_name, value, &logic_op),
        }
    }
    
    /// 解析键名，提取字段名、逻辑运算符和操作符
    fn parse_key(&self, key: &str) -> (String, String, String) {
        let mut field_name = key.to_string();
        let mut logic_op = "|".to_string(); // 默认OR
        let mut operator = "".to_string();
        
        // 检查逻辑运算符
        if key.contains("&") {
            logic_op = "&".to_string();
        } else if key.contains("!") {
            logic_op = "!".to_string();
        }
        
        // 提取操作符
        if key.contains("{}") {
            operator = "{}".to_string();
            field_name = key.replace("&{}", "").replace("|{}", "").replace("!{}", "").replace("{}", "");
        } else if key.contains("<>") {
            operator = "<>".to_string();
            field_name = key.replace("<>", "");
        } else if key.contains("$") {
            operator = "$".to_string();
            field_name = key.replace("$", "");
        } else if key.contains("?") {
            operator = "?".to_string();
            field_name = key.replace("?", "");
        } else if key.contains("~") {
            operator = "~".to_string();
            field_name = key.replace("~", "");
        } else if key.contains("%") {
            operator = "%".to_string();
            field_name = key.replace("%", "");
        }
        
        (field_name, logic_op, operator)
    }
    
    /// 构建IN条件
    fn build_in_condition(&self, field: &str, value: &Value, logic_op: &str) -> String {
        let escaped_field = self.escape_identifier(field);
        
        match value {
            Value::Array(arr) => {
                let values: Vec<String> = arr.iter().map(|v| self.format_value(v)).collect();
                let condition = format!("{} IN ({})", escaped_field, values.join(","));
                if logic_op == "!" {
                    format!("{} NOT IN ({})", escaped_field, values.join(","))
                } else {
                    condition
                }
            },
            _ => {
                let formatted_value = self.format_value(value);
                if logic_op == "!" {
                    format!("{} != {}", escaped_field, formatted_value)
                } else {
                    format!("{} = {}", escaped_field, formatted_value)
                }
            }
        }
    }
    
    /// 构建包含条件（JSON_CONTAINS）
    fn build_contains_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        let formatted_value = self.format_value(value);
        format!("JSON_CONTAINS({}, {})", escaped_field, formatted_value)
    }
    
    /// 构建LIKE条件
    fn build_like_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                format!("{} LIKE {}", escaped_field, self.escape_string_value(s))
            },
            Value::Array(arr) => {
                let conditions: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{} LIKE {}", escaped_field, self.escape_string_value(s)))
                    .collect();
                if conditions.is_empty() {
                    "1=1".to_string()
                } else {
                    format!("({})", conditions.join(" OR "))
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建正则表达式条件
    fn build_regexp_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                format!("{} REGEXP {}", escaped_field, self.escape_string_value(s))
            },
            Value::Array(arr) => {
                let conditions: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{} REGEXP {}", escaped_field, self.escape_string_value(s)))
                    .collect();
                if conditions.is_empty() {
                    "1=1".to_string()
                } else {
                    format!("({})", conditions.join(" OR "))
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建BETWEEN条件
    fn build_between_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                let parts: Vec<&str> = s.split(',').collect();
                if parts.len() == 2 {
                    let start = self.escape_string_value(parts[0].trim());
                    let end = self.escape_string_value(parts[1].trim());
                    format!("{} BETWEEN {} AND {}", escaped_field, start, end)
                } else {
                    "1=1".to_string()
                }
            },
            Value::Array(arr) => {
                if arr.len() == 2 {
                    let start = self.format_value(&arr[0]);
                    let end = self.format_value(&arr[1]);
                    format!("{} BETWEEN {} AND {}", escaped_field, start, end)
                } else {
                    "1=1".to_string()
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建范围条件
    fn build_range_condition(&self, field: &str, value: &Value, logic_op: &str) -> String {
        let escaped_field = self.escape_identifier(field);
        
        match value {
            Value::String(s) => {
                let conditions: Vec<String> = s.split(',')
                    .map(|condition| {
                        let condition = condition.trim();
                        if condition.starts_with("<=") {
                            format!("{} <= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with(">=") {
                            format!("{} >= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("<") {
                            format!("{} < {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with(">") {
                            format!("{} > {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with("!=") {
                            format!("{} != {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("=") {
                            format!("{} = {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else {
                            format!("{} = {}", escaped_field, self.escape_string_value(condition))
                        }
                    })
                    .collect();
                let joiner = if logic_op == "&" { " AND " } else { " OR " };
                format!("({})", conditions.join(joiner))
            },
            Value::Array(arr) => {
                let conditions: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|condition| {
                        let condition = condition.trim();
                        if condition.starts_with("<=") {
                            format!("{} <= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with(">=") {
                            format!("{} >= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("<") {
                            format!("{} < {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with(">") {
                            format!("{} > {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with("!=") {
                            format!("{} != {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("=") {
                            format!("{} = {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else {
                            format!("{} = {}", escaped_field, self.escape_string_value(condition))
                        }
                    })
                    .collect();
                let joiner = if logic_op == "&" { " AND " } else { " OR " };
                format!("({})", conditions.join(joiner))
            },
            _ => "1=1".to_string(),
        }
    }
}

impl SqlDialect for MySqlDialect {
    fn build_insert_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        values: &[String],
    ) -> String {
        let escaped_fields: Vec<String> = fields
            .iter()
            .map(|f| self.escape_identifier(f))
            .collect();
        
        format!(
            "INSERT INTO {}.{}({}) VALUES({})",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            escaped_fields.join(","),
            values.join(",")
        )
    }

    fn build_update_sql(
        &self,
        schema: &str,
        table: &str,
        set_clauses: &[String],
        where_clause: &str,
    ) -> String {
        format!(
            "UPDATE {}.{} SET {} WHERE {}",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            set_clauses.join(","),
            where_clause
        )
    }

    fn build_delete_sql(
        &self,
        schema: &str,
        table: &str,
        where_clause: &str,
    ) -> String {
        format!(
            "DELETE FROM {}.{} WHERE {}",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            where_clause
        )
    }

    fn build_select_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        where_clause: Option<&str>,
        order_by: Option<&str>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let escaped_fields: Vec<String> = if fields.is_empty() {
            vec!["*".to_string()]
        } else {
            fields.iter().map(|f| self.escape_identifier(f)).collect()
        };

        let mut sql = format!(
            "SELECT {} FROM {}.{}",
            escaped_fields.join(","),
            self.escape_identifier(schema),
            self.escape_identifier(table)
        );

        if let Some(where_clause) = where_clause {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if let Some(order_by) = order_by {
            sql.push_str(&format!(" ORDER BY {}", order_by));
        }

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {}", limit));
            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        sql
    }

    fn escape_identifier(&self, identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn escape_string_value(&self, value: &str) -> String {
        format!("'{}'", value.replace("'", "''").replace("\\", "\\\\"))
    }

    fn format_value(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => self.escape_string_value(s),
            _ => self.escape_string_value(&value.to_string()),
        }
    }

    fn build_wheres(&self, conditions: &Map<String, Value>) -> String {
        let mut where_clauses = Vec::new();
        
        for (key, value) in conditions {
            if value.is_null() {
                continue;
            }
            
            let clause = self.parse_condition(key, value);
            if !clause.is_empty() {
                where_clauses.push(clause);
            }
        }
        
        if where_clauses.is_empty() {
            "1=1".to_string()
        } else {
            where_clauses.join(" AND ")
        }
    }

    fn build_columns(&self, column_spec: &str) -> Vec<String> {
        if column_spec.trim() == "*" {
            return vec!["*".to_string()];
        }
        
        column_spec
            .split(',')
            .map(|col| {
                let col = col.trim();
                if col.contains(':') {
                    // 处理别名：field:alias
                    let parts: Vec<&str> = col.split(':').collect();
                    if parts.len() == 2 {
                        format!("{} AS {}", self.escape_identifier(parts[0].trim()), self.escape_identifier(parts[1].trim()))
                    } else {
                        self.escape_identifier(col)
                    }
                } else {
                    // 普通字段
                    self.escape_identifier(col)
                }
            })
            .collect()
    }
}

/// PostgreSQL方言实现
pub struct PostgreSqlDialect;

impl PostgreSqlDialect {
    /// 解析单个条件
    fn parse_condition(&self, key: &str, value: &Value) -> String {
        // 解析逻辑运算符
        let (field_name, logic_op, operator) = self.parse_key(key);
        
        match operator.as_str() {
            "{}" => {
                // 对于{}操作符，需要判断值的类型来决定是IN条件还是范围条件
                match value {
                    Value::Array(_) => self.build_in_condition(&field_name, value, &logic_op),
                    Value::String(s) if s.contains(',') && (s.contains('<') || s.contains('>') || s.contains('=')) => {
                        self.build_range_condition(&field_name, value, &logic_op)
                    },
                    _ => self.build_in_condition(&field_name, value, &logic_op),
                }
            },
            "<>" => self.build_contains_condition(&field_name, value),
            "$" => self.build_like_condition(&field_name, value),
            "?" => self.build_regexp_condition(&field_name, value),
            "~" => self.build_regexp_condition(&field_name, value),
            "%" => self.build_between_condition(&field_name, value),
            _ => self.build_range_condition(&field_name, value, &logic_op),
        }
    }
    
    /// 解析键名，提取字段名、逻辑运算符和操作符
    fn parse_key(&self, key: &str) -> (String, String, String) {
        let mut field_name = key.to_string();
        let mut logic_op = "|".to_string(); // 默认OR
        let mut operator = "".to_string();
        
        // 检查逻辑运算符
        if key.contains("&") {
            logic_op = "&".to_string();
        } else if key.contains("!") {
            logic_op = "!".to_string();
        }
        
        // 提取操作符
        if key.contains("{}") {
            operator = "{}".to_string();
            field_name = key.replace("&{}", "").replace("|{}", "").replace("!{}", "").replace("{}", "");
        } else if key.contains("<>") {
            operator = "<>".to_string();
            field_name = key.replace("<>", "");
        } else if key.contains("$") {
            operator = "$".to_string();
            field_name = key.replace("$", "");
        } else if key.contains("?") {
            operator = "?".to_string();
            field_name = key.replace("?", "");
        } else if key.contains("~") {
            operator = "~".to_string();
            field_name = key.replace("~", "");
        } else if key.contains("%") {
            operator = "%".to_string();
            field_name = key.replace("%", "");
        }
        
        (field_name, logic_op, operator)
    }
    
    /// 构建IN条件
    fn build_in_condition(&self, field: &str, value: &Value, logic_op: &str) -> String {
        let escaped_field = self.escape_identifier(field);
        
        match value {
            Value::Array(arr) => {
                let values: Vec<String> = arr.iter().map(|v| self.format_value(v)).collect();
                let condition = format!("{} IN ({})", escaped_field, values.join(","));
                if logic_op == "!" {
                    format!("{} NOT IN ({})", escaped_field, values.join(","))
                } else {
                    condition
                }
            },
            _ => {
                let formatted_value = self.format_value(value);
                if logic_op == "!" {
                    format!("{} != {}", escaped_field, formatted_value)
                } else {
                    format!("{} = {}", escaped_field, formatted_value)
                }
            }
        }
    }
    
    /// 构建包含条件（PostgreSQL使用@>操作符）
    fn build_contains_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        let formatted_value = self.format_value(value);
        format!("{} @> {}", escaped_field, formatted_value)
    }
    
    /// 构建LIKE条件
    fn build_like_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                format!("{} LIKE {}", escaped_field, self.escape_string_value(s))
            },
            Value::Array(arr) => {
                let conditions: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{} LIKE {}", escaped_field, self.escape_string_value(s)))
                    .collect();
                if conditions.is_empty() {
                    "1=1".to_string()
                } else {
                    format!("({})", conditions.join(" OR "))
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建正则表达式条件（PostgreSQL使用~操作符）
    fn build_regexp_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                format!("{} ~ {}", escaped_field, self.escape_string_value(s))
            },
            Value::Array(arr) => {
                let conditions: Vec<String> = arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{} ~ {}", escaped_field, self.escape_string_value(s)))
                    .collect();
                if conditions.is_empty() {
                    "1=1".to_string()
                } else {
                    format!("({})", conditions.join(" OR "))
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建BETWEEN条件
    fn build_between_condition(&self, field: &str, value: &Value) -> String {
        let escaped_field = self.escape_identifier(field);
        match value {
            Value::String(s) => {
                let parts: Vec<&str> = s.split(',').collect();
                if parts.len() == 2 {
                    let start = self.escape_string_value(parts[0].trim());
                    let end = self.escape_string_value(parts[1].trim());
                    format!("{} BETWEEN {} AND {}", escaped_field, start, end)
                } else {
                    "1=1".to_string()
                }
            },
            Value::Array(arr) => {
                if arr.len() == 2 {
                    let start = self.format_value(&arr[0]);
                    let end = self.format_value(&arr[1]);
                    format!("{} BETWEEN {} AND {}", escaped_field, start, end)
                } else {
                    "1=1".to_string()
                }
            },
            _ => "1=1".to_string()
        }
    }
    
    /// 构建范围条件
    fn build_range_condition(&self, field: &str, value: &Value, logic_op: &str) -> String {
        let escaped_field = self.escape_identifier(field);
        
        match value {
            Value::String(s) => {
                let conditions: Vec<String> = s.split(',')
                    .map(|condition| {
                        let condition = condition.trim();
                        if condition.starts_with("<=") {
                            format!("{} <= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with(">=") {
                            format!("{} >= {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("<") {
                            format!("{} < {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with(">") {
                            format!("{} > {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else if condition.starts_with("!=") {
                            format!("{} != {}", escaped_field, self.escape_string_value(&condition[2..]))
                        } else if condition.starts_with("=") {
                            format!("{} = {}", escaped_field, self.escape_string_value(&condition[1..]))
                        } else {
                            format!("{} = {}", escaped_field, self.escape_string_value(condition))
                        }
                    })
                    .collect();
                
                if conditions.is_empty() {
                    "1=1".to_string()
                } else {
                    let connector = if logic_op == "&" { " AND " } else { " OR " };
                    format!("({})", conditions.join(connector))
                }
            },
            _ => {
                let formatted_value = self.format_value(value);
                format!("{} = {}", escaped_field, formatted_value)
            }
        }
    }
}

impl SqlDialect for PostgreSqlDialect {
    fn build_insert_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        values: &[String],
    ) -> String {
        let escaped_fields: Vec<String> = fields
            .iter()
            .map(|f| self.escape_identifier(f))
            .collect();
        
        format!(
            "INSERT INTO {}.{}({}) VALUES({})",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            escaped_fields.join(","),
            values.join(",")
        )
    }

    fn build_update_sql(
        &self,
        schema: &str,
        table: &str,
        set_clauses: &[String],
        where_clause: &str,
    ) -> String {
        format!(
            "UPDATE {}.{} SET {} WHERE {}",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            set_clauses.join(","),
            where_clause
        )
    }

    fn build_delete_sql(
        &self,
        schema: &str,
        table: &str,
        where_clause: &str,
    ) -> String {
        format!(
            "DELETE FROM {}.{} WHERE {}",
            self.escape_identifier(schema),
            self.escape_identifier(table),
            where_clause
        )
    }

    fn build_select_sql(
        &self,
        schema: &str,
        table: &str,
        fields: &[String],
        where_clause: Option<&str>,
        order_by: Option<&str>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let escaped_fields: Vec<String> = if fields.is_empty() {
            vec!["*".to_string()]
        } else {
            fields.iter().map(|f| self.escape_identifier(f)).collect()
        };

        let mut sql = format!(
            "SELECT {} FROM {}.{}",
            escaped_fields.join(","),
            self.escape_identifier(schema),
            self.escape_identifier(table)
        );

        if let Some(where_clause) = where_clause {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if let Some(order_by) = order_by {
            sql.push_str(&format!(" ORDER BY {}", order_by));
        }

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }

    fn escape_identifier(&self, identifier: &str) -> String {
        format!("\"{}\"", identifier.replace("\"", "\"\""))
    }

    fn escape_string_value(&self, value: &str) -> String {
        format!("'{}'", value.replace("'", "''"))
    }

    fn format_value(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => self.escape_string_value(s),
            _ => self.escape_string_value(&value.to_string()),
        }
    }

    fn build_wheres(&self, conditions: &Map<String, Value>) -> String {
        let mut where_clauses = Vec::new();
        
        for (key, value) in conditions {
            if value.is_null() {
                continue;
            }
            
            let clause = self.parse_condition(key, value);
            if !clause.is_empty() {
                where_clauses.push(clause);
            }
        }
        
        if where_clauses.is_empty() {
            "1=1".to_string()
        } else {
            where_clauses.join(" AND ")
        }
    }

    fn build_columns(&self, column_spec: &str) -> Vec<String> {
        if column_spec.trim() == "*" {
            return vec!["*".to_string()];
        }
        
        column_spec
            .split(',')
            .map(|col| {
                let col = col.trim();
                if col.contains(':') {
                    // 处理别名：field:alias
                    let parts: Vec<&str> = col.split(':').collect();
                    if parts.len() == 2 {
                        format!("{} AS {}", self.escape_identifier(parts[0].trim()), self.escape_identifier(parts[1].trim()))
                    } else {
                        self.escape_identifier(col)
                    }
                } else {
                    // 普通字段
                    self.escape_identifier(col)
                }
            })
            .collect()
    }
}

/// 方言工厂
pub struct DialectFactory;

impl DialectFactory {
    /// 根据数据源类型创建对应的方言实现
    pub fn create_dialect(kind: &DataSourceKind) -> Box<dyn SqlDialect> {
        match kind {
            DataSourceKind::Mysql => Box::new(MySqlDialect),
            DataSourceKind::Postgres => Box::new(PostgreSqlDialect),
        }
    }
}

/// SQL构建器
pub struct SqlBuilder {
    dialect: Box<dyn SqlDialect>,
}

impl SqlBuilder {
    /// 创建新的SQL构建器
    pub fn new(kind: &DataSourceKind) -> Self {
        Self {
            dialect: DialectFactory::create_dialect(kind),
        }
    }

    /// 构建INSERT语句
    pub fn build_insert(
        &self,
        schema: &str,
        table: &str,
        data: &Map<String, Value>,
    ) -> String {
        let fields: Vec<String> = data.keys().cloned().collect();
        let values: Vec<String> = data.values().map(|v| self.dialect.format_value(v)).collect();
        
        self.dialect.build_insert_sql(schema, table, &fields, &values)
    }

    /// 构建UPDATE语句
    pub fn build_update(
        &self,
        schema: &str,
        table: &str,
        data: &Map<String, Value>,
        id: i64,
    ) -> String {
        let set_clauses: Vec<String> = data
            .iter()
            .filter(|(k, _)| k.as_str() != "id")
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    self.dialect.escape_identifier(k),
                    self.dialect.format_value(v)
                )
            })
            .collect();

        let where_clause = format!("{}={}", self.dialect.escape_identifier("id"), id);
        
        self.dialect.build_update_sql(schema, table, &set_clauses, &where_clause)
    }

    /// 构建DELETE语句
    pub fn build_delete(
        &self,
        schema: &str,
        table: &str,
        id: i64,
    ) -> String {
        let where_clause = format!("{}={}", self.dialect.escape_identifier("id"), id);
        self.dialect.build_delete_sql(schema, table, &where_clause)
    }

    /// 构建带自定义WHERE子句的DELETE语句
    pub fn build_delete_with_where(
        &self,
        schema: &str,
        table: &str,
        where_clause: &str,
    ) -> String {
        self.dialect.build_delete_sql(schema, table, where_clause)
    }

    /// 构建SELECT字段列表
    pub fn build_columns(&self, column_spec: &str) -> Vec<String> {
        self.dialect.build_columns(column_spec)
    }

    /// 构建SELECT语句
    pub fn build_select(
        &self,
        schema: &str,
        table: &str,
        fields: Option<&[String]>,
        where_clause: Option<&str>,
        order_by: Option<&str>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let fields = fields.unwrap_or(&[]);
        self.dialect.build_select_sql(
            schema,
            table,
            fields,
            where_clause,
            order_by,
            limit,
            offset,
        )
    }

    /// 构建复杂WHERE条件
    pub fn build_where_conditions(
        &self,
        conditions: &Map<String, Value>,
    ) -> String {
        self.dialect.build_wheres(conditions)
    }

    /// 构建带复杂WHERE条件的SELECT语句
    pub fn build_select_with_conditions(
        &self,
        schema: &str,
        table: &str,
        fields: Option<&[String]>,
        conditions: &Map<String, Value>,
        order_by: Option<&str>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let where_clause = self.build_where_conditions(conditions);
        let where_clause = if where_clause == "1=1" { None } else { Some(where_clause.as_str()) };
        
        let fields = fields.unwrap_or(&[]);
        self.dialect.build_select_sql(
            schema,
            table,
            fields,
            where_clause,
            order_by,
            limit,
            offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_dialect() {
        let dialect = MySqlDialect;
        let sql = dialect.build_select_sql("public", "user", &vec!["id".to_string()], None, None, None, None);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_postgresql_dialect() {
        let dialect = PostgreSqlDialect;
        let sql = dialect.build_select_sql("public", "user", &vec!["id".to_string()], None, None, None, None);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_sql_builder() {
        let builder = SqlBuilder { dialect: DialectFactory::create_dialect(&DataSourceKind::Mysql) };
        let sql = builder.build_select("public", "user", None, None, None, Some(10), Some(0));
        assert!(sql.contains("SELECT"));
    }
}