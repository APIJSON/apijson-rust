use std::fmt::Debug;
use sqlx::ColumnIndex;
use crate::app::handler::util::string_util::{is_num, is_name};

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
        let mut fcs: Vec<String> = Vec::new();

        let _ = columns_str
            .split(';')
            .map(|cs| {
                let mut part = cs;
                let mut alias = "";
                let ind = part.rfind(':');
                if (ind.is_some()) {
                    alias = &part[(ind.unwrap() + 1)..];
                    part = &part[0..ind.unwrap()];
                }

                let start = part.find('(');
                let end = if start.is_none() { 0 } else { part.rfind(')').unwrap() };

                let fun = if end <= 0 { "" } else { &part[..start.unwrap()] };
                let arg_str = if end <= 0 { part } else { &part[(start.unwrap() + 1)..end] };
                let args = arg_str.split(',').map(|mut a_s| {
                    let mut als = "";
                    if fun.is_empty() {
                        let ind = a_s.rfind(':');
                        if (ind.is_some()) {
                            als = &a_s[(ind.unwrap() + 1)..];
                            a_s = &a_s[0..ind.unwrap()];
                        }
                    }

                    if als.is_empty() {
                        if is_num(a_s) {
                            return a_s.to_string();
                        }
                        if is_name(a_s) {
                            return format!("`{}`", a_s.replace("`", "\\`"));
                        }
                        return format!("'{}'", a_s.replace("'", "\\'"));
                    } else {
                        if is_num(a_s) {
                            return format!("{} AS `{}`", a_s, als.replace("`", "\\`"));
                        }
                        if is_name(a_s) {
                            return format!("`{}` AS `{}`", a_s.replace("`", "\\`"), als.replace("`", "\\`"));
                        }
                        return format!("'{}' AS `{}`", a_s.replace("'", "\\'"), als.replace("`", "\\`"));
                    }
                }).collect::<Vec<String>>();

                let a_s = args.join(",");
                let a_s2 = a_s.as_str();
                if fun.is_empty() {
                    if alias.is_empty() {
                        fcs.push(format!("{}", a_s2));
                    } else {
                        fcs.push(format!("{} AS `{}`", a_s2, alias.replace("`", "\\`")));
                    }
                } else {
                    if alias.is_empty() {
                        fcs.push(format!("{}({})", fun, a_s2));
                    } else {
                        fcs.push(format!("{}({}) AS `{}`", fun, a_s2, alias.replace("`", "\\`")));
                    }
                }
            }).count();

        return fcs
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