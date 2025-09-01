use fnv::FnvHashMap;
use sqlx::{
    Column, Row, TypeInfo,
    postgres::{PgColumn, PgPool, PgRow},
    types::Decimal,
};
use indexmap::IndexMap;
use crate::app::datasource::metadata::{put_db_meta, put_db_tables, put_table_meta};
use crate::app::datasource::codec::base64_encode;
use crate::app::datasource::{ColumnMeta, TableMeta};

// PostgreSQL系统数据库列表
const PG_SYS_DB: &[&str] = &["information_schema", "pg_catalog", "pg_toast", "postgres", "template0", "template1"];

/// PostgreSQL数据库连接结构体
///
/// 用于管理 PostgreSQL 数据库连接池，提供数据库操作的基础功能。
///
/// # 字段
/// * `pool` - PostgreSQL 连接池，用于执行数据库操作
#[derive(Debug, Clone)]
pub struct PgConn {
    /// PostgreSQL 连接池
    pool: PgPool,
}

impl PgConn {
    /// 创建一个新的PostgreSQL数据库连接实例
    ///
    /// 该方法会建立 PostgreSQL 连接池，并初始化数据库元数据信息。
    ///
    /// # 参数
    /// * `url` - PostgreSQL 数据库连接 URL
    ///
    /// # 返回值
    /// * `Ok(Self)` - 成功创建的数据库连接实例
    /// * `Err(sqlx::Error)` - 连接或初始化过程中发生的错误
    pub async fn new(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(url).await?;
        let mut ds = Self { pool };
        ds.init().await?;
        Ok(ds)
    }

    /// 初始化数据库连接
    ///
    /// 加载所有用户数据库及其表结构信息到内存缓存中。
    ///
    /// # 返回值
    /// * `Ok(())` - 初始化成功
    /// * `Err(sqlx::Error)` - 初始化过程中发生的错误
    async fn init(&mut self) -> Result<(), sqlx::Error> {
        let db_names = self.load_db().await?;
        for db_name in db_names {
            self.load_db_table(&db_name).await?;
        }
        Ok(())
    }

    /// 加载数据库列表
    ///
    /// 从 PostgreSQL 的 information_schema 中查询所有用户数据库的名称和大小，
    /// 过滤掉系统数据库，并将数据库信息写入全局缓存。
    ///
    /// # 返回值
    /// * `Ok(Vec<String>)` - 成功加载的数据库名称列表
    /// * `Err(sqlx::Error)` - 查询或处理过程中发生的错误
    async fn load_db(&mut self) -> Result<Vec<String>, sqlx::Error> {
        // 查询所有数据库的名称和大小（单位：MB）
        let list_db_sql = "SELECT datname AS name, 
                          ROUND(pg_database_size(datname) / 1024.0 / 1024.0, 2) AS size
                          FROM pg_database WHERE datistemplate = false;";

        let db_list = sqlx::query(list_db_sql).fetch_all(&self.pool).await?;
        let mut db_names = Vec::with_capacity(db_list.len());

        for db_row in db_list.iter() {
            // 获取数据库名称
            let db_name: String = db_row.try_get("name")?;

            // 跳过 PostgreSQL 系统数据库
            if PG_SYS_DB.contains(&db_name.as_str()) {
                continue;
            }

            // 获取数据库大小并转换为 f64 类型
            let db_size: Decimal = db_row.try_get("size")?;
            let db_size = db_size.to_string().parse::<f64>().unwrap_or(0.0);

            // 使用外部缓存替代内部静态变量
            put_db_meta(db_name.clone(), db_size);

            // 将数据库名称添加到结果列表中
            db_names.push(db_name);
        }

        Ok(db_names)
    }
    
    /// 加载指定数据库中的所有表信息
    ///
    /// 该方法会查询指定数据库中的所有基础表（BASE TABLE），获取表名和表注释，
    /// 然后加载每个表的列元数据信息，并将这些信息存储到外部缓存中。
    ///
    /// # 参数
    /// * `schema` - 数据库名称
    ///
    /// # 返回值
    /// * `Ok(())` - 成功加载所有表信息
    /// * `Err(sqlx::Error)` - 查询或处理过程中发生的错误
    async fn load_db_table(&mut self, schema: &str) -> Result<(), sqlx::Error> {
        // 构造查询表信息的 SQL 语句，只查询基础表（BASE TABLE）
        let list_db_table_sql = format!(
            "SELECT t.table_name, 
                    COALESCE(obj_description(c.oid), '') as table_comment
             FROM information_schema.tables t
             LEFT JOIN pg_class c ON c.relname = t.table_name
             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE t.table_catalog = '{}' 
               AND t.table_schema = 'public' 
               AND t.table_type = 'BASE TABLE'",
            schema
        );

        // 执行查询，获取所有表的信息
        let tables = sqlx::query(&list_db_table_sql)
            .fetch_all(&self.pool)
            .await?;
        
        // 预分配容量的表名列表，用于存储当前数据库中的所有表名
        let mut table_name_list = Vec::with_capacity(tables.len());

        // 遍历查询结果，构建每个表的元数据信息
        for table_row in tables {
            // 获取表名
            let table_name: String = table_row.try_get("table_name")?;
            
            // 获取表注释
            let table_comment: String = table_row.try_get("table_comment").unwrap_or_default();

            // 加载表的列元数据信息
            let columns = self.load_table_meta(schema, &table_name).await?;
            
            // 构建表元数据结构体
            let table_meta = TableMeta {
                schema: schema.to_string(),
                name: table_name.clone(),
                columns,
                comment: Some(table_comment),
            };

            // 记录表加载日志
            log::info!("postgres.table: {}.{} loaded", schema, &table_name);

            // 将表元数据存入外部缓存
            put_table_meta(schema, &table_name, table_meta);
            
            // 将表名添加到表名列表中
            table_name_list.push(table_name);
        }

        // 将当前数据库的表列表存入外部缓存
        put_db_tables(schema.to_string(), table_name_list);

        Ok(())
    }

    /// 加载表的列元数据信息
    ///
    /// 通过查询 information_schema.columns 获取指定表的所有列信息，
    /// 并将结果转换为 `ColumnMeta` 结构体列表，最终构建成以列名为键的哈希映射。
    ///
    /// # 参数
    /// * `schema` - 数据库名称
    /// * `table_name` - 表名称
    ///
    /// # 返回值
    /// * `Ok(FnvHashMap<String, ColumnMeta>)` - 成功加载的列元数据映射
    /// * `Err(sqlx::Error)` - 查询或处理过程中发生的错误
    async fn load_table_meta(&self, schema: &str, table_name: &str) -> Result<FnvHashMap<String, ColumnMeta>, sqlx::Error> {
        // 构造查询表列信息的 SQL 语句
        let sql = format!(
            "SELECT 
                c.column_name as field,
                c.data_type as type_name,
                c.is_nullable as null_flag,
                c.column_default as default_value,
                COALESCE(col_description(pgc.oid, c.ordinal_position), '') as comment,
                CASE 
                    WHEN pk.column_name IS NOT NULL THEN 'PRI'
                    WHEN uk.column_name IS NOT NULL THEN 'UNI'
                    ELSE ''
                END as key_type,
                '' as extra
             FROM information_schema.columns c
             LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
             LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace
             LEFT JOIN (
                 SELECT ku.column_name
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                 WHERE tc.table_catalog = '{}' AND tc.table_name = '{}' AND tc.constraint_type = 'PRIMARY KEY'
             ) pk ON pk.column_name = c.column_name
             LEFT JOIN (
                 SELECT ku.column_name
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                 WHERE tc.table_catalog = '{}' AND tc.table_name = '{}' AND tc.constraint_type = 'UNIQUE'
             ) uk ON uk.column_name = c.column_name
             WHERE c.table_catalog = '{}' AND c.table_name = '{}'
             ORDER BY c.ordinal_position",
            schema, table_name, schema, table_name, schema, table_name
        );
        
        // 执行查询并获取结果
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        
        // 创建一个预分配容量的哈希映射，用于存储列元数据
        let mut column_map = FnvHashMap::with_capacity_and_hasher(rows.len(), Default::default());
        
        // 遍历列信息，构建 ColumnMeta 并插入到哈希映射中
        for row in rows {
            let field: String = row.try_get("field")?;
            let type_name: String = row.try_get("type_name")?;
            let null_flag: String = row.try_get("null_flag")?;
            let default_value: Option<String> = row.try_get("default_value").ok();
            let comment: String = row.try_get("comment").unwrap_or_default();
            let key_type: String = row.try_get("key_type")?;
            let extra: String = row.try_get("extra")?;
            
            let column_meta = ColumnMeta {
                field: field.clone(),
                type_name,
                null: Some(null_flag),
                default: default_value,
                comment: Some(comment),
                key: Some(key_type),
                extra: Some(extra),
            };
            
            column_map.insert(field, column_meta);
        }
        
        Ok(column_map)
    }

    /// 查询单条记录
    ///
    /// 执行给定的 SQL 查询语句，返回第一条匹配的记录。
    /// 如果 SQL 语句中未包含 LIMIT 子句，会自动添加 `LIMIT 1` 以提高查询效率。
    ///
    /// # 参数
    /// * `sql` - 要执行的 SQL 查询语句
    /// * `params` - 查询参数列表，用于绑定到 SQL 语句中的占位符
    ///
    /// # 返回值
    /// * `Ok(Some(IndexMap<String, serde_json::Value>))` - 成功查询到记录，返回包含字段名和值的映射
    /// * `Ok(None)` - 没有查询到记录
    /// * `Err(sqlx::Error)` - 查询过程中发生错误
    pub async fn query_one(&self, sql: &str, params: Vec<String>) -> Result<Option<IndexMap<String, serde_json::Value>>, sqlx::Error> {
        // 如果 SQL 语句中没有包含 LIMIT，则自动添加 LIMIT 1 以提高查询效率
        let sql = if !sql.to_lowercase().contains("limit") {
            format!("{} LIMIT 1", sql)
        } else {
            sql.to_string()
        };

        // 构建查询并绑定参数
        let mut query = sqlx::query(&sql);
        for param in params {
            query = query.bind(param);
        }

        // 执行查询并获取结果
        let row_opt = query.fetch_optional(&self.pool).await?;

        // 如果查询到记录，则将行数据转换为 IndexMap
        match row_opt {
            Some(row) => {
                let columns = row.columns();
                let mut record = IndexMap::with_capacity(columns.len());
                
                // 遍历所有列，将列名和对应的值插入到 IndexMap 中
                for column in columns {
                    let value = Self::get_column_val(&row, column);
                    record.insert(column.name().to_string(), value);
                }
                
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// 查询多条记录
    ///
    /// 执行给定的 SQL 查询语句，返回所有匹配的记录列表。
    ///
    /// # 参数
    /// * `sql` - 要执行的 SQL 查询语句
    /// * `params` - 查询参数列表，用于绑定到 SQL 语句中的占位符
    ///
    /// # 返回值
    /// * `Ok(Vec<IndexMap<String, serde_json::Value>>)` - 成功查询到的记录列表，每条记录为字段名和值的映射
    /// * `Err(sqlx::Error)` - 查询过程中发生错误
    pub async fn query_list(&self, sql: &str, params: Vec<String>) -> Result<Vec<IndexMap<String, serde_json::Value>>, sqlx::Error> {
        // 构建查询并绑定参数
        let mut query = sqlx::query(sql);
        for param in params {
            query = query.bind(param);
        }

        // 执行查询并获取所有结果行
        let rows = query.fetch_all(&self.pool).await?;

        // 预分配容量的结果向量
        let mut results = Vec::with_capacity(rows.len());

        // 遍历每一行，将行数据转换为 IndexMap 并添加到结果中
        for row in rows.into_iter() {
            let columns = row.columns();
            let mut record = IndexMap::with_capacity(columns.len());
            
            // 遍历所有列，将列名和对应的值插入到 IndexMap 中
            for column in columns {
                let value = Self::get_column_val(&row, column);
                record.insert(column.name().to_string(), value);
            }
            
            results.push(record);
        }

        Ok(results)
    }

    /// 从数据库行中获取指定列的值并转换为 JSON 值
    ///
    /// 根据列的数据类型，将数据库中的值转换为相应的 JSON 值类型。
    /// 支持多种 PostgreSQL 数据类型的转换。
    ///
    /// # 参数
    /// * `row` - 数据库查询结果行的引用
    /// * `column` - 列信息的引用
    ///
    /// # 返回值
    /// 返回转换后的 JSON 值
    fn get_column_val(row: &PgRow, column: &PgColumn) -> serde_json::Value {
        let column_name = column.name();
        let type_name = column.type_info().name();
        
        match type_name {
            // 整数类型
            "INT2" | "SMALLINT" => {
                row.try_get::<i16, _>(column_name)
                    .map_or(serde_json::Value::Null, |val| serde_json::Value::Number(val.into()))
            }
            "INT4" | "INTEGER" => {
                row.try_get::<i32, _>(column_name)
                    .map_or(serde_json::Value::Null, |val| serde_json::Value::Number(val.into()))
            }
            "INT8" | "BIGINT" => {
                row.try_get::<i64, _>(column_name)
                    .map_or(serde_json::Value::Null, |val| serde_json::Value::Number(val.into()))
            }
            // 浮点数类型
            "FLOAT4" | "REAL" => {
                row.try_get::<f32, _>(column_name)
                    .map_or(serde_json::Value::Null, |val| {
                        serde_json::Number::from_f64(val as f64)
                            .map_or(serde_json::Value::Null, serde_json::Value::Number)
                    })
            }
            "FLOAT8" | "DOUBLE PRECISION" => {
                row.try_get::<f64, _>(column_name)
                    .map_or(serde_json::Value::Null, |val| {
                        serde_json::Number::from_f64(val)
                            .map_or(serde_json::Value::Null, serde_json::Value::Number)
                    })
            }
            // 布尔类型
            "BOOL" | "BOOLEAN" => {
                row.try_get::<bool, _>(column_name)
                    .map_or(serde_json::Value::Null, serde_json::Value::Bool)
            }
            // 字符串类型
            "VARCHAR" | "CHAR" | "TEXT" | "NAME" => {
                row.try_get::<String, _>(column_name)
                    .map_or(serde_json::Value::Null, serde_json::Value::String)
            }
            // JSON 类型
            "JSON" | "JSONB" => {
                row.try_get::<serde_json::Value, _>(column_name)
                    .unwrap_or(serde_json::Value::Null)
            }
            // 二进制类型
            "BYTEA" => {
                row.try_get::<Vec<u8>, _>(column_name)
                    .map_or(serde_json::Value::Null, |bytes| {
                        // 尝试转换为 UTF-8 字符串，失败则进行 Base64 编码
                        match String::from_utf8(bytes.clone()) {
                            Ok(s) => serde_json::Value::String(s),
                            Err(_) => serde_json::Value::String(base64_encode(bytes)),
                        }
                    })
            }
            // 数值类型
            "NUMERIC" | "DECIMAL" => {
                row.try_get::<Decimal, _>(column_name)
                    .map(|decimal| serde_json::Value::String(decimal.to_string()))
                    .unwrap_or_else(|err| {
                        log::error!(
                            "Failed to get DECIMAL value for column '{}': {}",
                            column_name, err
                        );
                        serde_json::Value::Null
                    })
            }
            // 日期时间类型
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                row.try_get::<chrono::NaiveDateTime, _>(column_name)
                    .map_or(serde_json::Value::Null, |dt| {
                        serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    })
            }
            "DATE" => {
                row.try_get::<chrono::NaiveDate, _>(column_name)
                    .map_or(serde_json::Value::Null, |date| {
                        serde_json::Value::String(date.format("%Y-%m-%d").to_string())
                    })
            }
            "TIME" => {
                row.try_get::<chrono::NaiveTime, _>(column_name)
                    .map_or(serde_json::Value::Null, |time| {
                        serde_json::Value::String(time.format("%H:%M:%S").to_string())
                    })
            }
            // UUID 类型
            "UUID" => {
                row.try_get::<uuid::Uuid, _>(column_name)
                    .map_or(serde_json::Value::Null, |uuid| {
                        serde_json::Value::String(uuid.to_string())
                    })
            }
            // 默认情况：尝试作为字符串处理
            _ => {
                log::warn!("Unknown PostgreSQL type: {}, treating as string", type_name);
                row.try_get::<String, _>(column_name)
                    .map_or(serde_json::Value::Null, serde_json::Value::String)
            }
        }
    }

    /// 更新数据
    ///
    /// 执行给定的 UPDATE SQL 语句，返回受影响的行数。
    ///
    /// # 参数
    /// * `sql` - 要执行的 UPDATE SQL 语句
    ///
    /// # 返回值
    /// * `Ok(u64)` - 成功更新数据，返回受影响的行数
    /// * `Err(sqlx::Error)` - 更新过程中发生错误
    pub async fn update(&self, sql: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// 查询记录数
    ///
    /// 执行给定的 COUNT SQL 语句，返回查询到的记录数。
    ///
    /// # 参数
    /// * `sql` - 要执行的 COUNT SQL 语句
    /// * `params` - 查询参数列表，用于绑定到 SQL 语句中的占位符
    ///
    /// # 返回值
    /// * `Ok(i64)` - 成功查询到记录数
    /// * `Err(sqlx::Error)` - 查询过程中发生错误
    pub async fn count(&self, sql: &str, params: Vec<String>) -> Result<i64, sqlx::Error> {
        let mut query_scalar = sqlx::query_scalar::<_, i64>(sql);
        for param in params {
            query_scalar = query_scalar.bind(param);
        }
        let count = query_scalar.fetch_one(&self.pool).await?;
        Ok(count)
    }

    /// 创建表
    ///
    /// 执行给定的 CREATE TABLE SQL 语句。
    ///
    /// # 参数
    /// * `sql` - 要执行的 CREATE TABLE SQL 语句
    ///
    /// # 返回值
    /// * `Ok(())` - 成功创建表
    /// * `Err(sqlx::Error)` - 创建表过程中发生错误
    pub async fn create_table(&self, sql: &str) -> Result<(), sqlx::Error> {
        sqlx::query(sql).execute(&self.pool).await?;
        Ok(())
    }
}