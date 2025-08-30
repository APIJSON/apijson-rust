-- PostgreSQL 16 初始化脚本（数据库/角色/表：users, moments, comments）

-- 1) 创建数据库（若不存在）
SELECT 'CREATE DATABASE panda_db_3 WITH ENCODING ''UTF8''' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'panda_db_3')\gexec

-- 2) 创建角色（若不存在）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'panda_user'
    ) THEN
        CREATE ROLE panda_user LOGIN PASSWORD 'panda123';
    END IF;
END$$;

-- 3) 授权数据库给角色
GRANT ALL PRIVILEGES ON DATABASE panda_db_3 TO panda_user;

-- 4) 切换到目标数据库
\connect panda_db_3

-- 5) 表结构：users
CREATE TABLE IF NOT EXISTS users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(50),
    avatar_url VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6) 表结构：moments（朋友圈动态）
CREATE TABLE IF NOT EXISTS moments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_moments_user_id ON moments(user_id);

-- 7) 表结构：comments（动态评论）
CREATE TABLE IF NOT EXISTS comments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    moment_id BIGINT NOT NULL REFERENCES moments(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comments_moment_id ON comments(moment_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);