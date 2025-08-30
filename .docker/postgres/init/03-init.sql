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

-- 8) Mock data: users (10 rows)
INSERT INTO users (username, display_name, avatar_url) VALUES
('user01', 'User 01', 'https://example.com/avatars/u01.jpg'),
('user02', 'User 02', 'https://example.com/avatars/u02.jpg'),
('user03', 'User 03', 'https://example.com/avatars/u03.jpg'),
('user04', 'User 04', 'https://example.com/avatars/u04.jpg'),
('user05', 'User 05', 'https://example.com/avatars/u05.jpg'),
('user06', 'User 06', 'https://example.com/avatars/u06.jpg'),
('user07', 'User 07', 'https://example.com/avatars/u07.jpg'),
('user08', 'User 08', 'https://example.com/avatars/u08.jpg'),
('user09', 'User 09', 'https://example.com/avatars/u09.jpg'),
('user10', 'User 10', 'https://example.com/avatars/u10.jpg');

-- 9) Mock data: moments (10 rows, user_id 1..10)
INSERT INTO moments (user_id, content) VALUES
(1, 'Moment #1 by user 1'),
(2, 'Moment #2 by user 2'),
(3, 'Moment #3 by user 3'),
(4, 'Moment #4 by user 4'),
(5, 'Moment #5 by user 5'),
(6, 'Moment #6 by user 6'),
(7, 'Moment #7 by user 7'),
(8, 'Moment #8 by user 8'),
(9, 'Moment #9 by user 9'),
(10, 'Moment #10 by user 10');

-- 10) Mock data: comments (10 rows, comment i -> moment i, commenter user (i%10)+1)
INSERT INTO comments (moment_id, user_id, content) VALUES
(1, 2, 'Comment #1 on moment 1 by user 2'),
(2, 3, 'Comment #2 on moment 2 by user 3'),
(3, 4, 'Comment #3 on moment 3 by user 4'),
(4, 5, 'Comment #4 on moment 4 by user 5'),
(5, 6, 'Comment #5 on moment 5 by user 6'),
(6, 7, 'Comment #6 on moment 6 by user 7'),
(7, 8, 'Comment #7 on moment 7 by user 8'),
(8, 9, 'Comment #8 on moment 8 by user 9'),
(9, 10, 'Comment #9 on moment 9 by user 10'),
(10, 1, 'Comment #10 on moment 10 by user 1');