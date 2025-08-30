-- Create database if not exists
CREATE DATABASE IF NOT EXISTS panda_db_1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user and grant privileges
CREATE USER IF NOT EXISTS 'panda_user'@'%' IDENTIFIED BY 'panda123';
GRANT ALL PRIVILEGES ON panda_db_1.* TO 'panda_user'@'%';
FLUSH PRIVILEGES;

-- Use database
USE panda_db_1;

-- Users: 基础用户信息，简洁字段
CREATE TABLE IF NOT EXISTS users (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(50),
    avatar_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Moments: 朋友圈动态，作者+内容+时间
CREATE TABLE IF NOT EXISTS moments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_moments_user_id (user_id),
    CONSTRAINT fk_moments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Comments: 动态评论，属于某条动态，评论者+内容+时间
CREATE TABLE IF NOT EXISTS comments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    moment_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_comments_moment_id (moment_id),
    INDEX idx_comments_user_id (user_id),
    CONSTRAINT fk_comments_moment FOREIGN KEY (moment_id) REFERENCES moments(id) ON DELETE CASCADE,
    CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Mock data: users (10 rows)
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

-- Mock data: moments (10 rows, user_id 1..10)
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

-- Mock data: comments (10 rows, comment i -> moment i, commenter user (i%10)+1)
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