-- Create database if not exists
CREATE DATABASE IF NOT EXISTS panda_db_1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user and grant privileges
CREATE USER IF NOT EXISTS 'panda_user'@'%' IDENTIFIED BY 'panda123';
GRANT ALL PRIVILEGES ON panda_db_1.* TO 'panda_user'@'%';
FLUSH PRIVILEGES;

-- Create some example tables
USE panda_db_1;

CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_profiles (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    avatar_url VARCHAR(255),
    bio TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert mock data for users table
INSERT INTO users (username, email, password_hash) VALUES
('kevin_lee', 'kevin.lee@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('lisa_wang', 'lisa.wang@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('mike_chen', 'mike.chen@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('nancy_kim', 'nancy.kim@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('oscar_liu', 'oscar.liu@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('penny_zhang', 'penny.zhang@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('quinn_wu', 'quinn.wu@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('rachel_yang', 'rachel.yang@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('steve_park', 'steve.park@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('tina_zhou', 'tina.zhou@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e');

-- Insert mock data for user_profiles table
INSERT INTO user_profiles (user_id, first_name, last_name, avatar_url, bio) VALUES
(1, 'Kevin', 'Lee', 'https://example.com/avatars/kevin.jpg', 'Security engineer specializing in cybersecurity and penetration testing.'),
(2, 'Lisa', 'Wang', 'https://example.com/avatars/lisa.jpg', 'UI/UX designer with a focus on user-centered design principles.'),
(3, 'Mike', 'Chen', 'https://example.com/avatars/mike.jpg', 'Database administrator with expertise in MySQL and PostgreSQL.'),
(4, 'Nancy', 'Kim', 'https://example.com/avatars/nancy.jpg', 'Project manager coordinating agile development teams.'),
(5, 'Oscar', 'Liu', 'https://example.com/avatars/oscar.jpg', 'Cloud architect designing scalable infrastructure solutions.'),
(6, 'Penny', 'Zhang', 'https://example.com/avatars/penny.jpg', 'Business analyst bridging the gap between business and technology.'),
(7, 'Quinn', 'Wu', 'https://example.com/avatars/quinn.jpg', 'Game developer creating immersive gaming experiences.'),
(8, 'Rachel', 'Yang', 'https://example.com/avatars/rachel.jpg', 'Marketing technologist leveraging data for growth strategies.'),
(9, 'Steve', 'Park', 'https://example.com/avatars/steve.jpg', 'Systems administrator maintaining enterprise IT infrastructure.'),
(10, 'Tina', 'Zhou', 'https://example.com/avatars/tina.jpg', 'Research scientist exploring artificial intelligence applications.');}]}}}