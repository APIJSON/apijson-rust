-- Create database if not exists
CREATE DATABASE IF NOT EXISTS panda_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user and grant privileges
CREATE USER IF NOT EXISTS 'panda_user'@'%' IDENTIFIED BY 'panda123';
GRANT ALL PRIVILEGES ON panda_db.* TO 'panda_user'@'%';
FLUSH PRIVILEGES;

-- Create some example tables
USE panda_db;

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
('alice_smith', 'alice.smith@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('bob_johnson', 'bob.johnson@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('charlie_brown', 'charlie.brown@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('diana_prince', 'diana.prince@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('edward_norton', 'edward.norton@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('fiona_green', 'fiona.green@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('george_white', 'george.white@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('helen_black', 'helen.black@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('ivan_gray', 'ivan.gray@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('julia_red', 'julia.red@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e');

-- Insert mock data for user_profiles table
INSERT INTO user_profiles (user_id, first_name, last_name, avatar_url, bio) VALUES
(1, 'Alice', 'Smith', 'https://example.com/avatars/alice.jpg', 'Software engineer passionate about web development and open source.'),
(2, 'Bob', 'Johnson', 'https://example.com/avatars/bob.jpg', 'Full-stack developer with expertise in React and Node.js.'),
(3, 'Charlie', 'Brown', 'https://example.com/avatars/charlie.jpg', 'DevOps engineer focused on cloud infrastructure and automation.'),
(4, 'Diana', 'Prince', 'https://example.com/avatars/diana.jpg', 'Product manager with a background in UX design.'),
(5, 'Edward', 'Norton', 'https://example.com/avatars/edward.jpg', 'Data scientist specializing in machine learning and AI.'),
(6, 'Fiona', 'Green', 'https://example.com/avatars/fiona.jpg', 'Frontend developer with a passion for creating beautiful user interfaces.'),
(7, 'George', 'White', 'https://example.com/avatars/george.jpg', 'Backend developer experienced in microservices architecture.'),
(8, 'Helen', 'Black', 'https://example.com/avatars/helen.jpg', 'QA engineer ensuring software quality and reliability.'),
(9, 'Ivan', 'Gray', 'https://example.com/avatars/ivan.jpg', 'Mobile app developer for iOS and Android platforms.'),
(10, 'Julia', 'Red', 'https://example.com/avatars/julia.jpg', 'Technical writer and documentation specialist.');}]}}}