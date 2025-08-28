-- Create database and user
CREATE DATABASE panda_db;
CREATE USER panda_user WITH ENCRYPTED PASSWORD 'panda123';
GRANT ALL PRIVILEGES ON DATABASE panda_db TO panda_user;

-- Connect to the new database
\c panda_db;

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO panda_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO panda_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO panda_user;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Create some example tables
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

CREATE TABLE IF NOT EXISTS user_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    avatar_url VARCHAR(255),
    bio TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_user_id ON user_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_profiles_metadata ON user_profiles USING GIN(metadata);

-- Create a function to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_profiles_updated_at BEFORE UPDATE ON user_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions on new objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO panda_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO panda_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO panda_user;

-- Insert mock data for users table
INSERT INTO users (username, email, password_hash) VALUES
('alex_morgan', 'alex.morgan@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('bella_garcia', 'bella.garcia@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('carlos_rodriguez', 'carlos.rodriguez@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('diana_martinez', 'diana.martinez@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('ethan_lopez', 'ethan.lopez@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('faith_wilson', 'faith.wilson@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('gabriel_anderson', 'gabriel.anderson@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('hannah_taylor', 'hannah.taylor@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('isaac_thomas', 'isaac.thomas@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e'),
('jade_jackson', 'jade.jackson@example.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.PJ/..e');

-- Insert mock data for user_profiles table
INSERT INTO user_profiles (user_id, first_name, last_name, avatar_url, bio, metadata) VALUES
((SELECT id FROM users WHERE username = 'alex_morgan'), 'Alex', 'Morgan', 'https://example.com/avatars/alex.jpg', 'Blockchain developer exploring decentralized applications.', '{"skills": ["Solidity", "Web3", "Smart Contracts"], "experience": 3}'),
((SELECT id FROM users WHERE username = 'bella_garcia'), 'Bella', 'Garcia', 'https://example.com/avatars/bella.jpg', 'Machine learning engineer working on computer vision projects.', '{"skills": ["Python", "TensorFlow", "OpenCV"], "experience": 4}'),
((SELECT id FROM users WHERE username = 'carlos_rodriguez'), 'Carlos', 'Rodriguez', 'https://example.com/avatars/carlos.jpg', 'Site reliability engineer ensuring system uptime and performance.', '{"skills": ["Kubernetes", "Prometheus", "Grafana"], "experience": 5}'),
((SELECT id FROM users WHERE username = 'diana_martinez'), 'Diana', 'Martinez', 'https://example.com/avatars/diana.jpg', 'Product designer creating intuitive user experiences.', '{"skills": ["Figma", "Sketch", "Prototyping"], "experience": 6}'),
((SELECT id FROM users WHERE username = 'ethan_lopez'), 'Ethan', 'Lopez', 'https://example.com/avatars/ethan.jpg', 'Cybersecurity analyst protecting digital assets and infrastructure.', '{"skills": ["Penetration Testing", "SIEM", "Incident Response"], "experience": 7}'),
((SELECT id FROM users WHERE username = 'faith_wilson'), 'Faith', 'Wilson', 'https://example.com/avatars/faith.jpg', 'Technical lead coordinating cross-functional development teams.', '{"skills": ["Leadership", "Architecture", "Mentoring"], "experience": 8}'),
((SELECT id FROM users WHERE username = 'gabriel_anderson'), 'Gabriel', 'Anderson', 'https://example.com/avatars/gabriel.jpg', 'Data engineer building robust data pipelines and analytics platforms.', '{"skills": ["Apache Spark", "Kafka", "Airflow"], "experience": 4}'),
((SELECT id FROM users WHERE username = 'hannah_taylor'), 'Hannah', 'Taylor', 'https://example.com/avatars/hannah.jpg', 'Growth hacker optimizing user acquisition and retention strategies.', '{"skills": ["A/B Testing", "Analytics", "SEO"], "experience": 3}'),
((SELECT id FROM users WHERE username = 'isaac_thomas'), 'Isaac', 'Thomas', 'https://example.com/avatars/isaac.jpg', 'Platform engineer building developer tools and infrastructure.', '{"skills": ["Docker", "CI/CD", "Terraform"], "experience": 5}'),
((SELECT id FROM users WHERE username = 'jade_jackson'), 'Jade', 'Jackson', 'https://example.com/avatars/jade.jpg', 'Content strategist creating engaging technical documentation.', '{"skills": ["Technical Writing", "Content Strategy", "Documentation"], "experience": 4}');}]}}}