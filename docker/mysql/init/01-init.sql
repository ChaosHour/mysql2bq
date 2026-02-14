-- Initialize database for mysql2bq testing
USE app;

-- Create users table matching the config
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO users (email, name) VALUES
('alice@example.com', 'Alice Johnson'),
('bob@example.com', 'Bob Smith'),
('charlie@example.com', 'Charlie Brown');

-- Create a trigger to update updated_at (for testing updates)
DELIMITER ;;
CREATE TRIGGER users_update_trigger BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END;;
DELIMITER ;

-- Grant replication permissions to the replica user
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replica'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replica'@'%';
FLUSH PRIVILEGES;