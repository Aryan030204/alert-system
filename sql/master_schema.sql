CREATE TABLE brands (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    db_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO brands (name, db_name) VALUES
('PTS', 'PTS'),
('BBB', 'BBB'),
('TMC', 'TMC'),
('MILA', 'MILA');

CREATE TABLE alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255),
    metric_type ENUM('base','derived') NOT NULL,
    formula TEXT,
    threshold_type ENUM('absolute','percentage_drop','percentage_rise') NOT NULL,
    threshold_value DOUBLE NOT NULL,
    severity ENUM('low','medium','high') DEFAULT 'low',
    cooldown_minutes INT DEFAULT 30,
    is_active TINYINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands(id),
    INDEX idx_brand_id (brand_id)
);

CREATE TABLE alert_conditions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_id INT NOT NULL,
    condition_group INT DEFAULT 1,
    metric_name VARCHAR(255) NOT NULL,
    operator ENUM('<','>','<=','>=','=','!=') NOT NULL,
    compare_value DOUBLE NOT NULL,
    logic ENUM('AND','OR') DEFAULT 'AND',
    FOREIGN KEY (alert_id) REFERENCES alerts(id) ON DELETE CASCADE
);

CREATE TABLE alert_channels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_id INT NOT NULL,
    channel_type ENUM('slack','email','webhook') NOT NULL,
    channel_config JSON NOT NULL,
    FOREIGN KEY (alert_id) REFERENCES alerts(id) ON DELETE CASCADE
);

CREATE TABLE alert_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_id INT NOT NULL,
    brand_id INT NOT NULL,
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSON,
    FOREIGN KEY (alert_id) REFERENCES alerts(id) ON DELETE CASCADE,
    FOREIGN KEY (brand_id) REFERENCES brands(id),
    INDEX idx_brand (brand_id)
);
