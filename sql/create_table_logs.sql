CREATE SCHEMA IF NOT EXISTS logs;
CREATE SEQUENCE IF NOT EXISTS logs.seq_lg_messages INCREMENT 1 START 1;


CREATE TABLE IF NOT EXISTS logs.load_csv_to_ds (
    ID SERIAL PRIMARY KEY,
    TABLE_NAME VARCHAR(30),
    CSV_PATH TEXT,
    TIME_START_LOAD TIMESTAMP DEFAULT CURRENT_TIMESTAMP(0),
    TIME_END_LOAD TIMESTAMP,
    condition TEXT
);


CREATE TABLE IF NOT EXISTS logs.lg_messages_dm ( 	
    record_id INT,
    date_time TIMESTAMP,
    pid TEXT,
    message TEXT,
    message_type TEXT,
    usename TEXT, 
    datname TEXT, 
    client_addr TEXT, 
    application_name TEXT,
    backend_start TEXT
);
