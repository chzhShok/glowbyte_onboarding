CREATE TABLE IF NOT EXISTS random_data (
    id NUMERIC(3) NOT NULL,
    name VARCHAR(15) NOT NULL,
    date DATE NOT NULL,
    UNIQUE (date)
);
