CREATE TABLE {schema}.{table} (
id SERIAL NOT NULL,
report_date DATE,
nb_investors INTEGER,
volume INTEGER,
total_value DOUBLE PRECISION,
CONSTRAINT pk_bt_euro_classic PRIMARY KEY (id),
CONSTRAINT unique_date UNIQUE (report_date)
);