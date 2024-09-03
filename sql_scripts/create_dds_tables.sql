DROP TABLE IF EXISTS dds.logs CASCADE;
DROP TABLE IF EXISTS dds.fct_works CASCADE;
DROP TABLE IF EXISTS dds.dm_topics CASCADE;
DROP TABLE IF EXISTS dds.dm_dates CASCADE;
DROP TABLE IF EXISTS dds.dm_employees CASCADE;
DROP TABLE IF EXISTS dds.dm_works_info CASCADE;
DROP TABLE IF EXISTS dds.dm_standard_points CASCADE;
DROP TABLE IF EXISTS dds.load_info CASCADE;

DROP FUNCTION IF EXISTS dds.create_triggers;
DROP FUNCTION IF EXISTS dds.insert_function;
DROP FUNCTION IF EXISTS dds.update_function;
DROP FUNCTION IF EXISTS dds.delete_function;
DROP FUNCTION IF EXISTS dds.truncate_function;

------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dds.insert_function() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO dds.logs(table_name, action, action_ts, data)
	VALUES(TG_TABLE_NAME, TG_OP, current_timestamp, row_to_json(NEW));
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dds.update_function() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO dds.logs(table_name, action, action_ts, data)
	VALUES(TG_TABLE_NAME, TG_OP, current_timestamp, jsonb_build_object('NEW', row_to_json(NEW), 'OLD', row_to_json(OLD)));
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dds.delete_function() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO dds.logs(table_name, action, action_ts, data)
	VALUES(TG_TABLE_NAME, TG_OP, current_timestamp, row_to_json(OLD));
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dds.truncate_function() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO dds.logs(table_name, action, action_ts, data)
	VALUES(TG_TABLE_NAME, TG_OP, current_timestamp, NULL);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dds.create_triggers(table_name text)
RETURNS void AS $$
BEGIN 
	EXECUTE format(
		'CREATE TRIGGER %I_insert_trigger
		BEFORE INSERT ON dds.%I
		FOR EACH ROW
		EXECUTE PROCEDURE dds.insert_function();

		CREATE TRIGGER %I_update_trigger
		BEFORE UPDATE ON dds.%I
		FOR EACH ROW
		EXECUTE PROCEDURE dds.update_function();

		CREATE TRIGGER %I_delete_trigger
		AFTER DELETE ON dds.%I
		FOR EACH ROW
		EXECUTE PROCEDURE dds.delete_function();

		CREATE TRIGGER %I_truncate_trigger
		AFTER TRUNCATE ON dds.%I
		FOR EACH STATEMENT
		EXECUTE PROCEDURE dds.truncate_function()',
		table_name, table_name, table_name, table_name,
		table_name, table_name, table_name, table_name
	);
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------

CREATE TABLE dds.logs(
	id SERIAL PRIMARY KEY,
	action VARCHAR(8) NOT NULL,
	table_name VARCHAR(50) NOT NULL,
	action_ts TIMESTAMP NOT NULL,
	data JSON
);

------------------------------------------------------------------------------------

CREATE TABLE dds.dm_topics (
	id SERIAL PRIMARY KEY,
	topic_name VARCHAR(50) UNIQUE NOT NULL
);

SELECT dds.create_triggers('dm_topics');

------------------------------------------------------------------------------------

CREATE TABLE dds.dm_dates(
	id SERIAL PRIMARY KEY,
	date DATE NOT NULL UNIQUE, 
	year SMALLINT CHECK(year > 2000) NOT NULL,
	month SMALLINT CHECK(month >= 1 AND month <= 12) NOT NULL,
	day SMALLINT CHECK(day >= 1 AND day <= 31) NOT NULL
);

SELECT dds.create_triggers('dm_dates');

------------------------------------------------------------------------------------

CREATE TABLE dds.dm_employees(
	id SERIAL PRIMARY KEY,
	service_number VARCHAR(10) NOT NULL,
	name VARCHAR(50) NOT NULL,
	position VARCHAR(50) NOT NULL,
	active_from DATE CHECK(active_from >= '2000-01-01') NOT NULL,
	active_to DATE
);

SELECT dds.create_triggers('dm_employees');

------------------------------------------------------------------------------------

CREATE TABLE dds.dm_standard_points(
	id SERIAL PRIMARY KEY,
	standard_id VARCHAR(20) NOT NULL,
	ratio FLOAT CHECK(ratio > 0) NOT NULL,
	standard_text VARCHAR NOT NULL,
	active_from DATE CHECK(active_from >= '2000-01-01') NOT NULL,
	active_to DATE CHECK(active_to >= '2000-01-01')
);

SELECT dds.create_triggers('dm_standard_points');

------------------------------------------------------------------------------------

CREATE TABLE dds.dm_works_info(
	id SERIAL PRIMARY KEY,
	amount_sheets FLOAT CHECK(amount_sheets >= 0) NOT NULL,
	amount_hours FLOAT CHECK(amount_hours >= 0) NOT NULL,
	standard_point_id INT NOT NULL,
	work VARCHAR NOT NULL,
	CONSTRAINT all_unq UNIQUE(amount_sheets, amount_hours, standard_point_id, work),
	CONSTRAINT dm_works_info_standard_point_id_fk FOREIGN KEY (standard_point_id) REFERENCES dds.dm_standard_points(id)
);

SELECT dds.create_triggers('dm_works_info');

------------------------------------------------------------------------------------

CREATE TABLE dds.fct_works(
	id SERIAL PRIMARY KEY,
	emp_id INT NOT NULL,
	topic_id INT NOT NULL,
	date_id INT NOT NULL,
	work_id INT NOT NULL,
	standard_hours FLOAT CHECK(standard_hours >= 0) NOT NULL,
	CONSTRAINT fct_works_emp_id_fk FOREIGN KEY (emp_id) REFERENCES dds.dm_employees(id),
	CONSTRAINT fct_works_topic_id_fk FOREIGN KEY (topic_id) REFERENCES dds.dm_topics(id),
	CONSTRAINT fct_works_date_id_fk FOREIGN KEY (date_id) REFERENCES dds.dm_dates(id),
	CONSTRAINT fct_works_work_id_fk FOREIGN KEY (work_id) REFERENCES dds.dm_works_info(id)
);

SELECT dds.create_triggers('fct_works');

------------------------------------------------------------------------------------

CREATE TABLE dds.load_info(
	id SERIAL PRIMARY KEY,
	key VARCHAR(50) UNIQUE NOT NULL,
	value JSON NOT NULL
);

SELECT dds.create_triggers('load_info');