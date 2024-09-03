DROP TABLE IF EXISTS stg.headsectors_info;
DROP TABLE IF EXISTS stg.employees;
DROP TABLE IF EXISTS stg.standard;

------------------------------------------------------------------------------------

CREATE TABLE stg.headsectors_info(
	id SERIAL PRIMARY KEY,
	service_number VARCHAR(10) NOT NULL,
	name VARCHAR(50) NOT NULL,
	date DATE NOT NULL,
	amount_sheets FLOAT NOT NULL,
	amount_hours FLOAT NOT NULL,
	topic VARCHAR(50) NOT NULL,
	standard_point VARCHAR(20) NOT NULL,
	work VARCHAR NOT NULL
);

CREATE TABLE stg.employees(
	id SERIAL PRIMARY KEY,
	name VARCHAR(50) NOT NULL,
	position VARCHAR(50) NOT NULL,
	service_number VARCHAR(10) NOT NULL,
	last_update_date DATE NOT NULL
);

CREATE TABLE stg.standard(
	id SERIAL PRIMARY KEY,
	standard_id VARCHAR(20) NOT NULL,
	ratio FLOAT NOT NULL,
	standard_text VARCHAR NOT NULL,
	last_update_date DATE NOT NULL
);
