SET search_path TO work_info;

DROP TABLE IF EXISTS cdm.full_works_last_months;
DROP TABLE IF EXISTS cdm.full_works_history;
DROP TABLE IF EXISTS cdm.load_info;

DROP VIEW IF EXISTS cdm.position_statistic;
DROP VIEW IF EXISTS cdm.works_this_month;
DROP VIEW IF EXISTS cdm.works_previous_month;
DROP VIEW IF EXISTS cdm.works_month_before_previous;

------------------------------------------------------------------------------------

CREATE TABLE cdm.full_works_last_months(
	id SERIAL PRIMARY KEY,
	service_number VARCHAR(10) NOT NULL,
	name VARCHAR(50) NOT NULL,
	position VARCHAR(50) NOT NULL,
	topic VARCHAR(50) NOT NULL,
	hours FLOAT CHECK(hours >= 0) NOT NULL,
	month INT CHECK(month >= 1 AND month <= 12) NOT NULL,
	year INT CHECK(year >= 2000) NOT NULL
);

CREATE TABLE cdm.full_works_history(
	id SERIAL PRIMARY KEY,
	service_number VARCHAR(10) NOT NULL,
	name VARCHAR(50) NOT NULL,
	position VARCHAR(50) NOT NULL,
	topic VARCHAR(50) NOT NULL,
	hours FLOAT CHECK(hours >= 0) NOT NULL,
	month INT CHECK(month >= 1 AND month <= 12) NOT NULL,
	year INT CHECK(year >= 2000) NOT NULL
);

CREATE TABLE cdm.load_info(
	id SERIAL PRIMARY KEY,
	key VARCHAR(50) UNIQUE NOT NULL,
	value JSON NOT NULL
);

------------------------------------------------------------------------------------

CREATE VIEW cdm.position_statistic AS
WITH employees_hiring_dates AS(
    SELECT service_number, MIN(active_from) AS hiring_date
    FROM dds.dm_employees
    GROUP BY service_number
), employees_retiring_dates AS (
    SELECT service_number, MAX(active_to) AS retiring_date
    FROM dds.dm_employees
    GROUP BY service_number
), active_employees AS (
    SELECT *
    FROM dds.dm_employees
    WHERE active_to IS NULL
), info_with_flags AS (
    SELECT e.id , e.service_number, e."name", e."position", e.active_from, e.active_to, 
        CASE 
            WHEN e.active_from = ehd.hiring_date THEN true
            ELSE false
        END AS hire,
        CASE 
            WHEN e.service_number IN 
                (
                    SELECT service_number 
                    FROM active_employees
                )  THEN false
            WHEN e.active_to != erd.retiring_date THEN false
            ELSE true
        END AS retire
    FROM dds.dm_employees e
    LEFT JOIN employees_hiring_dates ehd USING(service_number)
    LEFT JOIN employees_retiring_dates erd USING(service_number)
), info_with_flags_and_positions AS (
    SELECT a.service_number, a."name", a."position" AS position_from, b."position" AS position_to, 
        a.active_from, a.active_to, a.hire, a.retire, 
        CASE 
            WHEN a.active_to IS NOT NULL AND NOT a.retire THEN true
            ELSE false
        END AS change_position
    FROM info_with_flags a
    LEFT JOIN info_with_flags b ON a.service_number = b.service_number AND a.active_to = b.active_from
), not_filtered AS (
    SELECT service_number, name, position_from AS position, active_from AS date_update, 
    	'hire' AS change_info, 
    	'На должность "' || position_from || '"' AS note
    FROM info_with_flags_and_positions
    WHERE hire
    UNION
    SELECT service_number, name, position_from, active_to AS date_update, 
    	'retire' AS change_info, 
    	'С должности "' || position_from || '"' AS note
    FROM info_with_flags_and_positions
    WHERE retire
    UNION 
    SELECT service_number, name, position_to, active_to AS date_update, 
    	'change_position' AS change_info,
        'С "' || position_from || '" на "' || position_to || '"' AS note
    FROM info_with_flags_and_positions
    WHERE change_position
)
SELECT *
FROM not_filtered
WHERE date_update > CURRENT_DATE - INTERVAL '1 year';

CREATE VIEW cdm.works_this_month AS
SELECT de.service_number, de."name", de."position", dt.topic_name AS topic, 
	fw.standard_hours, dd."year", dd."month", 
	CASE 
		WHEN amount_sheets = 0 THEN "work" || ': ' || amount_hours || ' час(а/ов)'
		ELSE "work" || ': ' || amount_sheets || ' ед. (номер стандарта: ' || standard_id || ' - ' || ratio || ')'
	END AS note
FROM dds.fct_works fw
LEFT JOIN dds.dm_employees de ON fw.emp_id = de.id 
LEFT JOIN dds.dm_dates dd ON fw.date_id = dd.id 
LEFT JOIN dds.dm_topics dt ON fw.topic_id = dt.id 
LEFT JOIN dds.dm_works_info dwi ON dwi.id = fw.work_id
LEFT JOIN dds.dm_standard_points dsp ON dsp.id = dwi.standard_point_id
WHERE dd."month" = date_part('month', current_date)
	AND dd."year" = date_part('year', current_date);

CREATE VIEW cdm.works_previous_month AS
SELECT de.service_number, de."name", de."position", dt.topic_name AS topic, 
	fw.standard_hours, dd."year", dd."month", 
	CASE 
		WHEN amount_sheets = 0 THEN "work" || ': ' || amount_hours || ' час(а/ов)'
		ELSE "work" || ': ' || amount_sheets || ' ед. (номер стандарта: ' || standard_id || ' - ' || ratio || ')'
	END AS note
FROM dds.fct_works fw
LEFT JOIN dds.dm_employees de ON fw.emp_id = de.id 
LEFT JOIN dds.dm_dates dd ON fw.date_id = dd.id 
LEFT JOIN dds.dm_topics dt ON fw.topic_id = dt.id 
LEFT JOIN dds.dm_works_info dwi ON dwi.id = fw.work_id
LEFT JOIN dds.dm_standard_points dsp ON dsp.id = dwi.standard_point_id
WHERE dd."month" = date_part('month', current_date - INTERVAL '1 month')
	AND dd."year" = date_part('year', current_date - INTERVAL '1 month');

CREATE VIEW cdm.works_month_before_previous AS
SELECT de.service_number, de."name", de."position", dt.topic_name AS topic, 
	fw.standard_hours, dd."year", dd."month", 
	CASE 
		WHEN amount_sheets = 0 THEN "work" || ': ' || amount_hours || ' час(а/ов)'
		ELSE "work" || ': ' || amount_sheets || ' ед. (номер стандарта: ' || standard_id || ' - ' || ratio || ')'
	END AS note
FROM dds.fct_works fw
LEFT JOIN dds.dm_employees de ON fw.emp_id = de.id 
LEFT JOIN dds.dm_dates dd ON fw.date_id = dd.id 
LEFT JOIN dds.dm_topics dt ON fw.topic_id = dt.id 
LEFT JOIN dds.dm_works_info dwi ON dwi.id = fw.work_id
LEFT JOIN dds.dm_standard_points dsp ON dsp.id = dwi.standard_point_id
WHERE dd."month" = date_part('month', current_date - INTERVAL '2 month')
	AND dd."year" = date_part('year', current_date - INTERVAL '2 month');
	