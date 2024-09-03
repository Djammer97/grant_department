from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import psycopg2
import pandas as pd
import json
import os
import requests

db_dest_hook = PostgresHook(postgres_conn_id = 'BD_WORK_INFO')
db_dest = db_dest_hook.get_connection(conn_id = 'BD_WORK_INFO')

metabase_hook = BaseHook('METABASE_DASHBOARDS_CONNECTION')
metabase_dest = metabase_hook.get_connection(conn_id = 'METABASE_DASHBOARDS_CONNECTION')

backup_dir = "/opt/airflow/backups"

backup_command = f"PGPASSWORD={db_dest.password} pg_dump -U {db_dest.login} -h {db_dest.host} -p {db_dest.port} {db_dest.schema} > \
    {backup_dir}/{db_dest.schema}_backup_$(date +%Y%m%d%H%M%S).sql"

def truncate_table_function(cursor, table_name):
    query = f"""
        TRUNCATE {table_name};
        ALTER SEQUENCE {table_name}_id_seq RESTART WITH 1;
    """

    cursor.execute(query)

def stg_tables_upload(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        # Попытка открыть файл, если файла не существует, то таск заканчивается

        file = f"/opt/airflow/works_data_source/{kwargs['source_name']}"

        try:
            df = pd.read_excel(file)
        except:
            print('Файла нет')
            return

        # Очистка stage таблицы от старых данных

        truncate_table_function(dest_cursor, kwargs['dest_table_name'])

        query = f"""
            INSERT INTO {kwargs['dest_table_name']}
            ({kwargs['dest_table_columns'][0]}, {kwargs['dest_table_columns'][1]}, {kwargs['dest_table_columns'][2]}, {kwargs['dest_table_column_unique']})
            VALUES(%(column_0)s, %(column_1)s, %(column_2)s, %(column_unique)s)
        """

        for index, row in df.iterrows():
            dest_cursor.execute(query, {
                'column_0': row[kwargs['source_table_columns'][0]],
                'column_1': row[kwargs['source_table_columns'][1]],
                'column_2': row[kwargs['source_table_columns'][2]],
                'column_unique': row[kwargs['source_table_column_unique']],
            }
            )

            print(row)

        connection.commit()
        dest_cursor.close()

def stg_headsectors_info_upload(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:
        
        dest_cursor = connection.cursor()

        truncate_table_function(dest_cursor, 'stg.headsectors_info')

        directory = "/opt/airflow/works_data_source"

        for filename in os.listdir(directory):
            if filename.startswith('headsectors_info'):
                file_path = os.path.join(directory, filename)

                try:
                    df = pd.read_excel(file_path)
                except:
                    print('Файла нет')
                    continue

                query = """
                    INSERT INTO stg.headsectors_info(service_number, name, "date", amount_sheets, 
                        amount_hours, topic, "work", standard_point)
                    VALUES (%(service_number)s, 
                        %(name)s, 
                        %(date)s, 
                        COALESCE(NULLIF(%(amount_sheets)s, 'NaN'), '0'), 
                        COALESCE(NULLIF(%(amount_hours)s, 'NaN'), '0'), 
                        COALESCE(NULLIF(%(topic)s::VARCHAR, 'NaN'), 'Вне темы'), 
                        %(work)s, 
                        COALESCE(NULLIF(%(standard_point)s, 'NaN'), '0')
                        )
                """

                for index, work in df.iterrows():
                    print(work)
                    dest_cursor.execute(query, {
                        'service_number': work['Табельный номер'],
                        'name': work['Имя'],
                        'date': work['Дата'],
                        'amount_sheets': work['Количество листов А4'],
                        'amount_hours': work['Количество часов'],
                        'topic': work['Тема'],
                        'work': work['Описание работы'],
                        'standard_point': work['Пункт норматива'],
                    })

        connection.commit()
        dest_cursor.close()

def get_load_info_by_key(cursor, key, level):
    query = f"""
        SELECT value
        FROM {level}.load_info
        WHERE key = %(key)s
    """

    cursor.execute(query, {
        'key': key
    })

    data = cursor.fetchone()

    if data:
        data = data[0]

    return data

def set_load_info_by_key(cursor, key, value, level):
    query = f"""
        INSERT INTO {level}.load_info("key", value)
        VALUES (%(key)s, %(value)s)
        ON CONFLICT (key)
        DO UPDATE SET
            value = %(value)s
    """

    print(key, value)

    cursor.execute(query, {
        'key': key,
        'value': value
    })

def make_dict_from_query_data(cursor, data):
    columns = [column[0] for column in cursor.description]

    data = [dict(zip(columns, one_data)) for one_data in data]

    return data

def dm_standard_points_and_employees_update(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        # Ставим метку на пункты, удаленные из норматива или списка сотрудников

        query = f"""
            UPDATE {kwargs['dest_table']}
            SET active_to = %(current_date)s
            WHERE active_to IS NULL AND {kwargs['unique_field']} NOT IN (
                SELECT  {kwargs['unique_field']}
	            FROM {kwargs['source_table']}
            )
        """
        
        dest_cursor.execute(query, {
            'current_date': datetime.now().date()
        })

        # Берем данные из информационной таблице, для определения даты, с которой брать данные

        data = get_load_info_by_key(dest_cursor, kwargs['load_table_key'], 'dds')

        # Выбираем данные с определенной даты

        if data:
            last_date = data['last_date']

            query = f"""
                SELECT {kwargs['source_columns'][0]}, {kwargs['source_columns'][1]}, 
                    {kwargs['source_columns'][2]}, {kwargs['date_column']}
                FROM {kwargs['source_table']}
                WHERE {kwargs['date_column']} > %(last_date)s
            """

            dest_cursor.execute(query, {
                'last_date': last_date
            })
        else:
            query = f"""
                SELECT {kwargs['source_columns'][0]}, {kwargs['source_columns'][1]}, 
                    {kwargs['source_columns'][2]}, {kwargs['date_column']}
                FROM {kwargs['source_table']}
            """

            dest_cursor.execute(query)
        
        data = dest_cursor.fetchall()
        
        if data:
            data = make_dict_from_query_data(dest_cursor, data)

            new_last_date = max(data, key=lambda row: row[kwargs['date_column']])[kwargs['date_column']]

            # Ставим метку на данные, которые были обновлены и добавляем новую строку

            for one_data in data:
                print(one_data)

                query = f"""
                    UPDATE {kwargs['dest_table']}
                    SET active_to = %(last_date)s
                    WHERE active_to IS NULL AND {kwargs['unique_field']} = %(unique_field)s
                """

                dest_cursor.execute(query, {
                    'last_date': one_data[kwargs['date_column']],
                    'unique_field': one_data[kwargs['unique_field']]
                })

                query = f"""
                    INSERT INTO {kwargs['dest_table']}({kwargs['dest_columns'][0]}, {kwargs['dest_columns'][1]}, 
                        {kwargs['dest_columns'][2]}, active_from)
                    VALUES(%(column_0)s, %(column_1)s, %(column_2)s, %(date_column)s)
                """

                dest_cursor.execute(query, {
                    'column_0': one_data[kwargs['source_columns'][0]],
                    'column_1': one_data[kwargs['source_columns'][1]],
                    'column_2': one_data[kwargs['source_columns'][2]],
                    'date_column': one_data[kwargs['date_column']],
                })

            new_data =  json.dumps({'last_date': str(new_last_date)})

            # Обновление данных о загрузке

            set_load_info_by_key(dest_cursor, kwargs['load_table_key'], new_data, 'dds')
        
        connection.commit()
        dest_cursor.close()

def dm_topics_update(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        query = """
            SELECT DISTINCT topic
            FROM stg.headsectors_info
            WHERE topic IS NOT NULL
        """

        data = dest_cursor.execute(query)

        data = dest_cursor.fetchall()

        query = """
            INSERT INTO dds.dm_topics(topic_name)
            VALUES(%(topic_name)s)
            ON CONFLICT(topic_name)
            DO NOTHING
        """

        if data:
            data = make_dict_from_query_data(dest_cursor, data)

            for one_data in data:
                print(one_data['topic'])

                dest_cursor.execute(query, {
                    'topic_name': one_data['topic']
                })
        
        connection.commit()
        dest_cursor.close()

def dm_dates_update(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        query = """
            SELECT DISTINCT "date"
            FROM stg.headsectors_info
        """

        data = dest_cursor.execute(query)

        data = dest_cursor.fetchall()

        query = """
            INSERT INTO dds.dm_dates("date", "year", "month", "day")
            VALUES(%(date)s, %(year)s, %(month)s, %(day)s)
            ON CONFLICT(date)
            DO NOTHING
        """

        if data:
            data = make_dict_from_query_data(dest_cursor, data)

            for one_data in data:
                print(one_data['date'])

                dest_cursor.execute(query, {
                    'date': one_data['date'],
                    'year': one_data['date'].year,
                    'month': one_data['date'].month,
                    'day': one_data['date'].day
                })
        
        connection.commit()
        dest_cursor.close()

def dm_works_info_update(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        # Берем данные по работам и соединяем с информацией из информационной таблицы
        # В фильтре ищем данные по пользователям либо с определенной даты либо тех кто не записан в информационной таблице
        # Также заменяем пункт стандарта на id из таблицы измерений стандарта, с учетом даты

        query = """
            WITH json_data AS (
                SELECT j.key, j.value
                FROM dds.load_info li, json_each_text(value) j
                WHERE li.key = 'works_info_last_dates'
            ),
            work_info_with_date AS (
            SELECT id, amount_sheets, amount_hours, "work", standard_point, "date"
            FROM stg.headsectors_info hi
            LEFT JOIN json_data ON hi.service_number = json_data.key
            WHERE hi."date" > json_data.value::date OR json_data.value IS NULL
            ),
            work_info_with_standard_id AS (
            SELECT wiwd.id, wiwd.amount_sheets, wiwd.amount_hours, wiwd."work", wiwd.standard_point, wiwd."date", sp.id AS standard_point_id, sp.active_from,
            ROW_NUMBER() OVER(PARTITION BY wiwd.id ORDER BY wiwd."date" - sp.active_from) AS days_diff_top
            FROM work_info_with_date wiwd
            LEFT JOIN dds.dm_standard_points sp ON wiwd.standard_point = sp.standard_id  
            WHERE wiwd.standard_point IS NULL OR wiwd.date >= sp.active_from 
            )
            SELECT amount_sheets, amount_hours, standard_point_id, work
            FROM work_info_with_standard_id
            WHERE days_diff_top = 1
        """

        dest_cursor.execute(query)

        data = dest_cursor.fetchall()

        if data:
            data = make_dict_from_query_data(dest_cursor, data)

            query = """
                INSERT INTO dds.dm_works_info(amount_sheets, amount_hours, standard_point_id, "work")
                VALUES(%(amount_sheets)s, %(amount_hours)s, %(standard_point_id)s, %(work)s)
                ON CONFLICT(amount_sheets, amount_hours, standard_point_id, "work") 
                DO NOTHING
            """

            # Вставляем все полученные данные

            for one_data in data:
                print(one_data)
                dest_cursor.execute(query, {
                    'amount_sheets': one_data['amount_sheets'],
                    'amount_hours': one_data['amount_hours'],
                    'standard_point_id': one_data['standard_point_id'],
                    'work': one_data['work'],
                })

            # Обновляем таблицу данных

            query = """
                WITH dates AS(
                    SELECT service_number, MAX(date) as date
                    FROM stg.headsectors_info
                    GROUP BY service_number
                    ORDER BY service_number
                )
                INSERT INTO dds.load_info (key, value)
                SELECT 'works_info_last_dates', json_object_agg(service_number, date) AS json_result
                FROM dates
                ON CONFLICT(key)
                DO UPDATE
                SET
                    value = excluded.value;
            """
            dest_cursor.execute(query)

        connection.commit()
        dest_cursor.close()

def fct_works_update(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        # Берем данные по работам и соединяем с информацией из информационной таблицы и всех таблиц измерений
        # В фильтре ищем данные по пользователям либо с определенной даты лиюо тех кто не записан в информационной таблице

        query = """
            WITH json_data AS (
                SELECT j.key, j.value
                FROM dds.load_info li, json_each_text(value) j
                WHERE li.key = 'works_last_dates'
            ), filtered_data AS (
                SELECT *
                FROM stg.headsectors_info hi
                LEFT JOIN json_data jd ON hi.service_number = jd.key
                WHERE jd.value IS NULL OR hi."date" > jd.value::DATE
            ), data_with_topic_id AS (
                SELECT fd.id, fd.service_number, fd.date, fd.amount_sheets, 
                    fd.amount_hours, fd.standard_point, fd.work, dt.id AS topic_id
                FROM filtered_data fd
                LEFT JOIN dds.dm_topics dt ON dt.topic_name = fd.topic
            ), data_with_days_diff_for_emp AS (
                SELECT dwti.id, dwti.date, dwti.amount_sheets, dwti.amount_hours, dwti.standard_point, dwti.work, dwti.topic_id, 
                    e.id AS emp_id, e.active_from, dwti.date - e.active_from AS days_diff,
                    ROW_NUMBER() OVER(PARTITION BY dwti.id ORDER BY dwti.date - e.active_from) AS days_diff_top
                FROM data_with_topic_id dwti
                LEFT JOIN dds.dm_employees e USING(service_number)
                WHERE dwti.date >= e.active_from 
            ), data_with_days_for_standard AS (
                SELECT dwd.id, dwd.date, dwd.amount_sheets, dwd.amount_hours, dwd.standard_point, dwd.work, dwd.topic_id, dwd.emp_id,
                    sp.active_from, sp.id AS st_id, sp.ratio,
                    ROW_NUMBER() OVER(PARTITION BY dwd.id ORDER BY dwd.date - sp.active_from) AS days_diff_top
                FROM data_with_days_diff_for_emp dwd
                LEFT JOIN dds.dm_standard_points sp ON dwd.standard_point = sp.standard_id
                WHERE days_diff_top = 1 AND (dwd.date >= sp.active_from OR dwd.standard_point IS NULL)
            ), data_without_hours AS (
                SELECT d.id AS date_id, dwdfs.topic_id, dwdfs.emp_id, wi.id AS work_id, dwdfs.amount_sheets, dwdfs.amount_hours, dwdfs.ratio
                FROM data_with_days_for_standard dwdfs
                LEFT JOIN dds.dm_works_info wi 
                    ON wi.standard_point_id = dwdfs.st_id AND 
                    wi.amount_sheets = dwdfs.amount_sheets AND
                    wi.amount_hours = dwdfs.amount_hours AND
                    wi.work = dwdfs.work
                LEFT JOIN dds.dm_dates d ON dwdfs.date = d.date
                WHERE dwdfs.days_diff_top = 1
            )
            SELECT emp_id, topic_id, date_id, work_id, 
                CASE
                    WHEN amount_sheets = 0 THEN amount_hours
                    ELSE amount_sheets * ratio
                END AS standard_hours
            FROM data_without_hours 
        """

        dest_cursor.execute(query)

        data = dest_cursor.fetchall()

        if data:
            data = make_dict_from_query_data(dest_cursor, data)

            query = """
                INSERT INTO dds.fct_works(emp_id, topic_id, date_id, work_id, standard_hours)
                VALUES(%(emp_id)s, %(topic_id)s, %(date_id)s, %(work_id)s, %(standard_hours)s)
            """

            # Вставляем все полученные данные

            for one_data in data:
                print(one_data)
                dest_cursor.execute(query, {
                    'emp_id': one_data['emp_id'],
                    'topic_id': one_data['topic_id'],
                    'date_id': one_data['date_id'],
                    'work_id': one_data['work_id'],
                    'standard_hours': one_data['standard_hours'],
                })

            # Обновляем таблицу данных

            query = """
                WITH dates AS(
                    SELECT service_number, MAX(date) as date
                    FROM stg.headsectors_info
                    GROUP BY service_number
                    ORDER BY service_number
                )
                INSERT INTO dds.load_info (key, value)
                SELECT 'works_last_dates', json_object_agg(service_number, date) AS json_result
                FROM dates
                ON CONFLICT(key)
                DO UPDATE
                SET
                    value = excluded.value;
            """
            dest_cursor.execute(query)

        connection.commit()
        dest_cursor.close()

def get_preprevious_year_and_month(first_date):
    if first_date.month == 1:
        return {'year': first_date.year - 1, 'month': 12}
    elif first_date.month == 2:
        return {'year': first_date.year - 1, 'month': 12}
    else:
        return {'year': first_date.year, 'month': first_date.month - 2}

def full_works_fullfil(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        dest_cursor = connection.cursor()

        current_date = datetime.now().date()

        pre_previous_date = get_preprevious_year_and_month(current_date)

        data = get_load_info_by_key(dest_cursor, 'full_works_last_id', 'cdm')

        if data:
            last_id = data['last_id']
        else:
            last_id = 0

        query = """
            INSERT INTO cdm.full_works_history(service_number, name, position, topic, hours, year, month)
            SELECT service_number, name, position, topic, hours, year, month
            FROM cdm.full_works_last_months
            WHERE year < %(year)s OR (year = %(year)s AND month < %(month)s)
        """

        dest_cursor.execute(query, {
            'year': pre_previous_date['year'],
            'month': pre_previous_date['month']
        })

        query = """
            DELETE FROM cdm.full_works_last_months
            WHERE year < %(year)s OR (year = %(year)s AND month < %(month)s)
        """

        dest_cursor.execute(query, {
            'year': pre_previous_date['year'],
            'month': pre_previous_date['month']
        })

        query = """
            SELECT fw.id
            FROM dds.fct_works fw
            WHERE fw.id > %(last_id)s
        """

        dest_cursor.execute(query, {
            'last_id': last_id
        })

        data = dest_cursor.fetchall()

        if data:
            data = make_dict_from_query_data(dest_cursor, data)
        else:
            return

        new_last_id = max(data, key=lambda one_data: one_data['id'])['id']

        query = """
            SELECT de.service_number, de."name", de."position", dt.topic_name, SUM(fw.standard_hours) AS standard_hours, dd."year", dd."month" 
            FROM dds.fct_works fw
            LEFT JOIN dds.dm_employees de ON fw.emp_id = de.id 
            LEFT JOIN dds.dm_dates dd ON fw.date_id = dd.id 
            LEFT JOIN dds.dm_topics dt ON fw.topic_id = dt.id 
            WHERE fw.id > %(last_id)s
            GROUP BY de.service_number, de."name", de."position", dt.topic_name, dd."year", dd."month" 
        """

        dest_cursor.execute(query, {
            'last_id': last_id
        })

        data = dest_cursor.fetchall()

        if data:
            data = make_dict_from_query_data(dest_cursor, data)
        else:
            return

        query = """
            INSERT INTO cdm.full_works_last_months(service_number, name, position, topic, hours, year, month)
            VALUES(%(service_number)s, %(name)s, %(position)s, %(topic)s, %(hours)s, %(year)s, %(month)s)
        """

        query_history = """
            INSERT INTO cdm.full_works_history(service_number, name, position, topic, hours, year, month)
            VALUES(%(service_number)s, %(name)s, %(position)s, %(topic)s, %(hours)s, %(year)s, %(month)s)
        """

        for one_data in data:
            print(one_data)
            if one_data['year'] < pre_previous_date['year'] or (one_data['year'] == pre_previous_date['year'] and one_data['month'] < pre_previous_date['month']):
                dest_cursor.execute(query_history, {
                    'service_number': one_data['service_number'],
                    'name': one_data['name'], 
                    'position': one_data['position'], 
                    'topic': one_data['topic_name'], 
                    'hours': one_data['standard_hours'], 
                    'year': one_data['year'], 
                    'month': one_data['month']
                })
            else:
                dest_cursor.execute(query, {
                    'service_number': one_data['service_number'],
                    'name': one_data['name'], 
                    'position': one_data['position'], 
                    'topic': one_data['topic_name'], 
                    'hours': one_data['standard_hours'], 
                    'year': one_data['year'], 
                    'month': one_data['month']
                })

        set_load_info_by_key(dest_cursor, 'full_works_last_id', json.dumps({'last_id': new_last_id}), 'cdm')

        connection.commit()
        dest_cursor.close() 

def get_year_month_with_delta(delta):
    now = datetime.now()

    if delta == 0:
        return {'year': now.year, 'month': now.month}
    elif now.month <= delta:
        return {'year': now.year - int(delta / 12), 'month': 12 - (delta % 12 - now.month)}
    return {'year': now.year, 'month': now.month - delta}

def dashboard_full_works_create(*args, **kwargs):
    with psycopg2.connect(dbname=db_dest.schema, user=db_dest.login, 
        password=db_dest.password, host=db_dest.host, port=db_dest.port) as connection:

        # Обявление основных переменных

        dest_cursor = connection.cursor()

        headers={
            'username': metabase_dest.login,
            'password': metabase_dest.password,
            "Content-Type": "application/json",
        }

        url = f'http://{metabase_dest.host}:{metabase_dest.port}/api'
        card_names = [
            'Результаты работ за прошлый месяц',
            'Результаты работ за этот месяц', 
            'Результаты работ за позапрошлый месяц',
        ]
        tab_names = [
            'Прошлый месяц',
            'Этот месяц', 
            'Позапрошлый месяц',
        ]
        dates = [
            get_year_month_with_delta(1),
            get_year_month_with_delta(0),
            get_year_month_with_delta(2)
        ]
        views_names = [
            'cdm.works_previous_month', 
            'cdm.works_this_month', 
            'cdm.works_month_before_previous'
        ]
        topics = []
        for one_date in dates:
            query = f"""
                SELECT DISTINCT topic 
                FROM cdm.full_works_last_months
                WHERE year = {one_date['year']} AND month = {one_date['month']}
            """
            dest_cursor.execute(query)
            data = dest_cursor.fetchall()
            if data:
                data = [one_data[0] for one_data in data]
            topics.append(data)
        
        print('Список работ - ', topics)
        
        dashboard_name = 'Результаты работ'

        session = requests.Session()
        auth_response = session.post(f'{url}/session', json=headers)

        if auth_response.ok:
            print('Авторизация прошла успешно')

            token = auth_response.json()['id']
            session.headers.update({"X-Metabase-Session": token})

            # Удаление старого dashboard и карточек запросов

            response = session.get(url=f"{url}/dashboard")
                        
            dashboards = response.json()

            print('Получен id dashboard - ', dashboards)

            for dashboard in dashboards:
                if dashboard['name'] == dashboard_name:
                    dashboard_id = dashboard['id']

                    response = session.get(url=f"{url}/card/").json()

                    question_ids = [card["id"] for card in response if card['name'].startswith(tuple(card_names))]

                    for question in question_ids:
                        print('Удаляем card с id ', question)
                        
                        response = session.delete(url=f'{url}/card/{question}')
                            
                    response = session.delete(f"{url}/dashboard/{dashboard_id}")
                    print('Удаляем dashboard с id ', dashboard_id)
                    break

            # Создание нового dashboard

            dashboard_data = {
                "name": dashboard_name,  
                "description": "Результаты работ за 3 месяца"
            }

            response = session.post(f"{url}/dashboard", json=dashboard_data)
            new_dashboard = response.json()
            new_dashboard_id = new_dashboard['id'] 

            print('Создаем dashboard с id ', new_dashboard_id)

            # Получаем номер БД, из которой берем данные
            response = session.get(f"{url}/database")
            database_id = [one_bd['id'] for one_bd in response.json()['data'] if one_bd['name'] == 'work_info'][0]
            
            # Создаем карточки запросов
            cards_id = []
            counter = -1
            for one_date, one_name, all_topics in zip(dates, card_names, topics):
                counter += 1
                cards_id.append([])

                query = f"""
                    SELECT service_number, name, position, SUM(hours) AS hours
                    FROM cdm.full_works_last_months
                    WHERE "year" = {one_date['year']} AND "month" = {one_date['month']}
                    GROUP BY service_number, name, position
                    ORDER BY service_number, name
                """
        
                card_data = {
                    "name": f'{one_name} - все (таблица)',
                    "dataset_query": {
                        "type": "native",
                        "native": {
                            "query": query,
                            "template-tags": {}
                        },
                        "database": database_id
                    },
                    "display": "table",
                    "visualization_settings": {}
                }
            
                response = session.post(f'{url}/card', json=card_data)

                cards_id[counter].append(response.json()['id'])

                query = f"""
                    SELECT service_number, SUM(hours) AS hours, name, position
                    FROM cdm.full_works_last_months
                    WHERE "year" = {one_date['year']} AND "month" = {one_date['month']}
                    GROUP BY service_number, name, position
                    ORDER BY service_number
                """

                card_data = {
                    "name": f'{one_name} - все (график)',
                    "dataset_query": {
                        "type": "native",
                        "native": {
                            "query": query,
                            "template-tags": {}
                        },
                        "database": database_id
                    },
                    "display": "bar",
                    "visualization_settings": {
                        "graph.dimensions": ["service_number"],  
                        "graph.metrics": ["hours"],  
                        "graph.x_axis.title_text": "Service number",
                        "graph.y_axis.title_text": "Hours"
                    }
                }

                response = session.post(f'{url}/card', json=card_data)

                cards_id[counter].append(response.json()['id'])

                card_data = {
                    "name": f'{one_name} - все (диаграмма)',
                    "dataset_query": {
                        "type": "native",
                        "native": {
                            "query": query,
                            "template-tags": {}
                        },
                        "database": database_id
                    },
                    "display": "pie",
                    "visualization_settings": {
                        "pie.dimension": "service_number",  
                        "pie.metric": "hours",
                        "pie.slice_order": "alphabetical",
                        "pie.show_slices_values": True,
                        "pie.slice_threshold": 0
                    }
                }

                response = session.post(f'{url}/card', json=card_data)

                cards_id[counter].append(response.json()['id'])

                for topic in all_topics:
                    query = f"""
                        SELECT service_number, name, position, topic, hours
                        FROM cdm.full_works_last_months
                        WHERE "year" = {one_date['year']} AND "month" = {one_date['month']} AND topic = '{topic}'
                        ORDER BY service_number, name
                    """

                    card_data = {
                        "name": f'{one_name} - {topic} (таблица)',
                        "dataset_query": {
                            "type": "native",
                            "native": {
                                "query": query,
                                "template-tags": {}
                            },
                            "database": database_id
                        },
                        "display": "table",
                        "visualization_settings": {}
                    }
                
                    response = session.post(f'{url}/card', json=card_data)

                    cards_id[counter].append(response.json()['id'])

                    query = f"""
                        SELECT service_number, hours, name, position
                        FROM cdm.full_works_last_months
                        WHERE "year" = {one_date['year']} AND "month" = {one_date['month']} AND topic = '{topic}'
                        ORDER BY service_number
                    """

                    card_data = {
                        "name": f'{one_name} - {topic} (график)',
                        "dataset_query": {
                            "type": "native",
                            "native": {
                                "query": query,
                                "template-tags": {}
                            },
                            "database": database_id
                        },
                        "display": "bar",
                        "visualization_settings": {
                            "graph.dimensions": ["service_number"],  
                            "graph.metrics": ["hours"],  
                            "graph.x_axis.title_text": "Service number",
                            "graph.y_axis.title_text": "Hours"
                        }
                    }

                    response = session.post(f'{url}/card', json=card_data)

                    cards_id[counter].append(response.json()['id'])

                    card_data = {
                        "name": f'{one_name} - {topic} (диаграмма)',  
                        "dataset_query": {
                            "type": "native",
                            "native": {
                                "query": query,
                                "template-tags": {}
                            },
                            "database": database_id
                        },
                        "display": "pie",
                        "visualization_settings": {
                            "pie.dimension": "service_number",  
                            "pie.metric": "hours",
                            "pie.slice_order": "alphabetical",
                            "pie.show_slices_values": True,
                            "pie.slice_threshold": 0
                        }
                    }

                    response = session.post(f'{url}/card', json=card_data)

                    cards_id[counter].append(response.json()['id'])

                query = f"""
                    SELECT service_number, "name", "position", topic, standard_hours, note
                    FROM {views_names[counter]}
                    ORDER BY service_number, topic, "name", "position", standard_hours, note
                """

                card_data = {
                    "name": f'{one_name} - (подробный отчет)',
                    "dataset_query": {
                        "type": "native",
                        "native": {
                            "query": query,
                            "template-tags": {}
                        },
                        "database": database_id
                    },
                    "display": "table",
                    "visualization_settings": {}
                }

                response = session.post(f'{url}/card', json=card_data)

                cards_id[counter].append(response.json()['id'])

            # Создаем вкладки и карточки dashbord-ов
            dashboard_card_data = []
            new_tab = []
            counter = -1
            for one_tab in cards_id:
                counter += 1
                for cards in range(0, len(one_tab), 3):
                    if len(one_tab) == cards + 1:
                        dashboard_card_data.append({
                            "id": one_tab[cards],
                            'card_id': one_tab[cards], 
                            'dashboard_tab_id': counter,
                            "size_x": 24,
                            "size_y": 7,
                            "row": 14 + cards * 20,
                            "col": 0,                
                            "parameter_mappings": [],
                            "series": []
                        }
                        )
                        break

                    dashboard_card_data.append({
                        "id": one_tab[cards],
                        'card_id': one_tab[cards], 
                        'dashboard_tab_id': counter,
                        "size_x": 24,
                        "size_y": 7,
                        "row": 0 + cards * 20,
                        "col": 0,                
                        "parameter_mappings": [],
                        "series": []
                    }
                    )

                    dashboard_card_data.append({
                        "id": one_tab[cards + 1],
                        'card_id': one_tab[cards + 1], 
                        'dashboard_tab_id': counter,
                        "size_x": 12,
                        "size_y": 7,
                        "row": 7 + cards * 20,
                        "col": 0,                
                        "parameter_mappings": [],
                        "series": []
                    }
                    )

                    dashboard_card_data.append({
                        "id": one_tab[cards + 2],
                        'card_id': one_tab[cards + 2], 
                        'dashboard_tab_id': counter,
                        "size_x": 12,
                        "size_y": 7,
                        "row": 7 + cards * 20,
                        "col": 12,                
                        "parameter_mappings": [],
                        "series": []
                    }
                    )


                new_tab.append({
                    'id': counter,
                    'name': tab_names[counter],
                }
                )

            # Обновление данных dashboard-а
            update_data = {
                "name": new_dashboard.get("name"),
                "description": new_dashboard.get("description", ""),
                "tabs": new_tab,
                "dashcards": dashboard_card_data,  
                'width': 'full'       
            }

            response = session.put(f"{url}/dashboard/{new_dashboard_id}", json=update_data)

        session.close()

default_args = {
    'owner': 'Djammer',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(year=2000, month=1, day=1, hour=9, minute=0, second=0)
}

dag = DAG(
    dag_id='employees_update', 
    schedule_interval='0 6 * * *',
    catchup=False, 
    default_args=default_args
)

backup_task = BashOperator(
    task_id='backup_database',
    bash_command=backup_command,
    dag=dag,
)

tg1 = TaskGroup(
    group_id='stg_upload',
    dag=dag
)

employees_stg_upload = PythonOperator(
    task_id='stg_employees_upload',
    python_callable=stg_tables_upload, 
    task_group = tg1,
    op_kwargs = {
        'source_name': 'employees.xlsx',
        'dest_table_name': 'stg.employees',
        'conf_table_key': 'employees_last_dates',
        'dest_table_columns': ['name', 'position', 'last_update_date'],
        'source_table_columns': ['Имя', 'Должность', 'Дата изменения'],
        'dest_table_column_unique': 'service_number',
        'source_table_column_unique': 'Табельный номер'
    },
    dag=dag)

standard_stg_upload = PythonOperator(
    task_id='stg_standard_upload',
    python_callable=stg_tables_upload, 
    task_group = tg1,
    op_kwargs = {
        'source_name': 'standard.xlsx',
        'dest_table_name': 'stg.standard',
        'conf_table_key': 'standard_last_dates',
        'dest_table_columns': ['ratio', 'standard_text', 'last_update_date'],
        'source_table_columns': ['Количество н/ч за единицу', 'Текст пункта', 'Дата обновления'],
        'dest_table_column_unique': 'standard_id',
        'source_table_column_unique': 'Пункт стандарта'
    },
    dag=dag)

headsectors_info_stg_upload = PythonOperator(
    task_id='stg_headsectors_info_upload',
    python_callable=stg_headsectors_info_upload,
    task_group=tg1,
    dag=dag
)

standard_points_dm_update = PythonOperator(
    task_id='dm_standard_points_update',
    python_callable=dm_standard_points_and_employees_update,
    op_kwargs = {
        'dest_table': 'dds.dm_standard_points',
        'unique_field': 'standard_id',
        'source_table': 'stg.standard',
        'load_table_key': 'standard_last_date',
        'source_columns': ['standard_id', 'ratio', 'standard_text'],
        'date_column': 'last_update_date',
        'dest_columns': ['standard_id', 'ratio', 'standard_text', 'active_from', 'active_to'],
    },
    dag=dag
)

tg2 = TaskGroup(
    group_id='dm_tables_upload',
    dag=dag
)

topics_dm_update = PythonOperator(
    task_id='dm_topics_update',
    python_callable=dm_topics_update,
    dag=dag,
    task_group=tg2
)

dates_dm_update = PythonOperator(
    task_id='dm_dates_update',
    python_callable=dm_dates_update,
    dag=dag,
    task_group=tg2
)

works_info_dm_update = PythonOperator(
    task_id='dm_works_info_update',
    python_callable=dm_works_info_update,
    dag=dag,
    task_group=tg2
)

employees_dm_update = PythonOperator(
    task_id='dm_employees_update',
    python_callable=dm_standard_points_and_employees_update,
    dag=dag,
    op_kwargs = {
        'dest_table': 'dds.dm_employees',
        'unique_field': 'service_number',
        'source_table': 'stg.employees',
        'load_table_key': 'employees_last_date',
        'source_columns': ['name', 'position', 'service_number'],
        'date_column': 'last_update_date',
        'dest_columns': ['name', 'position', 'service_number', 'active_from', 'active_to'],
    },
    task_group=tg2
)

works_fct_update = PythonOperator(
    task_id='fct_works_update',
    python_callable=fct_works_update,
    dag=dag,
)

cdm_full_works_fullfil = PythonOperator(
    task_id='cdm_full_works_fullfil',
    python_callable=full_works_fullfil,
    dag=dag
)

full_works_dashboard_create = PythonOperator(
    task_id = 'dashboard_full_works_create',
    python_callable=dashboard_full_works_create,
    dag=dag,
)

backup_task >> tg1 >> standard_points_dm_update >> tg2 >> works_fct_update >> cdm_full_works_fullfil >> full_works_dashboard_create