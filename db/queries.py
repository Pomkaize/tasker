import json
import sqlite3
import os

DB_NAME = os.path.join(os.path.dirname(__file__), './tasks.db')
DB_TASKS_TABLE_NAME = 'tasks'
DB_TASKS_RESULT_NAME = 'tasks_results'

sqlite_connection = sqlite3.connect(DB_NAME)


def get_table_connection():
    return sqlite_connection


def get_table_cursor():
    connection = get_table_connection()

    return connection.cursor()


def drop_tables():
    query1 = f'''
        DROP TABLE IF EXISTS {DB_TASKS_TABLE_NAME};
        '''

    query2 = f'''
        DROP TABLE IF EXISTS {DB_TASKS_RESULT_NAME};
        '''

    cursor = get_table_cursor()

    for query in [query1, query2]:
        cursor.execute(query)


def create_tasks_table():
    # NCHAR is used for id because of possible hexadecimal notation
    # 00c0d4299880c800 | c18cc65db0ac4c1c972b6bffe1b55a3c
    query = f'''
        CREATE TABLE {DB_TASKS_TABLE_NAME} (
            id NCHAR(55) PRIMARY KEY,
            is_processed BOOLEAN DEFAULT FALSE NOT NULL,
            processed_time datetime DEFAULT NULL
        );'''

    cursor = get_table_cursor()
    cursor.execute(query)


def create_tasks_results_table():
    # NCHAR is used for id because of possible hexadecimal notation
    # 00c0d4299880c800 | c18cc65db0ac4c1c972b6bffe1b55a3c
    query = f'''
        CREATE TABLE {DB_TASKS_RESULT_NAME} (
            id NCHAR(55),
            result TEXT,
            FOREIGN KEY(id) REFERENCES {DB_TASKS_TABLE_NAME}(id)
        );'''

    cursor = get_table_cursor()
    cursor.execute(query)


def create_tables():
    create_tasks_table()
    create_tasks_results_table()


def insert_tasks_ids(ids):
    records = list(map(lambda item: (item, False, None), ids))

    cursor = get_table_cursor()
    query = f'''INSERT INTO {DB_TASKS_TABLE_NAME} VALUES(?,?,?);'''
    cursor.executemany(query, records)

    get_table_connection().commit()


def prepare_task(task):
    (id, result) = task

    return id, json.dumps(result)


def insert_tasks_results(tasks):
    query1 = f'''
        UPDATE {DB_TASKS_TABLE_NAME}
        SET is_processed=TRUE, processed_time=datetime('now')
        WHERE id=?;'''

    query2 = f'''
        DELETE FROM {DB_TASKS_RESULT_NAME}
        WHERE id IN(?)
        '''

    query3 = f'''
          INSERT INTO {DB_TASKS_RESULT_NAME}
          VALUES (?, ?);'''

    records1 = [(task[0],) for task in tasks]
    records2 = (','.join([task[0] for task in tasks]),)
    records3 = list(map(prepare_task, tasks))

    cursor = get_table_cursor()

    cursor.execute(query2, records2)

    get_table_connection().commit()

    cursor.executemany(query1, records1)
    cursor.executemany(query3, records3)

    get_table_connection().commit()


def get_unprocessed_tasks_ids(count):
    query = f'''
        SELECT id
        FROM {DB_TASKS_TABLE_NAME}
        WHERE is_processed=FALSE
        LIMIT ?;'''

    result = get_table_cursor().execute(query, (count,)).fetchall()

    return [x[0] for x in result]

