"""Скрипт для заполнения данными таблиц в БД Postgres."""
from utils.csv_reader import *
from utils.db_connector import db_connector

# Пути к файлам CSV для удобства записаны в переменные
employees_filename = 'north_data/employees_data.csv'
customers_filename = 'north_data/customers_data.csv'
orders_filename = 'north_data/orders_data.csv'

# Списки с данными из файлов CSV записываются в переменные
employees_data = csv_reader(employees_filename)
customers_data = csv_reader(customers_filename)
orders_data = csv_reader(orders_filename)

# Подключение к базе данных PostgreSQL
db_conn = db_connector()

# Блок добавления в базу данных потаблично
try:
    with db_conn:
        with db_conn.cursor() as cur:
            for row in customers_data:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                            (row['customer_id'], row['company_name'], row['contact_name']))
            for row in employees_data:
                cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                            (row['employee_id'], row['first_name'], row['last_name'], row['title'], row['birth_date'],
                             row['notes']))
            for row in orders_data:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                            (row['order_id'], row['customer_id'], row['employee_id'], row['order_date'],
                             row['ship_city']))
finally:
    db_conn.close()
