import psycopg2


def db_connector():
    """Производит подключение к базе данных"""

    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password='665724'
    )

    return conn
