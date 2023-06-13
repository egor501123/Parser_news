import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import feedparser
import psycopg2


# Устанавливаем параметры подключения к базе данных постгресс
pg_host = 'AAA'
pg_port = 'aaa'
pg_database = 'postgres'
pg_user = 'postgres'
pg_password = 'aaaa'


# Функция для установления соединения с базой данных PostgreSQL
def connect_to_pg(host, port, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Connection to PostgreSQL database successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")


def create_table():

    connection_PG = connect_to_pg(host=pg_host, port=pg_port, database=pg_database, user=pg_user, password=pg_password)
    cursor = connection_PG.cursor()

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            pub_date TIMESTAMP NOT NULL,
            link TEXT NOT NULL
        )
    '''

    cursor.execute(create_table_query)
    connection_PG.commit()
    cursor.close()
    connection_PG.close()


# Функция для парсинга новостей и их загрузки в таблицу
def parse_and_load_news():
    connection_PG = connect_to_pg(host=pg_host, port=pg_port, database=pg_database, user=pg_user, password=pg_password)
    cursor = connection_PG.cursor()

    # Получение данных из RSS-ленты ТАСС
    rss_feed_url = 'https://tass.ru/rss/v2.xml'
    feed = feedparser.parse(rss_feed_url)

    for entry in feed.entries:
        title = entry.title
        description = entry.description
        pub_date = entry.published
        link = entry.link

        # Проверка наличия новости в таблице по ссылке
        check_query = f"SELECT COUNT(*) FROM news WHERE link = '{link}'"
        cursor.execute(check_query)
        result = cursor.fetchone()

        if result[0] == 0:
            # Вставка новой новости в таблицу
            insert_query = f"INSERT INTO news (title, description, pub_date, link) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (title, description, pub_date, link))
            connection_PG.commit()

    cursor.close()
    connection_PG.close()


# Создание DAG
default_args = {
    'owner': 'bugakov',
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('news_parsing_dag',
          default_args=default_args,
          schedule_interval='@daily')

# Создание операторов
create_table_operator = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag
)

parse_and_load_news_operator = PythonOperator(
    task_id='parse_and_load_news_task',
    python_callable=parse_and_load_news,
    dag=dag
)

# Определение порядка выполнения операторов
create_table_operator >> parse_and_load_news_operator
