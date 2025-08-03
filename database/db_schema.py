import logging
from config import load_db_config

def create_table_if_not_exists(cur, table_name, columns, index_columns=None):
    """
    Создание таблицы, если она не существует, и индекса по указанным столбцам.
    :param cur: Курсор для выполнения SQL-запросов.
    :param table_name: Имя создаваемой таблицы.
    :param columns: Словарь с именами столбцов и их типами данных.
    :param index_columns: Список столбцов для создания индекса (по умолчанию None).
    """
    # Формирование строки с определением столбцов
    columns_definition = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])

    # SQL-запрос для создания таблицы
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_definition}
    );
    """

    try:
        cur.execute(create_table_query)
        logging.info(f"Таблица {table_name} создана или уже существует.")

        # Создание индекса, если указаны столбцы
        if index_columns:
            index_name = f"idx_{table_name}_" + "_".join(index_columns)

            # Проверка существования индекса
            check_index_query = f"""
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = '{index_name}';
            """
            cur.execute(check_index_query)
            index_exists = cur.fetchone()[0] > 0

            if not index_exists:
                create_index_query = f"""
                CREATE INDEX {index_name} ON {table_name} ({', '.join(index_columns)});
                """
                cur.execute(create_index_query)
                logging.info(f"Индекс по столбцам {', '.join(index_columns)} для таблицы {table_name} создан.")
            else:
                logging.info(f"Индекс {index_name} уже существует на таблице {table_name}.")

    except Exception as e:
        logging.error(f"Ошибка при создании таблицы или индекса: {e}")

def drop_table_if_exists(cur, table_name):
    """
    Удаление таблицы, если она существует.
    :param cur: Курсор для выполнения SQL-запросов.
    :param table_name: Имя таблицы, которую нужно удалить.
    """
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        logging.info(f"Таблица {table_name} удалена, если существовала.")
    except Exception as e:
        logging.error(f"Ошибка при удалении таблицы {table_name}: {e}")

def create_netezza_table(cur, table_name, columns, distribute_column):
    """
    Создание таблицы в Netezza.
    :param cur: Курсор для выполнения SQL-запросов.
    :param table_name: Имя создаваемой таблицы.
    :param columns: Словарь с именами столбцов и их типами данных.
    :param distribute_column: Имя столбца для распределения.
    """
    # Формирование строки с определением столбцов
    columns_definition = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])

    # SQL-запрос для создания таблицы в Netezza
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_definition}
    )
    DISTRIBUTE ON ({distribute_column});
    """

    try:
        cur.execute(create_table_query)
        logging.info(f"Таблица {table_name} создана в Netezza.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы {table_name} в Netezza: {e}")
