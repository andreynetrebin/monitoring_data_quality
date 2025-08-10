import logging
from database.db_schema import create_table_if_not_exists, drop_table_if_exists, create_netezza_table, \
    create_netezza_table_from_select
from exporting_data.csv_export import export_results_to_csv


class DBOperations:
    def __init__(self, connection, db_type=None):
        self.connection = connection
        self.db_type = db_type

    def execute_query(self, query, csv_file=None):
        """Выполнение SQL-запроса и, при необходимости, экспорт результатов в CSV."""
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            cur.execute(query)
            results = cur.fetchall()  # Получаем результаты запроса
            if csv_file:
                # Если указан файл для экспорта, экспортируем данные
                export_results_to_csv(results, csv_file)
            return results

    def create_postgresql_table(self, table_name, columns, index_columns=None):
        """
        Создание таблицы, если она не существует.
        :param table_name: Имя создаваемой таблицы.
        :param columns: Словарь с именами столбцов и их типами данных.
        :param index_columns: Список столбцов для создания индекса (по умолчанию None).
        """
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            create_table_if_not_exists(cur, table_name, columns, index_columns)
            self.connection.commit()

    def create_netezza_table(self, table_name, columns, distribute_column):
        """
        Создание таблицы в Netezza.
        :param table_name: Имя создаваемой таблицы.
        :param columns: Словарь с именами столбцов и их типами данных.
        :param distribute_column: Имя столбца для распределения.
        """
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            create_netezza_table(cur, table_name, columns, distribute_column)
            self.connection.commit()

    def create_netezza_table_from_select(self, select_query, table_name, distribute_column):
        """
        Создание таблицы в Netezza.
        :select_query: Запрос для формирвания таблицы
        :param table_name: Имя создаваемой таблицы.
        :param distribute_column: Имя столбца для распределения.
        """
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            create_netezza_table_from_select(cur, select_query, table_name, distribute_column)
            self.connection.commit()

    def drop_table(self, table_name):
        """
        Удаление таблицы, если она существует.
        :param table_name: Имя таблицы, которую нужно удалить.
        """
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            drop_table_if_exists(cur, table_name, self.db_type)
            self.connection.commit()

    def clear_table(self, table_name):
        """
        Очистка таблицы.
        :param table_name: Имя таблицы, которую нужно очистить.
        """
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            cur.execute(f"TRUNCATE {table_name};")
            self.connection.commit()

    def insert_to_netezza_from_select_external_csv(self, table_name, external_csv):
        """
        Загрузка данных в Netezza из запроса SELECT.
        :param table_name: Имя таблицы, в которую будут загружены данные.
        :param external_csv: Имя внешнего csv-файла.
        """
        try:
            with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
                insert_query = f"""
                INSERT INTO {table_name}
                SELECT * FROM 
                EXTERNAL '{external_csv}'
                USING
                (
                    Y2BASE 2000
                    ENCODING 'internal'
                    REMOTESOURCE 'ODBC'
                    ESCAPECHAR '\\'
                );
                """
                cur.execute(insert_query)
                self.connection.commit()
                logging.info(f"Данные успешно загружены в таблицу {table_name} из внешнего файла {external_csv}.")
                return True
        except Exception as e:
            logging.error(
                f"Данные не загружены в таблицу {table_name} из внешнего файла {external_csv}. Возникла ошибка - {e}")
            return False

    def count_total_records(self, table_name):
        """Подсчет общего количества записей в таблице."""
        query = f"SELECT COUNT(*) FROM {table_name};"
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            cur.execute(query)
            return cur.fetchone()[0]

    def count_records(self, query):
        """Подсчет количества записей по заданному запросу."""
        with self.connection.get_cursor() as cur:  # Изменено на get_cursor()
            cur.execute(query)
            return cur.rowcount

    def insert_data(self, table_name, columns, values):
        """Универсальный метод для вставки данных в таблицу."""
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(values))
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        with self.connection.get_cursor() as cur:
            cur.execute(query, values)
            self.connection.commit()  # Подтверждение изменений

    def insert_check_result(self, check_description, record_count):
        """Вставка результата проверки в таблицу data_check_results."""
        self.insert_data("data_check_results", ["check_description", "record_count"], [check_description, record_count])
