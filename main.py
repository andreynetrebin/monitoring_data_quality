import logging
import time
from os import getcwd, path
from utils.logger import setup_logger
from database.db_connection import DatabaseConnection
from database.db_operations import DBOperations
from database.queries import master_opening_ils, master_closing_ils, historical_opening_ils, historical_closing_ils, \
    historical_opening_ils_portions, historical_closing_ils_portions
from import_data.data_loader import load_csv_to_table
from export_data.csv_export import export_data_to_csv_with_copy, export_data_to_csv, \
    from_netezza_export_to_csv_with_offset
from config import load_db_config
from utils.file_utils import create_directory, clear_directory


def main():
    # Настройка логирования
    setup_logger()

    # Загрузка конфигурации
    config = load_db_config()
    cur_dir_path = getcwd()

    table_master_opening_ils = 'master_opening_ils'
    table_historical_opening_ils = 'historical_opening_ils'
    table_master_closing_ils = 'master_closing_ils'
    table_historical_closing_ils = 'historical_closing_ils'

    # Подключение к базе мастер-системы (Greenplum)
    master_conn = DatabaseConnection(config['master_db'])
    logging.info("Подключение к мастер-системе установлено.")

    # 1. Выгрузка данных из базы мастер-системы
    start_time_export = time.time()
    with master_conn:
        with master_conn.get_cursor() as cur:
            opening_export_success = export_data_to_csv_with_copy(cur,
                                                                  f"COPY ({master_opening_ils}) TO STDOUT WITH CSV HEADER DELIMITER ';'",
                                                                  f'{table_master_opening_ils}.csv')
            closing_export_success = export_data_to_csv_with_copy(cur,
                                                                  f"COPY ({master_closing_ils}) TO STDOUT WITH CSV HEADER DELIMITER ';'",
                                                                  f'{table_master_closing_ils}.csv')
    # Проверка успешного экспорта файлов
    if opening_export_success and closing_export_success:
        end_time_export = time.time()  # Конец измерения времени
        logging.info(
            f"Выгрузка данных из мастер-системы завершена. Время выполнения: {end_time_export - start_time_export:.2f} секунд.")

        # Подключение к базе системы мониторинга (PostgreSQL)
        monitoring_conn = DatabaseConnection(config['monitoring_db'])
        monitoring_db_type = config['monitoring_db']['type']
        logging.info("Подключение к целевой системе мониторинга установлено.")

        # Инициализация операций с базой данных мониторинга
        db_ops = DBOperations(monitoring_conn, db_type=monitoring_db_type)

        with monitoring_conn:
            with monitoring_conn.get_cursor() as cur:
                # 2. Загрузка данных в базу системы мониторинга
                start_time_load = time.time()

                # Создание таблицы и загрузка данных по открытым лицевым счетам
                columns_master_opening_ils = {
                    'snils': 'VARCHAR(14)',
                    'acc_id': 'BIGINT',
                    'opening_date': 'DATE',
                    'opening_region': 'VARCHAR(6)',
                    'registration_reason': 'TEXT',
                }
                index_columns = ['acc_id']
                db_ops.drop_table(table_master_opening_ils)
                db_ops.create_postgresql_table(table_master_opening_ils, columns_master_opening_ils, index_columns)
                load_csv_to_table(cur, f"{table_master_opening_ils}.csv", table_master_opening_ils)

                # Создание таблицы и загрузка данных по закрытым лицевым счетам
                columns_master_closing_ils = {
                    'snils': 'VARCHAR(14)',
                    'acc_id': 'BIGINT',
                    'death_date': 'DATE',
                    'closing_region': 'VARCHAR(6)',
                    'closing_reason': 'TEXT',
                }
                index_columns = ['acc_id']  # Укажите столбцы для индексации
                db_ops.drop_table(table_master_closing_ils)
                db_ops.create_postgresql_table(table_master_closing_ils, columns_master_closing_ils, index_columns)
                load_csv_to_table(cur, f'{table_master_closing_ils}.csv', table_master_closing_ils)

                monitoring_conn.commit()
                end_time_load = time.time()
                logging.info(
                    f"Данные из мастер-системы успешно загружены в базу системы мониторинга. Время выполнения: {end_time_load - start_time_load:.2f} секунд.")

                # 3. Извлечение идентификаторов из базы мониторинга
                start_time_extract_ids = time.time()

                # Извлечение идентификаторов acc_id из таблицы master_opening_ils
                query1 = f'SELECT DISTINCT acc_id FROM {table_master_opening_ils};'
                db_ops.execute_query(query1, csv_file="ids_opening_ils.csv")

                # Извлечение идентификаторов acc_id из таблицы master_closing_ils
                query2 = f'SELECT DISTINCT acc_id FROM {table_master_closing_ils};'
                db_ops.execute_query(query2, csv_file="ids_closing_ils.csv")

                monitoring_conn.commit()
                end_time_extract_ids = time.time()
                logging.info(
                    f"Идентификаторы извлечены в csv-файлы: ids_opening_ils.csv, ids_closing_ils.csv. Время выполнения: {end_time_extract_ids - start_time_extract_ids:.2f} секунд.")

                # Подключение к базе исторической системы (Netezza)
                historical_conn = DatabaseConnection(config['historical_db'])
                logging.info("Подключение к базе исторической системы установлено.")
                historical_db_type = config['historical_db']['type']

                db_historical_ops = DBOperations(historical_conn, db_type=historical_db_type)

                # 4. Загрузка идентификаторов в историческую систему
                start_time_load_ids = time.time()
                with historical_conn:
                    with historical_conn.get_cursor() as cur:
                        prefix_table_name = 'vlg_mic'
                        columns_ids = {'acc_id': 'BIGINT'}
                        distribute_columns_ids = 'acc_id'

                        # Загрузка в Netezza идентификаторов по открытым лицевым счетам
                        table_ids_opening_ils = f'{prefix_table_name}_ids_opening_ils'
                        csv_external_opening_ids = path.join(cur_dir_path, 'ids_opening_ils.csv')
                        db_historical_ops.drop_table(table_ids_opening_ils)
                        db_historical_ops.create_netezza_table(table_ids_opening_ils, columns_ids,
                                                               distribute_columns_ids)
                        db_historical_ops.insert_to_netezza_from_select_external_csv(table_ids_opening_ils,
                                                                                     csv_external_opening_ids)

                        # Загрузка в Netezza идентификаторов по закрытым лицевым счетам
                        table_ids_closing_ils = f'{prefix_table_name}_ids_closing_ils'
                        csv_external_closing_ids = path.join(cur_dir_path, 'ids_closing_ils.csv')
                        db_historical_ops.drop_table(table_ids_closing_ils)
                        db_historical_ops.create_netezza_table(table_ids_closing_ils, columns_ids,
                                                               distribute_columns_ids)
                        db_historical_ops.insert_to_netezza_from_select_external_csv(table_ids_closing_ils,
                                                                                     csv_external_closing_ids)

                        end_time_load_ids = time.time()
                        logging.info(
                            f"Загружены идентификаторы в базу исторической системы. Время выполнения: {end_time_load_ids - start_time_load_ids:.2f} секунд.")

                        # 5. Выгрузка данных из исторической системы
                        # Выгрузка порциями в csv-файлы
                        start_time_create_historical_tables = time.time()  # Начало измерения времени

                        # Создание таблицы с данными из базы исторической системы по открытым лицевым счетам
                        table_historical_opening_ils = f'{prefix_table_name}_historical_opening_ils'
                        db_historical_ops.drop_table(table_historical_opening_ils)
                        db_historical_ops.create_netezza_table_from_select(historical_opening_ils,
                                                                           table_historical_opening_ils,
                                                                           distribute_columns_ids)
                        # Создание таблицы с данными из базы исторической системы по закрытым лицевым счетам
                        table_historical_closing_ils = f'{prefix_table_name}_historical_closing_ils'
                        db_historical_ops.drop_table(table_historical_closing_ils)
                        db_historical_ops.create_netezza_table_from_select(historical_closing_ils,
                                                                           table_historical_closing_ils,
                                                                           distribute_columns_ids)

                        end_time_create_historical_tables = time.time()
                        logging.info(
                            f"Созданы таблицы с данными из исторической системы. Время выполнения: {end_time_create_historical_tables - start_time_create_historical_tables:.2f} секунд.")

                        directory_csv_portions_opening_ils = path.join(cur_dir_path, table_historical_opening_ils)
                        create_directory(directory_csv_portions_opening_ils)
                        # Очистка директории по открытым лицевым счетам
                        clear_directory(directory_csv_portions_opening_ils)
                        # Извлечение данных из Netezza порциями по открытым лицевым счетам
                        from_netezza_export_to_csv_with_offset(cur, historical_opening_ils_portions,
                                                               directory_csv_portions_opening_ils,
                                                               table_historical_opening_ils)

                        directory_csv_portions_closing_ils = path.join(cur_dir_path, table_historical_closing_ils)
                        create_directory(directory_csv_portions_closing_ils)
                        # Очистка директории по закрытым лицевым счетам
                        clear_directory(directory_csv_portions_closing_ils)
                        # Извлечение данных из Netezza порциями по закрытым лицевым счетам
                        from_netezza_export_to_csv_with_offset(cur, historical_closing_ils_portions,
                                                               directory_csv_portions_closing_ils,
                                                               table_historical_closing_ils)

                        end_time_create_historical_tables = time.time()
                        logging.info(
                            f"Выгружены порциями данные из исторической системы. Время выполнения: {end_time_create_historical_tables - start_time_create_historical_tables:.2f} секунд.")
    else:
        logging.error(f"Ошибка: Не удалось экспортировать из базы один или оба файла.")

    logging.info("Работа завершена!")


if __name__ == "__main__":
    main()
