import logging
import time
from datetime import datetime
from os import getcwd, path, listdir
from utils.logger import setup_logger
from database.db_connection import DatabaseConnection
from database.db_operations import DBOperations
from database.queries import master_opening_ils, master_closing_ils, historical_opening_ils, historical_closing_ils, \
    historical_opening_ils_portions, historical_closing_ils_portions, part_query_integrating, check1_query, \
    check_query2, check_query3, check_query4, check_query5, check_query6, check_query7
from import_data.data_loader import load_csv_to_table
from export_data.csv_export import export_data_to_csv_with_copy, export_data_to_csv, \
    from_netezza_export_to_csv_with_offset, load_acc_ids_from_csv, merge_csv_files
from config import load_db_config
from utils.file_utils import create_directory, clear_directory
from export_data.report_export import perform_check_and_export


def export_data_from_master(config):
    # 1. Выгрузка данных из базы мастер-системы

    # Подключение к базе мастер-системы (Greenplum)
    master_conn = DatabaseConnection(config['master_db'])
    logging.info("Подключение к мастер-системе установлено.")
    csv_filename_opening_ils = 'master_opening_ils.csv'
    csv_filename_closing_ils = 'master_closing_ils.csv'

    start_time = time.time()
    with master_conn:
        with master_conn.get_cursor() as cur:
            opening_export_result = export_data_to_csv_with_copy(cur,
                                                                 f"COPY ({master_opening_ils}) TO STDOUT WITH CSV HEADER DELIMITER ';'",
                                                                 csv_filename_opening_ils)
            closing_export_result = export_data_to_csv_with_copy(cur,
                                                                 f"COPY ({master_closing_ils}) TO STDOUT WITH CSV HEADER DELIMITER ';'",
                                                                 csv_filename_closing_ils)
    # Проверка успешного экспорта файлов
    if opening_export_result and closing_export_result:
        end_time = time.time()  # Конец измерения времени
        logging.info(
            f"Выгрузка данных из мастер-системы завершена. Время выполнения: {end_time - start_time:.2f} секунд.")
        return True
    else:
        end_time = time.time()  # Конец измерения времени
        logging.info(
            f"Один или оба массива не выгружены из мастер-системы. Время выполнения: {end_time - start_time:.2f} секунд.")
        return False


def import_data_from_master_to_monitoring(config):
    # 2. Загрузка данных в базу системы мониторинга
    start_time = time.time()

    monitoring_table_master_opening_ils = 'master_opening_ils'
    monitoring_table_master_closing_ils = 'master_closing_ils'

    # Подключение к базе системы мониторинга (PostgreSQL)
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    monitoring_db_type = config['monitoring_db']['type']
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Инициализация операций с базой данных мониторинга
    db_ops = DBOperations(monitoring_conn, db_type=monitoring_db_type)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            # Создание таблицы и загрузка данных по открытым лицевым счетам
            columns_master_opening_ils = {
                'snils': 'VARCHAR(14)',
                'acc_id': 'BIGINT',
                'opening_date': 'DATE',
                'opening_region': 'VARCHAR(6)',
                'registration_reason': 'TEXT',
            }
            index_columns = ['acc_id']
            db_ops.drop_table(monitoring_table_master_opening_ils)
            db_ops.create_postgresql_table(monitoring_table_master_opening_ils, columns_master_opening_ils,
                                           index_columns)
            opening_import_result = load_csv_to_table(cur, f"{monitoring_table_master_opening_ils}.csv",
                                                      monitoring_table_master_opening_ils)

            # Создание таблицы и загрузка данных по закрытым лицевым счетам
            columns_master_closing_ils = {
                'snils': 'VARCHAR(14)',
                'acc_id': 'BIGINT',
                'death_date': 'DATE',
                'closing_region': 'VARCHAR(6)',
                'closing_reason': 'TEXT',
            }
            index_columns = ['acc_id']  # Укажите столбцы для индексации
            db_ops.drop_table(monitoring_table_master_closing_ils)
            db_ops.create_postgresql_table(monitoring_table_master_closing_ils, columns_master_closing_ils,
                                           index_columns)
            closing_import_result = load_csv_to_table(cur, f'{monitoring_table_master_closing_ils}.csv',
                                                      monitoring_table_master_closing_ils)

            # Проверка успешного экспорта файлов
            if opening_import_result and closing_import_result:
                monitoring_conn.commit()
                end_time = time.time()  # Конец измерения времени
                logging.info(
                    f"Импорт данных из мастер-системы в систему мониторинга завершен. Время выполнения: {end_time - start_time:.2f} секунд.")
                return True
            else:
                end_time = time.time()  # Конец измерения времени
                logging.info(
                    f"Один или оба массива не импортированы. Время выполнения: {end_time - start_time:.2f} секунд.")
                return False


def export_ids_from_monitoring(config):
    # 3. Извлечение идентификаторов из базы мониторинга

    start_time = time.time()
    monitoring_table_master_opening_ils = 'master_opening_ils'
    monitoring_table_master_closing_ils = 'master_closing_ils'

    # Подключение к базе системы мониторинга (PostgreSQL)
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    monitoring_db_type = config['monitoring_db']['type']
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Инициализация операций с базой данных мониторинга
    db_ops = DBOperations(monitoring_conn, db_type=monitoring_db_type)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:

            # Извлечение идентификаторов acc_id из таблицы master_opening_ils
            query1 = f'SELECT DISTINCT acc_id FROM {monitoring_table_master_opening_ils};'
            opening_export_result = db_ops.execute_query(query1, csv_file="ids_opening_ils.csv")

            # Извлечение идентификаторов acc_id из таблицы master_closing_ils
            query2 = f'SELECT DISTINCT acc_id FROM {monitoring_table_master_closing_ils};'
            opening_export_result = db_ops.execute_query(query2, csv_file="ids_closing_ils.csv")

            if opening_export_result is not None and closing_export_result is not None:
                end_time = time.time()
                logging.info(
                    f"Идентификаторы извлечены в csv-файлы. Время выполнения: {end_time - start_time:.2f} секунд.")
                return True
            else:
                end_time = time.time()
                logging.info(
                    f"Один или оба списка идентификаторов не извлечены в csv-файлы. Время выполнения: {end_time - start_time:.2f} секунд.")
                return False


def import_data_to_historical(config, cur_dir_path):
    # 4. Загрузка идентификаторов в историческую систему
    start_time = time.time()

    monitoring_table_historical_opening_ils = 'historical_opening_ils'
    monitoring_table_historical_closing_ils = 'historical_closing_ils'

    prefix_table_name = 'vlg_mic'

    # Подключение к базе исторической системы (Netezza)
    historical_conn = DatabaseConnection(config['historical_db'])
    logging.info("Подключение к базе исторической системы установлено.")
    historical_db_type = config['historical_db']['type']

    db_historical_ops = DBOperations(historical_conn, db_type=historical_db_type)

    with historical_conn:
        with historical_conn.get_cursor() as cur:
            columns_ids = {'acc_id': 'BIGINT'}
            distribute_columns_ids = 'acc_id'

            # Загрузка в Netezza идентификаторов по открытым лицевым счетам
            table_ids_opening_ils = f'{prefix_table_name}_ids_opening_ils'
            csv_external_opening_ids = path.join(cur_dir_path, 'ids_opening_ils.csv')
            db_historical_ops.drop_table(table_ids_opening_ils)
            db_historical_ops.create_netezza_table(table_ids_opening_ils, columns_ids,
                                                   distribute_columns_ids)
            opening_import_result = db_historical_ops.insert_to_netezza_from_select_external_csv(table_ids_opening_ils,
                                                                                                 csv_external_opening_ids)

            # Загрузка в Netezza идентификаторов по закрытым лицевым счетам
            table_ids_closing_ils = f'{prefix_table_name}_ids_closing_ils'
            csv_external_closing_ids = path.join(cur_dir_path, 'ids_closing_ils.csv')
            db_historical_ops.drop_table(table_ids_closing_ils)
            db_historical_ops.create_netezza_table(table_ids_closing_ils, columns_ids,
                                                   distribute_columns_ids)
            closing_import_result = db_historical_ops.insert_to_netezza_from_select_external_csv(table_ids_closing_ils,
                                                                                                 csv_external_closing_ids)

            if opening_import_result and closing_import_result:
                end_time = time.time()
                logging.info(
                    f"Идентификаторы загружены в историческую систему. Время выполнения: {end_time - start_time:.2f} секунд.")
                return True
            else:
                end_time = time.time()
                logging.info(
                    f"Один или оба списка идентификаторов не загружены в историческую систему. Время выполнения: {end_time - start_time:.2f} секунд.")
                return False


def export_data_from_historical(config, cur_dir_path):
    # 5. Выгрузка данных из исторической системы
    start_time = time.time()
    prefix_table_name = 'vlg_mic'

    monitoring_table_historical_opening_ils = 'historical_opening_ils'
    monitoring_table_historical_closing_ils = 'historical_closing_ils'
    table_historical_opening_ils = f"{prefix_table_name}_{monitoring_table_historical_opening_ils}"
    table_historical_closing_ils = f"{prefix_table_name}_{monitoring_table_historical_closing_ils}"

    # Подключение к базе исторической системы (Netezza)
    historical_conn = DatabaseConnection(config['historical_db'])
    logging.info("Подключение к базе исторической системы установлено.")
    historical_db_type = config['historical_db']['type']

    db_historical_ops = DBOperations(historical_conn, db_type=historical_db_type)

    with historical_conn:
        with historical_conn.get_cursor() as cur:
            distribute_columns_ids = 'acc_id'
            # Создание таблицы с данными из базы исторической системы по открытым лицевым счетам
            db_historical_ops.drop_table(table_historical_opening_ils)
            db_historical_ops.create_netezza_table_from_select(historical_opening_ils,
                                                               table_historical_opening_ils,
                                                               distribute_columns_ids)
            # Создание таблицы с данными из базы исторической системы по закрытым лицевым счетам
            db_historical_ops.drop_table(table_historical_closing_ils)
            db_historical_ops.create_netezza_table_from_select(historical_closing_ils,
                                                               table_historical_closing_ils,
                                                               distribute_columns_ids)

            end_time_create_historical_tables = time.time()
            logging.info(
                f"Созданы таблицы с данными из исторической системы. Время выполнения: {end_time_create_historical_tables - start_time_create_historical_tables:.2f} секунд.")

            directory_csv_portions_opening_ils = path.join(cur_dir_path, f"{table_historical_opening_ils}")
            create_directory(directory_csv_portions_opening_ils)
            # Очистка директории по открытым лицевым счетам
            clear_directory(directory_csv_portions_opening_ils)
            # Извлечение данных из Netezza порциями по открытым лицевым счетам
            opening_export_result = from_netezza_export_to_csv_with_offset(cur, historical_opening_ils_portions,
                                                                           directory_csv_portions_opening_ils,
                                                                           table_historical_opening_ils)

            directory_csv_portions_closing_ils = path.join(cur_dir_path, f"{table_historical_closing_ils}")
            create_directory(directory_csv_portions_closing_ils)
            # Очистка директории по закрытым лицевым счетам
            clear_directory(directory_csv_portions_closing_ils)
            # Извлечение данных из Netezza порциями по закрытым лицевым счетам
            closing_export_result = from_netezza_export_to_csv_with_offset(cur, historical_closing_ils_portions,
                                                                           directory_csv_portions_closing_ils,
                                                                           table_historical_closing_ils)

            if opening_export_result and closing_export_result:
                end_time = time.time()
                logging.info(
                    f"Выгружены данные в csv из исторической системы. Время выполнения: {end_time - start_time:.2f} секунд.")
                return True
            else:
                end_time = time.time()
                logging.info(
                    f"Не выгружены один или оба массива данных из исторической системы. Время выполнения: {end_time - start_time:.2f} секунд.")
                return False


def import_data_from_historical_to_monitoring(config, cur_dir_path):
    # 6. Загрузка в систему мониторинга, данных полученных из исторической системы.
    start_time = time.time()
    prefix_table_name = 'vlg_mic'
    monitoring_table_historical_opening_ils = 'historical_opening_ils'
    monitoring_table_historical_closing_ils = 'historical_closing_ils'

    table_historical_opening_ils = f"{prefix_table_name}_{monitoring_table_historical_opening_ils}"
    table_historical_closing_ils = f"{prefix_table_name}_{monitoring_table_historical_closing_ils}"

    directory_csv_portions_opening_ils = path.join(cur_dir_path, f"{table_historical_opening_ils}")
    directory_csv_portions_closing_ils = path.join(cur_dir_path, f"{table_historical_closing_ils}")

    # Подключение к базе системы мониторинга (PostgreSQL)
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    monitoring_db_type = config['monitoring_db']['type']
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Инициализация операций с базой данных мониторинга
    db_ops = DBOperations(monitoring_conn, db_type=monitoring_db_type)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            csv_files_portions_opening_ils = [f for f in listdir(directory_csv_portions_opening_ils) if
                                              f.endswith('.csv')]
            # создание таблицы под данные об открытых лицевых счетах из исторической системы
            columns_historical_opening_ils = {
                'acc_id': 'BIGINT',
                'acc_sts': 'INT',
                'opening_region': 'VARCHAR(6)',
            }
            index_columns = ['acc_id']  # Укажите столбцы для индексации

            db_ops.drop_table(monitoring_table_historical_opening_ils)
            db_ops.create_postgresql_table(monitoring_table_historical_opening_ils, columns_historical_opening_ils,
                                           index_columns)

            for csv_file in csv_files_portions_opening_ils:
                # Формирование полного пути к CSV файлу
                csv_file_path = path.join(directory_csv_portions_opening_ils, csv_file)
                load_csv_to_table(cur, csv_file_path, monitoring_table_historical_opening_ils)
            monitoring_conn.commit()
            logging.info(f"Загружены данные в базу мониторинга в таблицу - {monitoring_table_historical_opening_ils}")

            csv_files_portions_closing_ils = [f for f in listdir(directory_csv_portions_closing_ils) if
                                              f.endswith('.csv')]
            # создание таблицы под данные об открытых лицевых счетах из исторической системы
            columns_historical_closing_ils = {
                'acc_id': 'BIGINT',
                'acc_sts': 'INT',
                'death_date': 'DATE',
                'closing_region': 'VARCHAR(6)',
            }
            index_columns = ['acc_id']  # Укажите столбцы для индексации
            db_ops.drop_table(monitoring_table_historical_closing_ils)
            db_ops.create_postgresql_table(monitoring_table_historical_closing_ils, columns_historical_closing_ils,
                                           index_columns)

            for csv_file in csv_files_portions_closing_ils:
                # Формирование полного пути к CSV файлу
                csv_file_path = path.join(directory_csv_portions_closing_ils, csv_file)
                load_csv_to_table(cur, csv_file_path, monitoring_table_historical_closing_ils)

            monitoring_conn.commit()
            logging.info(f"Загружены данные в базу мониторинга в таблицу - {monitoring_table_historical_closing_ils}")
        end_time = time.time()
        logging.info(
            f"Время выполнения: {end_time - start_time:.2f} секунд.")
        return True


def perform_checks_data(config):
    # 7. Проведение проверок и формирование отчета

    start_time = time.time()

    monitoring_table_master_opening_ils = 'master_opening_ils'
    monitoring_table_master_closing_ils = 'master_closing_ils'
    # Подключение к базе системы мониторинга (PostgreSQL)
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    monitoring_db_type = config['monitoring_db']['type']
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Инициализация операций с базой данных мониторинга
    db_ops = DBOperations(monitoring_conn, db_type=monitoring_db_type)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            integrating_conn = DatabaseConnection(config['integrating_db'])
            integrating_db_type = config['integrating_db']['type']
            logging.info("Подключение к интеграционной базе установлено.")

            # Инициализация операций с интеграционной базой
            db_ops_integrating = DBOperations(integrating_conn, db_type=integrating_db_type)

            with integrating_conn:
                with integrating_conn.get_cursor() as cur_integrating:
                    # Создание таблицы для хранения проверок
                    columns_data_check_results = {
                        'id': 'SERIAL PRIMARY KEY',
                        'check_description': 'TEXT',
                        'record_count': 'INT',
                        'created_at': 'DATE DEFAULT CURRENT_DATE',
                    }
                    index_columns = ['id']
                    db_ops.create_postgresql_table('data_check_results', columns_data_check_results, index_columns)

                    report_lines = []  # Список для хранения строк отчета
                    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        # Подсчет общего количества записей в таблицах
                        total_opening_records = db_ops.count_total_records(monitoring_table_master_opening_ils)
                        total_closing_records = db_ops.count_total_records(monitoring_table_master_closing_ils)
                        report_lines.append(
                            "Проверка проводилась по данным полученным из ЕЦП ХОАД в сравнение с их состоянием в исторической системе (СПУ)")
                        report_lines.append("по открытым лицевым счетам после 12.05.2024 и умершим после 12.05.2024")
                        report_lines.append(f"Количество ИЛС открытых: {total_opening_records}")
                        report_lines.append(f"Количество ИЛС умерших: {total_closing_records}")
                        report_lines.append("=" * 30)

                        db_ops.insert_check_result("Количество ИЛС открытых", total_opening_records)
                        db_ops.insert_check_result("Количество ИЛС умерших", total_closing_records)

                        # Выполнение проверок
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check1_query,
                                                 'отсутствующие_открытые_лицевые_счета', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query2,
                                                 'закрытые_открытые_лицевые_счета', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query3,
                                                 'другой_регион_открытия_лицевых_счетов', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query4,
                                                 'отсутствующие_закрытые_лицевые_счета', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query5,
                                                 'не_закрытые_закрытые_лицевые_счета', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query6,
                                                 'другой_регион_закрытия_лицевых_счетов', report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query7, 'другая_дата_смерти',
                                                 report_lines)

                        # Формирование отчета
                        report_file = 'отчет.txt'
                        with open(report_file, 'w') as f:
                            f.write("Отчет о проверках данных\n")
                            f.write("=" * 30 + "\n")
                            f.write(f"Дата формирования отчета: {report_date}\n")
                            f.write("=" * 30 + "\n")
                            for line in report_lines:
                                f.write(line + "\n")
                        logging.info(f"Отчет сохранен в '{report_file}'.")

                    except Exception as e:
                        logging.error(f"Ошибка при выполнении запросов: {e}")
                    finally:
                        # Закрытие соединения
                        monitoring_conn.close()


def main():
    # Настройка логирования
    setup_logger()

    # Загрузка конфигурации
    config = load_db_config()
    cur_dir_path = getcwd()

    if export_data_from_master(config):
        if import_data_from_master_to_monitoring(config):
            if export_ids_from_monitoring(config):
                if import_data_to_historical(config, cur_dir_path):
                    if export_data_from_historical(config, cur_dir_path):
                        if import_data_from_historical_to_monitoring(config, cur_dir_path):
                            perform_checks_data(config)

    logging.info("Работа завершена!")


if __name__ == "__main__":
    main()
