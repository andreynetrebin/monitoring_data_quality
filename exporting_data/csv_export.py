import logging
import csv
import pandas as pd
import time
from os import path
from database.db_connection import DatabaseConnection
from database.queries import master_opening_ils, master_closing_ils, historical_opening_ils, historical_closing_ils, \
    historical_opening_ils_portions, historical_closing_ils_portions
from utils.file_utils import clear_directory, create_directory

# Создание директории temp_data, если она не существует
temp_data_dir = 'temp_data'
create_directory(temp_data_dir)


def export_data_to_csv_with_copy(cur, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с использованием COPY."""
    try:
        csv_file_path = path.join(temp_data_dir, csv_file)  # Полный путь к файлу
        with open(csv_file_path, 'w', newline='') as csvfile:
            cur.copy_expert(query, csvfile)
        logging.info(f"Данные выгружены в {csv_file_path}")
        return True  # Возвращаем True при успешном экспорте
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")
        return False  # Возвращаем False при ошибке


def export_data_to_csv(cur, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с ручной записью заголовков и данных."""
    try:
        csv_file_path = path.join(temp_data_dir, csv_file)  # Полный путь к файлу
        with open(csv_file_path, 'w', newline='') as csvfile:
            cur.execute(query)
            # Получаем названия колонок
            colnames = [desc[0] for desc in cur.description]
            # Записываем заголовки в CSV
            csvfile.write(';'.join(colnames) + '\n')
            # Записываем данные в CSV
            for row in cur.fetchall():
                csvfile.write(';'.join(map(str, row)) + '\n')
            logging.info(f"Данные выгружены в {csv_file_path}")
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")


def load_acc_ids_from_csv(file_path, encoding='utf-8'):
    """Загрузка acc_id из CSV-файла с указанием кодировки."""
    df = pd.read_csv(file_path, encoding=encoding, sep=';')
    return df['acc_id'].tolist()  # Предполагается, что колонка называется 'acc_id'


# Объединение двух CSV-файлов по acc_id
def merge_csv_files(file1, file2, output_file, encoding='utf-8'):
    """Объединение двух CSV-файлов по acc_id с учетом отсутствующих значений."""
    df1 = pd.read_csv(file1, encoding=encoding, sep=';')  # Загрузка первого файла
    df2 = pd.read_csv(file2)  # Загрузка второго файла

    # Объединение по acc_id с использованием left join
    merged_df = pd.merge(df1, df2, on='acc_id', how='left')  # Используем left join

    # Сохранение объединенных данных в новый CSV-файл
    output_file_path = path.join(temp_data_dir, output_file)  # Полный путь к выходному файлу
    merged_df.to_csv(output_file_path, index=False, sep=';', encoding=encoding)  # Указываем разделитель
    logging.info(f"Объединенные данные сохранены в '{output_file_path}'.")


def export_results_to_csv(results, csv_file):
    """Экспорт результатов выполнения запроса в CSV файл."""
    try:
        csv_file_path = path.join(temp_data_dir, csv_file)  # Полный путь к файлу
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            # Записываем данные в CSV
            writer.writerows(results)
        logging.info(f"Результаты выгружены в {csv_file_path}")
    except Exception as e:
        logging.error(f"Ошибка при выгрузке результатов в CSV: {e}")


def from_netezza_export_to_csv_with_offset(cur, base_query, directory, output_file_base, batch_size=20000):
    """Экспорт данных из Netezza в CSV с использованием CREATE EXTERNAL TABLE и смещения."""
    try:
        offset = 0
        while True:
            # Формирование полного SQL-запроса с учетом смещения
            time.sleep(3)
            query = f"{base_query} WHERE rn > {offset} AND rn <= {offset + batch_size};"

            # Создание внешней таблицы для выгрузки
            external_table_query = f"""
            CREATE EXTERNAL TABLE '{directory}\\{output_file_base}_{offset}.csv'
            USING (
                IncludeHeader   
                DELIMITER ';'
                ENCODING 'internal'
                REMOTESOURCE 'ODBC'
                ESCAPECHAR '\\'
            ) AS
            {query};
            """

            # Выполнение запроса на создание внешней таблицы и выгрузку данных
            cur.execute(external_table_query)

            # Проверка количества выгруженных строк
            rows_affected = cur.rowcount
            logging.info(f"Выгружено строк: {rows_affected} (offset: {offset})")

            # Увеличение смещения
            offset += batch_size

            # Если выгружено меньше, чем batch_size, значит, данные закончились
            if rows_affected < batch_size:
                break
        return True
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")
        return False  # Возвращаем False при ошибке


def export_data_from_master(config, account_type):
    """Выгрузка данных из базы мастер-системы."""
    master_conn = DatabaseConnection(config['master_db'])
    logging.info("Подключение к мастер-системе установлено.")

    csv_filename = f'master_{account_type}_ils.csv'
    query = master_opening_ils if account_type == 'opening' else master_closing_ils

    start_time = time.time()
    with master_conn:
        with master_conn.get_cursor() as cur:
            export_result = export_data_to_csv_with_copy(cur,
                                                         f"COPY ({query}) TO STDOUT WITH CSV HEADER DELIMITER ';'",
                                                         csv_filename)

    if export_result:
        end_time = time.time()
        logging.info(
            f"Выгрузка данных из мастер-системы по {account_type} завершена. Время выполнения: {end_time - start_time:.2f} секунд.")
        return csv_filename
    else:
        end_time = time.time()
        logging.error(
            f"Не удалось выгрузить данные из мастер-системы по {account_type}. Время выполнения: {end_time - start_time:.2f} секунд.")
        return None


def export_ids_from_monitoring(config):
    """Извлечение идентификаторов из базы мониторинга."""
    start_time = time.time()
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Переместите импорт DBOperations сюда
    from database.db_operations import DBOperations
    db_ops = DBOperations(monitoring_conn, db_type=config['monitoring_db']['type'])

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            for account_type in ['opening', 'closing']:
                csv_filename = f"ids_{account_type}_ils.csv"  # Имя файла
                query = f'SELECT DISTINCT acc_id FROM master_{account_type}_ils;'
                export_result = db_ops.execute_query(query, csv_file=csv_filename)

                if export_result is None:
                    logging.error(f"Не удалось извлечь идентификаторы для {account_type}.")
                    return False

    end_time = time.time()
    logging.info(f"Идентификаторы извлечены в csv-файлы. Время выполнения: {end_time - start_time:.2f} секунд.")
    return True


def export_data_from_historical(config, cur_dir_path):
    """Выгрузка данных из исторической системы."""
    start_time = time.time()
    historical_conn = DatabaseConnection(config['historical_db'])
    logging.info("Подключение к базе исторической системы установлено.")

    # Переместите импорт DBOperations сюда
    from database.db_operations import DBOperations
    db_historical_ops = DBOperations(historical_conn, db_type=config['historical_db']['type'])

    prefix_table_name = 'vlg_mic'

    with historical_conn:
        with historical_conn.get_cursor() as cur:
            for account_type in ['opening', 'closing']:
                table_historical = f"{prefix_table_name}_historical_{account_type}_ils"
                db_historical_ops.drop_table(table_historical)
                db_historical_ops.create_netezza_table_from_select(
                    historical_opening_ils if account_type == 'opening' else historical_closing_ils,
                    table_historical, 'acc_id'
                )

                directory_csv_portions = path.join(cur_dir_path, temp_data_dir, f"{table_historical}")
                create_directory(directory_csv_portions)
                clear_directory(directory_csv_portions)

                export_result = from_netezza_export_to_csv_with_offset(cur,
                                                                       historical_opening_ils_portions if account_type == 'opening' else historical_closing_ils_portions,
                                                                       directory_csv_portions, table_historical
                                                                       )

                if not export_result:
                    logging.error(f"Не удалось выгрузить данные для {account_type} из исторической системы.")
                    return False

    end_time = time.time()
    logging.info(
        f"Выгружены данные в csv из исторической системы. Время выполнения: {end_time - start_time:.2f} секунд.")
    return True
