import logging
import psycopg2
from utils.logger import setup_logger
from database.db_operations import DBOperations
from export.csv_export import export_data_to_csv, load_csv_to_table, clear_table, create_table_if_not_exists
from config import load_db_config
from os import getcwd

def main():
    # Настройка логирования
    setup_logger()

    # Загрузка конфигурации
    config = load_db_config()
    cur_dir_path = getcwd()

    table1_name = 'master_opening_ils'
    table2_name = 'historical_opening_ils'
    table3_name = 'master_closing_ils'
    table4_name = 'historical_closing_ils'

    # Подключение к базе мастер системы
    master_conn = psycopg2.connect(**config['master_db'])
    logging.info("Подключение к мастер-системе установлено.")

    # 1. Выгрузка данных из мастер системы (Greenplum)
    export_data_to_csv(master_conn, f"COPY {table1_name} TO STDOUT WITH CSV HEADER DELIMITER ';'", f'{table1_name}.csv')
    export_data_to_csv(master_conn, f"COPY {table3_name} TO STDOUT WITH CSV HEADER DELIMITER ';'", f'{table3_name}.csv')

    master_conn.close()
    logging.info("Подключение к мастер-системе закрыто.")

    # Подключение к целевой системе мониторинга
    monitoring_conn = psycopg2.connect(**config['monitoring_db'])
    logging.info("Подключение к целевой системе мониторинга установлено.")

    # Инициализация операций с базой данных
    db_ops = DBOperations(monitoring_conn)

    # 2. Загрузка данных в систему мониторинга
    with monitoring_conn:
        with monitoring_conn.cursor() as cur:
            # Создание и загрузка данных для таблицы opening_ils
            columns_master_opening_ils = {
                'snils': 'VARCHAR(14)',
                'acc_id': 'BIGINT',
                'opening_date': 'DATE',
                'opening_region': 'VARCHAR(6)',
                'update_moment': 'TIMESTAMP',
            }
            index_columns = ['acc_id']  # Укажите столбцы для индексации
            create_table_if_not_exists(cur, table1_name, columns_master_opening_ils, index_columns)
            clear_table(cur, table1_name)
            load_csv_to_table(cur, f'{table1_name}.csv', table1_name)

            # Создание и загрузка данных для таблицы closing_ils
            columns_master_closing_ils = {
                'snils': 'VARCHAR(14)',
                'acc_id': 'BIGINT',
                'death_date': 'DATE',
                'closing_region': 'VARCHAR(6)',
                'update_moment': 'TIMESTAMP',
            }
            index_columns = ['acc_id']  # Укажите столбцы для индексации
            create_table_if_not_exists(cur, table3_name, columns_master_closing_ils, index_columns)
            clear_table(cur, table3_name)
            load_csv_to_table(cur, f'{table3_name}.csv', table3_name)

        monitoring_conn.commit()
        logging.info("Данные из мастер-системы успешно загружены в целевую систему мониторинга.")

    monitoring_conn.close()
    logging.info("Подключение к целевой системе мониторинга закрыто.")

if __name__ == "__main__":
    main()
