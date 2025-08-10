import logging
import time
from os import path, listdir
from database.db_connection import DatabaseConnection
from .data_loader import load_csv_to_table

# Директория для импорта CSV-файлов
temp_data_dir = 'temp_data'


def import_data_to_monitoring(config, csv_filename, account_type):
    """Импорт данных в базу системы мониторинга."""
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    logging.info("Подключение к целевой системе мониторинга установлено.")

    from database.db_operations import DBOperations
    db_ops = DBOperations(monitoring_conn, db_type=config['monitoring_db']['type'])
    start_time = time.time()

    columns = {
        'opening': {
            'snils': 'VARCHAR(14)',
            'acc_id': 'BIGINT',
            'opening_date': 'DATE',
            'opening_region': 'VARCHAR(6)',
            'registration_reason': 'TEXT',
        },
        'closing': {
            'snils': 'VARCHAR(14)',
            'acc_id': 'BIGINT',
            'death_date': 'DATE',
            'closing_region': 'VARCHAR(6)',
            'closing_reason': 'TEXT',
        }
    }

    table_name = f'master_{account_type}_ils'
    db_ops.drop_table(table_name)
    db_ops.create_postgresql_table(table_name, columns[account_type], ['acc_id'])

    # Обновленный путь к CSV-файлу
    csv_file_path = path.join(temp_data_dir, csv_filename)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            import_result = load_csv_to_table(cur, csv_file_path, table_name)

        if import_result:
            monitoring_conn.commit()
            end_time = time.time()
            logging.info(
                f"Импорт данных по {account_type} завершен. Время выполнения: {end_time - start_time:.2f} секунд.")
            return True
        else:
            end_time = time.time()
            logging.error(
                f"Не удалось импортировать данные по {account_type}. Время выполнения: {end_time - start_time:.2f} секунд.")
            return False


def import_data_to_historical(config, cur_dir_path):
    """Загрузка идентификаторов в историческую систему."""
    start_time = time.time()
    historical_conn = DatabaseConnection(config['historical_db'])
    logging.info("Подключение к базе исторической системы установлено.")

    from database.db_operations import DBOperations
    db_historical_ops = DBOperations(historical_conn, db_type=config['historical_db']['type'])
    prefix_table_name = 'vlg_mic'

    with historical_conn:
        with historical_conn.get_cursor() as cur:
            for account_type in ['opening', 'closing']:
                table_ids = f'{prefix_table_name}_ids_{account_type}_ils'
                csv_external_ids = path.join(temp_data_dir, f'ids_{account_type}_ils.csv')  # Обновленный путь

                db_historical_ops.drop_table(table_ids)
                db_historical_ops.create_netezza_table(table_ids, {'acc_id': 'BIGINT'}, 'acc_id')
                import_result = db_historical_ops.insert_to_netezza_from_select_external_csv(table_ids,
                                                                                             csv_external_ids)

                if not import_result:
                    logging.error(f"Не удалось загрузить идентификаторы для {account_type} в историческую систему.")
                    return False

    end_time = time.time()
    logging.info(
        f"Идентификаторы загружены в историческую систему. Время выполнения: {end_time - start_time:.2f} секунд.")
    return True


def import_data_from_historical_to_monitoring(config, cur_dir_path):
    """Загрузка в систему мониторинга, данных полученных из исторической системы."""
    start_time = time.time()
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    logging.info("Подключение к целевой системе мониторинга установлено.")

    from database.db_operations import DBOperations
    db_ops = DBOperations(monitoring_conn, db_type=config['monitoring_db']['type'])

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            for account_type in ['opening', 'closing']:
                directory_csv_portions = path.join(cur_dir_path, temp_data_dir,
                                                   f"vlg_mic_historical_{account_type}_ils")
                csv_files_portions = [f for f in listdir(directory_csv_portions) if f.endswith('.csv')]

                columns = {
                    'opening': {
                        'acc_id': 'BIGINT',
                        'acc_sts': 'INT',
                        'opening_region': 'VARCHAR(6)',
                    },
                    'closing': {
                        'acc_id': 'BIGINT',
                        'acc_sts': 'INT',
                        'death_date': 'DATE',
                        'closing_region': 'VARCHAR(6)',
                    }
                }

                table_name = f'historical_{account_type}_ils'
                db_ops.drop_table(table_name)
                db_ops.create_postgresql_table(table_name, columns[account_type], ['acc_id'])

                for csv_file in csv_files_portions:
                    csv_file_path = path.join(directory_csv_portions, csv_file)
                    load_csv_to_table(cur, csv_file_path, table_name)

                monitoring_conn.commit()
                logging.info(f"Загружены данные в базу мониторинга в таблицу - {table_name}")

    end_time = time.time()
    logging.info(f"Время выполнения: {end_time - start_time:.2f} секунд.")
    return True
