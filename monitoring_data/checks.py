import logging
from datetime import datetime
import time
from os import path
from database.db_connection import DatabaseConnection
from exporting_data.report_export import perform_check_and_export
from database.queries import check_query1, check_query2, check_query3, check_query4, check_query5, check_query6, \
    check_query7
from utils.file_utils import create_directory


def perform_checks_data(config):
    """Проведение проверок и формирование отчета."""
    start_time = time.time()
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    logging.info("Подключение к целевой системе мониторинга установлено.")

    from database.db_operations import DBOperations
    db_ops = DBOperations(monitoring_conn, db_type=config['monitoring_db']['type'])

    # Создание директории для сохранения результатов
    current_date = datetime.now().strftime("%Y-%m-%d")  # Формат текущей даты
    result_directory = path.join('result_data', current_date)
    create_directory(result_directory)

    with monitoring_conn:
        with monitoring_conn.get_cursor() as cur:
            integrating_conn = DatabaseConnection(config['integrating_db'])
            logging.info("Подключение к интеграционной базе установлено.")

            db_ops_integrating = DBOperations(integrating_conn, db_type=config['integrating_db']['type'])

            with integrating_conn:
                with integrating_conn.get_cursor() as cur_integrating:
                    # Создание таблицы для хранения проверок
                    columns_data_check_results = {
                        'id': 'SERIAL PRIMARY KEY',
                        'check_description': 'TEXT',
                        'record_count': 'INT',
                        'created_at': 'DATE DEFAULT CURRENT_DATE',
                    }
                    db_ops.create_postgresql_table('data_check_results', columns_data_check_results, ['id'])

                    report_lines = []
                    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        # Подсчет общего количества записей в таблицах
                        total_opening_records = db_ops.count_total_records('master_opening_ils')
                        total_closing_records = db_ops.count_total_records('master_closing_ils')

                        historical_conn = DatabaseConnection(config['historical_db'])
                        db_ops_historical = DBOperations(historical_conn, db_type=config['historical_db']['type'])
                        with historical_conn:
                            with historical_conn.get_cursor() as cur_historical:
                                total_historical_opening_records = db_ops_historical.count_total_records(
                                    'vlg_mic_historical_opening_ils')
                                total_historical_closing_records = db_ops_historical.count_total_records(
                                    'vlg_mic_historical_closing_ils')

                        report_lines.append(
                            "Проверка проводилась по данным полученным из ЕЦП ХОАД в сравнение с их состоянием в исторической системе (СПУ)")
                        report_lines.append("по открытым лицевым счетам после 12.05.2024 и умершим после 12.05.2024")
                        report_lines.append(f"Количество ИЛС открытых: {total_opening_records}")
                        report_lines.append(f"Количество ИЛС умерших: {total_closing_records}")
                        report_lines.append("=" * 30)

                        db_ops.insert_check_result("Количество ИЛС открытых", total_opening_records)
                        db_ops.insert_check_result("Количество ИЛС умерших", total_closing_records)

                        db_ops.insert_check_result("Количество ИЛС открытых в исторической системе",
                                                   total_historical_opening_records)
                        db_ops.insert_check_result("Количество ИЛС умерших в исторической системе",
                                                   total_historical_closing_records)

                        # Выполнение проверок
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query1,
                                                 'открытые_лицевые_счета,_которые_отсутствуют_в_исторической_системе',
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query2,
                                                 "открытые_лицевые_счета,_у_которых_в_исторической_системе_статус_отличный_от_статуса_'Актуальный'",
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query3,
                                                 'открытые_лицевые_счета,_у_которых_другой_регион_открытия_в_исторической_системе',
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query4,
                                                 'закрытые_лицевые_счета,_которые_отсутствуют_в_исторической_системе',
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query5,
                                                 "закрытые_лицевые_счета,_у_которых_в_исторической_системе_статус_отличный_от_статусов_'Умер',_'Упразднен'",
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query6,
                                                 'закрытые_лицевые_счета,_у_которых_другой_регион_закрытия_в_исторической_системе',
                                                 report_lines)
                        perform_check_and_export(db_ops, db_ops_integrating, cur, check_query7,
                                                 'закрытые_лицевые_счета,_у_которых_другая_дата_смерти_в_исторической_системе',
                                                 report_lines)

                        # Формирование отчета
                        report_file = path.join(result_directory, 'отчет.txt')  # Обновленный путь к отчету
                        with open(report_file, 'w') as f:
                            f.write("Отчет о проверках данных\n")
                            f.write("=" * 30 + "\n")
                            f.write(f"Дата формирования отчета: {report_date}\n")
                            f.write("=" * 30 + "\n")
                            for line in report_lines:
                                f.write(line + "\n")
                        end_time = time.time()
                        logging.info(
                            f"Отчет сохранен в '{report_file}'. Время выполнения: {end_time - start_time:.2f} секунд.")
                        return True

                    except Exception as e:
                        logging.error(f"Ошибка при выполнении запросов: {e}")
                        return False
