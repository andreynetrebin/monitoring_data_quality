import logging
from datetime import datetime
from database.db_connection import DatabaseConnection
from database.db_operations import DBOperations
from export_data.csv_export import export_data_to_csv

def run_checks(monitoring_conn):
    """Проведение проверок и формирование отчета."""
    db_ops = DBOperations(monitoring_conn)
    report_lines = []  # Список для хранения строк отчета

    # Получение текущей даты и времени
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Подсчет общего количества записей в таблицах
        total_opening_records = db_ops.count_total_records('public.master_opening_ils')
        total_closing_records = db_ops.count_total_records('public.master_closing_ils')
        report_lines.append(f"Количество ИЛС открытых: {total_opening_records}")
        report_lines.append(f"Количество ИЛС закрытых: {total_closing_records}")
        report_lines.append("=" * 30)

        # 1. Открытые лицевые счета, которые отсутствуют в исторической системе
        query1 = """
        SELECT moi.*
        FROM public.master_opening_ils moi
        WHERE NOT EXISTS(
            SELECT 1
            FROM public.historical_opening_ils hoi
            WHERE hoi.acc_id = moi.acc_id 
        )
        """
        count1 = db_ops.count_records(query1)
        if count1 > 0:
            export_data_to_csv(monitoring_conn, query1, 'отсутствующие_открытые_лицевые_счета.csv')
            report_lines.append("1. Открытые лицевые счета, которые отсутствуют в исторической системе:")
            report_lines.append(f"   - Количество записей: {count1}")
            report_lines.append("   - Данные выгружены в 'отсутствующие_открытые_лицевые_счета.csv'.")
        else:
            report_lines.append("1. Открытые лицевые счета, которые отсутствуют в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 2. Открытые лицевые счета, которые закрыты в исторической системе
        query2 = """
        SELECT moi.*, hoi.*
        FROM public.master_opening_ils moi
        JOIN public.historical_opening_ils hoi ON moi.acc_id = hoi.acc_id
        WHERE hoi.acc_sts <> 1
        """
        count2 = db_ops.count_records(query2)
        if count2 > 0:
            export_data_to_csv(monitoring_conn, query2, 'закрытые_открытые_лицевые_счета.csv')
            report_lines.append("2. Открытые лицевые счета, которые закрыты в исторической системе:")
            report_lines.append(f"   - Количество записей: {count2}")
            report_lines.append("   - Данные выгружены в 'закрытые_открытые_лицевые_счета.csv'.")
        else:
            report_lines.append("2. Открытые лицевые счета, которые закрыты в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 3. Открытые лицевые счета, у которых другой регион открытия в исторической системе
        query3 = """
        SELECT moi.*, hoi.*
        FROM public.master_opening_ils moi
        JOIN public.historical_opening_ils hoi ON moi.acc_id = hoi.acc_id
        WHERE hoi.acc_sts = 1 AND moi.opening_region <> hoi.opening_region
        """
        count3 = db_ops.count_records(query3)
        if count3 > 0:
            export_data_to_csv(monitoring_conn, query3, 'другой_регион_открытия_лицевых_счетов.csv')
            report_lines.append("3. Открытые лицевые счета, у которых другой регион открытия в исторической системе:")
            report_lines.append(f"   - Количество записей: {count3}")
            report_lines.append("   - Данные выгружены в 'другой_регион_открытия_лицевых_счетов.csv'.")
        else:
            report_lines.append("3. Открытые лицевые счета, у которых другой регион открытия в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 4. Закрытые лицевые счета, которые отсутствуют в исторической системе
        query4 = """
        SELECT mci.*
        FROM public.master_closing_ils mci
        WHERE NOT EXISTS(
            SELECT 1
            FROM public.historical_closing_ils hci
            WHERE hci.acc_id = mci.acc_id 
        )
        """
        count4 = db_ops.count_records(query4)
        if count4 > 0:
            export_data_to_csv(monitoring_conn, query4, 'отсутствующие_закрытые_лицевые_счета.csv')
            report_lines.append("4. Закрытые лицевые счета, которые отсутствуют в исторической системе:")
            report_lines.append(f"   - Количество записей: {count4}")
            report_lines.append("   - Данные выгружены в 'отсутствующие_закрытые_лицевые_счета.csv'.")
        else:
            report_lines.append("4. Закрытые лицевые счета, которые отсутствуют в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 5. Закрытые лицевые счета, которые не закрыты в исторической системе
        query5 = """
        SELECT mci.*, hci.*
        FROM public.master_closing_ils mci
        JOIN public.historical_closing_ils hci ON mci.acc_id = hci.acc_id
        WHERE hci.acc_sts NOT IN (3, 4)
        """
        count5 = db_ops.count_records(query5)
        if count5 > 0:
            export_data_to_csv(monitoring_conn, query5, 'не_закрытые_закрытые_лицевые_счета.csv')
            report_lines.append("5. Закрытые лицевые счета, которые не закрыты в исторической системе:")
            report_lines.append(f"   - Количество записей: {count5}")
            report_lines.append("   - Данные выгружены в 'не_закрытые_закрытые_лицевые_счета.csv'.")
        else:
            report_lines.append("5. Закрытые лицевые счета, которые не закрыты в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 6. Закрытые лицевые счета, у которых другой регион закрытия в исторической системе
        query6 = """
        SELECT mci.*, hci.*
        FROM public.master_closing_ils mci 
        JOIN public.historical_closing_ils hci ON mci.acc_id = hci.acc_id
        WHERE hci.acc_sts = 3 AND mci.closing_region <> hci.closing_region 
        """
        count6 = db_ops.count_records(query6)
        if count6 > 0:
            export_data_to_csv(monitoring_conn, query6, 'другой_регион_закрытия_лицевых_счетов.csv')
            report_lines.append("6. Закрытые лицевые счета, у которых другой регион закрытия в исторической системе:")
            report_lines.append(f"   - Количество записей: {count6}")
            report_lines.append("   - Данные выгружены в 'другой_регион_закрытия_лицевых_счетов.csv'.")
        else:
            report_lines.append("6. Закрытые лицевые счета, у которых другой регион закрытия в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

        # 7. Закрытые лицевые счета, у которых другая дата закрытия в исторической системе
        query7 = """
        SELECT mci.*, hci.*
        FROM public.master_closing_ils mci 
        JOIN public.historical_closing_ils hci ON mci.acc_id = hci.acc_id
        WHERE hci.acc_sts = 3 AND mci.death_date <> DATE(hci.aud_time) 
        """
        count7 = db_ops.count_records(query7)
        if count7 > 0:
            export_data_to_csv(monitoring_conn, query7, 'другая_дата_закрытия_лицевых_счетов.csv')
            report_lines.append("7. Закрытые лицевые счета, у которых другая дата закрытия в исторической системе:")
            report_lines.append(f"   - Количество записей: {count7}")
            report_lines.append("   - Данные выгружены в 'другая_дата_закрытия_лицевых_счетов.csv'.")
        else:
            report_lines.append("7. Закрытые лицевые счета, у которых другая дата закрытия в исторической системе:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")

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

        monitoring_conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при выполнении запросов: {e}")
    finally:
        # Закрытие соединения
        monitoring_conn.close()

if __name__ == "__main__":
    config = load_db_config()
    monitoring_conn = DatabaseConnection(config['monitoring_db'])
    run_checks(monitoring_conn)
