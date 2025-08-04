# export_data/csv_export
import logging
import csv


def export_data_to_csv_with_copy(cur, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с использованием COPY."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            cur.copy_expert(query, csvfile)
        logging.info(f"Данные выгружены в {csv_file}")
        return True  # Возвращаем True при успешном экспорте
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")
        return False  # Возвращаем False при ошибке


def export_data_to_csv(conn, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с ручной записью заголовков и данных."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            with conn.cursor() as cur:
                # Выполняем запрос и получаем данные
                cur.execute(query)
                # Получаем названия колонок
                colnames = [desc[0] for desc in cur.description]
                # Записываем заголовки в CSV
                csvfile.write(';'.join(colnames) + '\n')
                # Записываем данные в CSV
                for row in cur.fetchall():
                    csvfile.write(';'.join(map(str, row)) + '\n')
        logging.info(f"Данные выгружены в {csv_file}")
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")


def export_results_to_csv(results, csv_file):
    """Экспорт результатов выполнения запроса в CSV файл."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            # Записываем данные в CSV
            writer.writerows(results)
        logging.info(f"Результаты выгружены в {csv_file}")
    except Exception as e:
        logging.error(f"Ошибка при выгрузке результатов в CSV: {e}")


def from_netezza_export_to_csv_with_offset(cur, base_query, directory, output_file_base, batch_size=20000):
    """Экспорт данных из Netezza в CSV с использованием CREATE EXTERNAL TABLE и смещения."""
    offset = 0
    while True:
        # Формирование полного SQL-запроса с учетом смещения
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

    logging.info("Выгрузка завершена.")
