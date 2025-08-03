# export/csv_export
import logging

def export_data_to_csv_with_copy(conn, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с использованием COPY."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            with conn.get_cursor() as cur:
                cur.copy_expert(query, csvfile)
        logging.info(f"Данные выгружены в {csv_file}")
    except Exception as e:
        logging.error(f"Ошибка при выгрузке данных в CSV: {e}")

def export_data_to_csv(conn, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с ручной записью заголовков и данных."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            with conn.get_cursor() as cur:
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

def from_netezza_export_to_csv_with_offset(netezza_conn, base_query, directory, output_file_base, batch_size=20000):
    """Экспорт данных из Netezza в CSV с использованием CREATE EXTERNAL TABLE и смещения."""
    offset = 0
    while True:
        # Формирование полного SQL-запроса с учетом смещения
        query = f"{base_query} WHERE rn > {offset} AND rn <= {offset + batch_size};"

        # Создание внешней таблицы для выгрузки
        external_table_query = f"""
        CREATE EXTERNAL TABLE '{directory}/{output_file_base}_{offset}.csv'
        USING (
            IncludeHeader   
            DELIMITER ';'
            ENCODING 'internal'
            REMOTESOURCE 'ODBC'
            ESCAPECHAR '\\'
        ) AS
        {query};
        """

        with netezza_conn.cursor() as cur:
            # Создание внешней таблицы и выгрузка данных
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
