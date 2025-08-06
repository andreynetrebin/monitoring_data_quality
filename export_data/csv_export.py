# export_data/csv_export
import logging
import csv
import pandas as pd
import time


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


def export_data_to_csv(cur, query, csv_file):
    """Выгрузка данных из базы данных в CSV файл с ручной записью заголовков и данных."""
    try:
        with open(csv_file, 'w', newline='') as csvfile:
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
    merged_df.to_csv(output_file, index=False, sep=';', encoding=encoding)  # Указываем разделитель
    logging.info(f"Объединенные данные сохранены в '{output_file}'.")


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
