import pandas as pd
from os import path
from datetime import datetime
from exporting_data.csv_export import export_data_to_csv, load_acc_ids_from_csv, merge_csv_files
from database.queries import part_query_integrating
from utils.file_utils import create_directory


def perform_check_and_export(db_ops, db_ops_integrating, cur, check_query, check_name, report_lines):
    """Выполняет проверку, экспортирует данные и обновляет отчет."""

    # Создание директории для сохранения результатов
    current_date = datetime.now().strftime("%Y-%m-%d")  # Формат текущей даты
    result_directory = path.join('result_data', current_date)
    create_directory(result_directory)

    count = db_ops.count_records(check_query)
    if count > 0:
        # Обновление путей к файлам
        csv_file_path = path.join(result_directory, f'{check_name}.csv')
        export_data_to_csv(cur, check_query, csv_file_path, results=True)

        acc_ids = load_acc_ids_from_csv(csv_file_path, encoding='cp1251')

        if acc_ids:
            acc_ids_str = ', '.join(map(str, acc_ids))
            query_integrating = f"""{part_query_integrating} ({acc_ids_str}) and datatype = 1) as a
            WHERE rn = 1 
            ORDER BY bsn_ts DESC;
            """
            integra_csv_file = path.join(result_directory, f'{check_name}_integra.csv')
            results = db_ops_integrating.execute_query(query_integrating)
            results_df = pd.DataFrame(results, columns=['acc_id', 'status', 'error_txt', 'bsn_ts', 'ts'])
            results_df.to_csv(integra_csv_file, index=False)

            result_csv_file = path.join(result_directory, f'{check_name}_доб_интеграция.csv')
            merge_csv_files(csv_file_path, integra_csv_file, result_csv_file, encoding='cp1251', results=True)

            report_lines.append(f"{check_name.replace('_', ' ').capitalize()}:")
            report_lines.append(f"   - Количество записей: {count}")
            report_lines.append(f"   - Данные выгружены в '{result_csv_file}'.")
        else:
            report_lines.append(f"{check_name.replace('_', ' ').capitalize()}:")
            report_lines.append("   - Количество записей: 0")
            report_lines.append("   - Данные не выгружены.")
    else:
        report_lines.append(f"{check_name.replace('_', ' ').capitalize()}:")
        report_lines.append("   - Количество записей: 0")
        report_lines.append("   - Данные не выгружены.")

    db_ops.insert_check_result(check_name.replace('_', ' ').capitalize(), count)
