import logging
import json
import sys
from os import getcwd, path
from utils.logger import setup_logger
from importing_data.csv_import import import_data_to_monitoring, import_data_to_historical, \
    import_data_from_historical_to_monitoring
from exporting_data.csv_export import export_data_from_master, export_ids_from_monitoring, export_data_from_historical
from config import load_db_config
from monitoring_data.checks import perform_checks_data
from utils.file_utils import clear_directory

STATE_FILE = 'processing_state.json'


def read_processing_state():
    """Чтение состояния обработки из JSON-файла."""
    if path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}


def write_processing_state(state):
    """Запись состояния обработки в JSON-файл."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


def process_accounts(config, account_type):
    """Обработка открытых или закрытых лицевых счетов."""
    csv_filename = export_data_from_master(config, account_type)
    if csv_filename:
        return import_data_to_monitoring(config, csv_filename, account_type)
    return False


def main():
    # Настройка логирования
    setup_logger()
    # Загрузка конфигурации
    config = load_db_config()
    cur_dir_path = getcwd()
    # Чтение состояния обработки
    state = read_processing_state()
    current_step = state.get('current_step', 0)
    # Очистка временного каталога
    if current_step == 0:
        clear_directory(path.join(cur_dir_path, 'temp_data'))
    try:
        if current_step < 1 and process_accounts(config, 'opening'):
            write_processing_state({'current_step': 1})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 1-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 2 and process_accounts(config, 'closing'):
            write_processing_state({'current_step': 2})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 2-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 3 and export_ids_from_monitoring(config):
            write_processing_state({'current_step': 3})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 3-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 4 and import_data_to_historical(config, cur_dir_path):
            write_processing_state({'current_step': 4})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 4-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 5 and export_data_from_historical(config, cur_dir_path):
            write_processing_state({'current_step': 5})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 5-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 6 and import_data_from_historical_to_monitoring(config, cur_dir_path):
            write_processing_state({'current_step': 6})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 6-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    try:
        if current_step < 7:
            if perform_checks_data(config):
                logging.info("Все этапы обработки уже выполнены. Сбрасываем состояние.")
                write_processing_state({'current_step': 0})
    except Exception as e:
        logging.error(f"Ошибка при выполнении 7-го этапа: {e}")
        sys.exit(1)  # Завершение работы скрипта с кодом 1

    logging.info("Работа завершена!")


if __name__ == "__main__":
    main()
