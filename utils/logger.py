import logging
import os


def setup_logger(log_file='app.log', log_level=logging.DEBUG):
    """Настройка логирования."""
    # Создание директории для логов, если она не существует
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Полный путь к файлу лога
    log_file_path = os.path.join('logs', log_file)

    # Настройка формата логирования
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )

    logging.info("Логирование настроено.")
