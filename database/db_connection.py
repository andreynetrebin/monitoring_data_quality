import logging
import psycopg2
import nzpy
from utils.logger import setup_logger  # Импортируем функцию настройки логирования

# Настройка логирования
setup_logger()  # Вызываем функцию для настройки логирования


class DatabaseConnection:
    def __init__(self, db_config):
        logging.info(f"Инициализация подключения к базе данных с конфигурацией: {db_config}")
        self.db_type = db_config.get('type')  # Определяем тип базы данных
        self.conn = None
        self.db_config = db_config  # Сохраняем конфигурацию базы данных
        self.connect()  # Устанавливаем соединение при инициализации

    def connect(self):
        """Устанавливает соединение с базой данных в зависимости от типа."""
        if self.db_type == 'postgresql':
            self.conn = self.connect_to_postgresql(self.db_config)
        elif self.db_type == 'netezza':
            self.conn = self.connect_to_netezza(self.db_config)
        else:
            logging.error(f"Unsupported database type: {self.db_type}")
            raise ValueError("Unsupported database type")

    def connect_to_netezza(self, db_config):
        """Подключение к базе данных Netezza."""
        try:
            connection = nzpy.connect(
                host=db_config['host'],
                port=int(db_config['port']),
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['dbname'],
                timeout=3600  # Установите таймаут в 3600 секунд
            )
            logging.info("Успешное подключение к Netezza.")
            return connection
        except Exception as e:
            logging.error(f"Ошибка при подключении к Netezza: {e}")
            raise

    def connect_to_postgresql(self, db_config):
        """Подключение к базе данных PostgreSQL."""
        try:
            connection = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['dbname'],
            )
            logging.info("Успешное подключение к PostgreSQL.")
            return connection
        except Exception as e:
            logging.error(f"Ошибка при подключении к PostgreSQL: {e}")
            raise

    def get_cursor(self):
        """Получение курсора, проверяя состояние соединения."""
        if self.conn is None or self.conn.closed:
            logging.info("Соединение закрыто или отсутствует, пересоздание соединения.")
            self.connect()  # Пересоздаем соединение
        logging.debug("Получение курсора.")
        return self.conn.cursor()

    def close(self):
        """Закрытие соединения с базой данных."""
        if self.conn is not None and not self.conn.closed:
            logging.info("Закрытие соединения с базой данных.")
            self.conn.close()

    def commit(self):
        """Подтверждение транзакции."""
        if self.conn is not None and not self.conn.closed:
            logging.info("Подтверждение транзакции.")
            self.conn.commit()

    def __enter__(self):
        logging.debug("Вход в контекстный менеджер.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logging.debug("Выход из контекстного менеджера.")
        self.close()
