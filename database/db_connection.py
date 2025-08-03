import psycopg2
import nzpy
from config import load_db_config

class DatabaseConnection:
    def __init__(self, db_config):
        self.db_type = db_config.get('type', 'postgresql')  # Определяем тип базы данных
        if self.db_type == 'postgresql':
            self.conn = psycopg2.connect(**db_config)
        elif self.db_type == 'netezza':
            self.conn = self.connect_to_netezza(db_config)
        else:
            raise ValueError("Unsupported database type")

    def connect_to_netezza(self, db_config):
        """Подключение к базе данных Netezza."""
        return nzpy.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['dbname'],
            timeout=3600  # Установите таймаут в 3600 секунд
        )

    def get_cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()
