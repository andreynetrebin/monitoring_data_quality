import configparser


def load_db_config(filename='config.ini'):
    """Загрузка конфигурации базы данных из INI-файла."""
    config = configparser.ConfigParser()
    config.read(filename)
    return {
        'master_db': {
            'dbname': config.get('master_db', 'dbname'),
            'user': config.get('master_db', 'user'),
            'password': config.get('master_db', 'password'),
            'host': config.get('master_db', 'host'),
            'port': config.get('master_db', 'port'),
            'type': config.get('master_db', 'type')
        },
        'historical_db': {
            'dbname': config.get('historical_db', 'dbname'),
            'user': config.get('historical_db', 'user'),
            'password': config.get('historical_db', 'password'),
            'host': config.get('historical_db', 'host'),
            'port': config.get('historical_db', 'port'),
            'type': config.get('historical_db', 'type')
        },
        'monitoring_db': {
            'dbname': config.get('monitoring_db', 'dbname'),
            'user': config.get('monitoring_db', 'user'),
            'password': config.get('monitoring_db', 'password'),
            'host': config.get('monitoring_db', 'host'),
            'port': config.get('monitoring_db', 'port'),
            'type': config.get('monitoring_db', 'type')
        },
        'integrating_db': {
            'dbname': config.get('integrating_db', 'dbname'),
            'user': config.get('integrating_db', 'user'),
            'password': config.get('integrating_db', 'password'),
            'host': config.get('integrating_db', 'host'),
            'port': config.get('integrating_db', 'port'),
            'type': config.get('integrating_db', 'type')
        },
        'ip_for_dashboard': {
            'ip_address': config.get('ip_for_dashboard', 'ip_address'),
        }

    }
