import os
import shutil


def create_directory(directory_path):
    """Создание директории, если она не существует."""
    try:
        os.makedirs(directory_path, exist_ok=True)  # Создает директорию и все промежуточные директории
        print(f"Директория '{directory_path}' успешно создана или уже существует.")
    except Exception as e:
        print(f"Ошибка при создании директории '{directory_path}': {e}")


def clear_directory(directory_path):
    """Очистка директории, удаление всех файлов и поддиректорий."""
    try:
        if os.path.exists(directory_path) and os.path.isdir(directory_path):
            shutil.rmtree(directory_path)  # Удаляет директорию и все ее содержимое
            os.makedirs(directory_path)  # Восстанавливает пустую директорию
            print(f"Директория '{directory_path}' успешно очищена.")
        else:
            print(f"Директория '{directory_path}' не существует.")
    except Exception as e:
        print(f"Ошибка при очистке директории '{directory_path}': {e}")
