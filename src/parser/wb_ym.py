import os
import pandas as pd
from src.logger import logger

def get_subject_data(file_path, sheet_name="WB"):
    """
    Читает Excel-файл по указанному пути и листу,
    возвращает DataFrame с колонками: parent_id, subject_id, YM_id.

    :param file_path: Путь к файлу wb-ym.xlsx.
    :param sheet_name: Название листа (по умолчанию "WB").
    :return: DataFrame с данными или None при ошибке.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        logger.info(f"Файл '{file_path}' успешно прочитан (лист '{sheet_name}').")
    except Exception as e:
        logger.error(f"Ошибка чтения файла '{file_path}' или листа '{sheet_name}': {e}")
        return None

    # Проверка наличия необходимых столбцов
    required_columns = ["parent_id", "subject_id", "YM_id"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Отсутствуют следующие столбцы: {', '.join(missing_columns)}")
        return None

    # Возвращаем только нужные столбцы
    return df[required_columns]

# if __name__ == "__main__":
#     file_path = os.path.join("data", "wb-ym.xlsx")
#     subject_data = get_subject_data(file_path)
#     if subject_data is not None:
#         # Пример использования: вывод первых 5 строк, чтобы проверить содержимое subject_id
#         print(subject_data.head())

