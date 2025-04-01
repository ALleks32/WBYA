import os
import pandas as pd
from src.logger import logger
from typing import Optional

def get_product_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    Читает CSV-файл по указанному пути и возвращает DataFrame с колонками: subjectID, vendorCode.

    :param file_path: Путь к файлу productsWBMarusya.csv.
    :return: DataFrame с выбранными данными или None при возникновении ошибки.
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Файл '{file_path}' успешно прочитан.")
    except Exception as e:
        logger.error(f"Ошибка чтения файла '{file_path}': {e}")
        return None

    required_columns = ["subjectID", "vendorCode"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Отсутствуют следующие столбцы: {', '.join(missing_columns)}")
        return None

    return df[required_columns]

# if __name__ == "__main__":
#     file_path = os.path.join("data", "productsWBMarusya.csv")
#     product_data = get_product_data(file_path)
#     if product_data is not None:
#         # Выводим первые 5 строк для проверки извлечённых данных
#         print(product_data.head())
