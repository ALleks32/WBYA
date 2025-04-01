import os
import pandas as pd
from src.logger import logger
from typing import Optional


def get_mi_data(file_path: str, delimiter: str = ';', encoding: str = 'utf-8', on_bad_lines: str = 'skip') -> Optional[
    pd.DataFrame]:
    """
    Читает CSV-файл по указанному пути и возвращает DataFrame с колонками: 'Артикул', 'Категория_YMid'.
    Параметр on_bad_lines позволяет задавать стратегию обработки строк с некорректным количеством столбцов.

    :param file_path: Путь к файлу 48342725_МИ.csv.
    :param delimiter: Разделитель в CSV-файле (по умолчанию ';', так как в файле используются точки с запятой).
    :param encoding: Кодировка файла (по умолчанию 'utf-8').
    :param on_bad_lines: Стратегия обработки некорректных строк (по умолчанию 'skip').
    :return: DataFrame с выбранными данными или None при возникновении ошибки.
    """
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines=on_bad_lines)
        logger.info(f"Файл '{file_path}' успешно прочитан с параметром on_bad_lines='{on_bad_lines}'.")
    except Exception as e:
        logger.error(f"Ошибка чтения файла '{file_path}': {e}")
        return None

    # Удаляем лишние пробелы и приводим имена столбцов к единому виду
    df.columns = df.columns.str.strip()

    required_columns = ["Артикул", "Категория_YMid"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Отсутствуют следующие столбцы: {', '.join(missing_columns)}")
        logger.info(f"Доступные столбцы: {', '.join(df.columns)}")
        return None

    return df[required_columns]
#
#
# if __name__ == "__main__":
#     file_path = os.path.join("data", "48342725_МИ.csv")
#     mi_data = get_mi_data(file_path)
#     if mi_data is not None:
#         # Выводим первые 5 строк для проверки извлечённых данных
#         print(mi_data.head())
