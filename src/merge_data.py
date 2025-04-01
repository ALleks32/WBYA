from typing import Optional

import pandas as pd

from src.logger import logger
from src.parser.cabinet import get_mi_data
from src.parser.productsX import get_product_data
from src.parser.wb_ym import get_subject_data


def merge_all_data(wb_file_path: str, products_file_path: str, cabinet_file_path: str) -> Optional[pd.DataFrame]:
    """
    Загружает данные из трёх источников:
      - wb-ym.xlsx (ожидаются столбцы: parent_id, subject_id, YM_id)
      - productsWBMarusya.csv (ожидаются столбцы: subjectID, vendorCode и т.д.)
      - 48342725_МИ.csv (ожидается столбец: Артикул)

    Логика объединения:
      1. Для wb-ym данных создается ключ объединения 'join_key':
         если 'subject_id' заполнен (более глубокий и точный), используется он, иначе берется 'parent_id'.
      2. Производится объединение wb-ym и продуктов по ключу:
         wb-ym.join_key == products.subjectID.
      3. Далее производится дополнительное объединение с данными кабинета,
         где products.vendorCode должно совпадать с кабинетом.Артикул.

    В финальном DataFrame сохраняется столбец YM_id из wb-ym, который содержит нужную информацию.

    :param wb_file_path: Путь к файлу wb-ym.xlsx.
    :param products_file_path: Путь к файлу productsWBMarusya.csv.
    :param cabinet_file_path: Путь к файлу 48342725_МИ.csv.
    :return: Финальный объединённый DataFrame или None при ошибке.
    """
    # Загрузка данных
    wb_data = get_subject_data(wb_file_path)
    if wb_data is None:
        logger.error("Не удалось загрузить данные из файла wb-ym.")
        return None

    product_data = get_product_data(products_file_path)
    if product_data is None:
        logger.error("Не удалось загрузить данные из файла продуктов.")
        return None

    mi_data = get_mi_data(cabinet_file_path)
    if mi_data is None:
        logger.error("Не удалось загрузить данные из файла кабинета.")
        return None

    # Приводим ключевые столбцы к строковому типу
    wb_data['subject_id'] = wb_data['subject_id'].astype(str)
    wb_data['parent_id'] = wb_data['parent_id'].astype(str)
    product_data['subjectID'] = product_data['subjectID'].astype(str)

    # Создаем столбец 'join_key': приоритет отдается 'subject_id', иначе используется 'parent_id'
    wb_data['join_key'] = wb_data.apply(
        lambda row: row['subject_id'].strip() if row['subject_id'].strip() not in ("", "nan")
        else row['parent_id'].strip(), axis=1
    )

    # Объединяем wb-ym и продукты по ключу join_key == subjectID
    merged_df = pd.merge(wb_data, product_data, left_on='join_key', right_on='subjectID', how='inner')
    logger.info(f"Объединение wb-ym и продуктов выполнено успешно. Получено строк: {len(merged_df)}")

    # Приводим столбцы для объединения с данными кабинета к строковому типу
    merged_df['vendorCode'] = merged_df['vendorCode'].astype(str)
    mi_data['Артикул'] = mi_data['Артикул'].astype(str)

    # Дополнительное объединение: объединяем по условию, что vendorCode из продуктов совпадает с Артикул из кабинета
    final_df = pd.merge(merged_df, mi_data, left_on='vendorCode', right_on='Артикул', how='inner')
    logger.info(f"Финальное объединение выполнено успешно. Получено строк: {len(final_df)}")

    # В финальном DataFrame сохраняется столбец YM_id из wb-ym
    return final_df
