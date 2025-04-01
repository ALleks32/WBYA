import os

from src.logger import logger
from src.parser.cabinet import get_mi_data
from src.parser.productsX import get_product_data
from src.parser.wb_ym import get_subject_data
from src.merge_data import merge_all_data


if __name__ == "__main__":
    # Определяем пути к файлам
    file_wb_ym = os.path.join("data", "wb-ym.xlsx")
    file_products = os.path.join("data", "productsWBMarusya.csv")
    file_cabinet = os.path.join("data", "48342725_МИ.csv")

    # Логируем загрузку исходных данных
    wb_data = get_subject_data(file_wb_ym)
    if wb_data is not None:
        logger.info(f"Данные из wb-ym загружены. Строк: {len(wb_data)}")
    product_data = get_product_data(file_products)
    if product_data is not None:
        logger.info(f"Данные из продуктов загружены. Строк: {len(product_data)}")
    mi_data = get_mi_data(file_cabinet)
    if mi_data is not None:
        logger.info(f"Данные из кабинета загружены. Строк: {len(mi_data)}")

    # Получаем финальный объединенный DataFrame с обновленным YM_id
    final_df = merge_all_data(file_wb_ym, file_products, file_cabinet)
    if final_df is not None:
        logger.info("Финальное объединение данных выполнено успешно.")
        print("Финальные объединенные данные (с обновленным YM_id):")
        print(final_df.head())
    else:
        logger.error("Объединение данных завершилось неудачно.")