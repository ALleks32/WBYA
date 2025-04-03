from typing import Optional

import requests
from src.logger import logger

def get_subject_id(name_product: str) -> Optional[int]:
    """
    Делает запрос к API Wildberries, чтобы получить subject_id.
    Если subject_id отсутствует, возвращаем subjectParentId.
    """
    url = (
        "https://search.wb.ru/exactmatch/ru/common/v9/"
        f"search?appType=1&curr=page=1&query={name_product}&resultset=catalog"
    )
    logger.debug(f"Запрос к WB API: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка при запросе к API Wildberries для '{name_product}': {e}")
        return None

    products = data.get("data", {}).get("products", [])
    if not products:
        logger.debug(f"API WB вернул пустой список products для '{name_product}'.")
        return None

    product = products[0]
    subject_id = product.get("subjectId")
    if subject_id is not None:
        logger.debug(f"Для '{name_product}' получен subjectId: {subject_id}")
        return subject_id
    else:
        parent_id = product.get("subjectParentId")
        logger.debug(f"Для '{name_product}' subjectId отсутствует, возвращаем parentId: {parent_id}")
        return parent_id