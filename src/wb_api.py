import requests

def get_subject_id(name_product: str) -> int | None:
    url = (
        "https://search.wb.ru/exactmatch/ru/common/v9/"
        f"search?appType=1&curr=page=1&query={name_product}&resultset=catalog"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Ошибка при запросе: {e}")
        return None

    products = data.get("data", {}).get("products", [])
    if not products:
        # Если список товаров пуст
        return None

    # Берём, например, первый товар из списка (или перебираем все - зависит от логики)
    product = products[0]
    subject_id = product.get("subjectId")
    if subject_id is not None:
        return subject_id
    else:
        # Если subjectId нет, то возвращаем subjectParentId
        return product.get("subjectParentId")
