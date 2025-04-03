import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

from src.wb_ym import WbYMProcessor

processor = WbYMProcessor("wb-ym.xlsx")

# 1. Считываем данные с листа "WB"
df_wb = processor.read_wb_data(sheet_name="WB")

# 2. Считываем данные с листа "YM"
df_ym = processor.read_ym_data(sheet_name="YM")

# 3. Выполняем обновление YM_id в WB
processor.update_wb_with_ym()

# 4. Смотрим, что получилось
df_result = processor.wb_data
df_result.to_excel("wb-ym_updated.xlsx", index=False)
print("Файл wb-ym_updated.xlsx сохранён.")

# Если хотите увидеть результат в консоли:
print("Результат:", df_result)
