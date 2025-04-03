import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

from src.wb_ym import WbYMProcessor
from src.data_readers import WbYMReader

reader = WbYMReader("wb-ym.xlsx")

# 1. Считываем данные с листа "WB"
df_wb = reader.read_wb_data(sheet_name="WB")

# 2. Считываем данные с листа "YM"
df_ym = reader.read_ym_data(sheet_name="YM")


processor = WbYMProcessor("wb-ym.xlsx",df_wb, df_ym)


# 3. Выполняем обновление YM_id в WB
processor.update_wb_with_ym()

