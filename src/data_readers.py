import pandas as pd
from typing import Optional
from src.logger import logger


class WbYMReader:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._df_wb: Optional[pd.DataFrame] = None
        self._df_ym: Optional[pd.DataFrame] = None

    def read_wb_data(self, sheet_name: str = "WB") -> Optional[pd.DataFrame]:
        """Читает лист 'WB' и сохраняет в self._df_wb."""
        required_columns = ['parent_id', 'parent_name', 'subject_id', 'subject_name', 'YM_id']
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
            logger.info(f"Файл '{self.file_path}' успешно прочитан (лист '{sheet_name}').")
            logger.debug(f"Прочитано строк: {df.shape[0]}, столбцов: {df.shape[1]}")
        except Exception as e:
            logger.error(f"Ошибка чтения файла '{self.file_path}' (лист '{sheet_name}'): {e}")
            return None

        # Проверяем наличие нужных колонок
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(
                f"В листе '{sheet_name}' отсутствуют колонки: {', '.join(missing_columns)}"
            )
            return None

        self._df_wb = df[required_columns].copy()
        logger.info(f"WB-данные успешно сохранены. Итоговая форма: {self._df_wb.shape}")
        return self._df_wb

    def read_ym_data(self, sheet_name: str = "YM") -> Optional[pd.DataFrame]:
        """Читает лист 'YM' и сохраняет в self._df_ym."""
        required_columns = ['last_id', 'last_name']
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
            logger.info(f"Файл '{self.file_path}' успешно прочитан (лист '{sheet_name}').")
            logger.debug(f"Прочитано строк: {df.shape[0]}, столбцов: {df.shape[1]}")
        except Exception as e:
            logger.error(f"Ошибка чтения файла '{self.file_path}' (лист '{sheet_name}'): {e}")
            return None

        # Аналогичная проверка наличия необходимых колонок
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(
                f"В листе '{sheet_name}' отсутствуют колонки: {', '.join(missing_columns)}"
            )
            return None

        self._df_ym = df[required_columns].copy()
        logger.info(f"YM-данные успешно сохранены. Итоговая форма: {self._df_ym.shape}")
        return self._df_ym

    @property
    def wb_data(self) -> Optional[pd.DataFrame]:
        """Возвращает текущую копию данных из листа WB."""
        return self._df_wb

    @property
    def ym_data(self) -> Optional[pd.DataFrame]:
        """Возвращает текущую копию данных из листа YM."""
        return self._df_ym
