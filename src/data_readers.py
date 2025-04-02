
import pandas as pd
from pathlib import Path
from typing import Optional
from src.logger import logger


class WbYMReader:

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._df = None

    def read_data(self, sheet_name: str = "WB") -> Optional[pd.DataFrame]:
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
            logger.info(f"Файл '{self.file_path}' успешно прочитан (лист '{sheet_name}').")
        except Exception as e:
            logger.error(f"Ошибка чтения файла '{self.file_path}' (лист '{sheet_name}'): {e}")
            return None

        required_columns = ['parent_id', 'parent_name','subject_id', 'subject_name', 'YM_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"В файле wb-ym отсутствуют следующие колонки: {', '.join(missing_columns)}")
            return None

        self._df = df[required_columns]
        return self._df

    @property
    def data(self) -> Optional[pd.DataFrame]:
        return self._df



class CabinetTableReader:
    """
    Класс для чтения файлов с таблицами кабинетов из указанной папки.
    Ожидаемые колонки: 'Категория_YMname', 'Категория_YMid'.
    Данные из всех файлов объединяются в один DataFrame.
    """
    def __init__(self, folder_path: str):
        self.folder_path = folder_path
        self._df = None

    def read_data(self) -> Optional[pd.DataFrame]:
        folder = Path(self.folder_path)
        if not folder.exists() or not folder.is_dir():
            logger.error(f"Папка '{self.folder_path}' не найдена или не является директорией.")
            return None

        dfs = []
        for file in folder.iterdir():
            if file.suffix.lower() in [".xlsx", ".xls"]:
                try:
                    df = pd.read_excel(file)
                    logger.info(f"Файл '{file}' успешно прочитан (Excel).")
                except Exception as e:
                    logger.error(f"Ошибка чтения файла '{file}': {e}")
                    continue
            elif file.suffix.lower() == ".csv":
                try:
                    df = pd.read_csv(file, delimiter=";", encoding="utf-8", on_bad_lines="skip")
                    logger.info(f"Файл '{file}' успешно прочитан (CSV).")
                except Exception as e:
                    logger.error(f"Ошибка чтения файла '{file}': {e}")
                    continue
            else:
                logger.warning(f"Файл '{file}' имеет неподдерживаемый формат и пропущен.")
                continue

            required_columns = ['Категория_YMname', 'Категория_YMid']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"В файле '{file}' отсутствуют следующие колонки: {', '.join(missing_columns)}")
                continue

            dfs.append(df[required_columns])

        if not dfs:
            logger.error("Не удалось прочитать ни одного файла с корректными данными из папки.")
            return None

        combined_df = pd.concat(dfs, ignore_index=True)
        self._df = combined_df
        logger.info(f"Объединено {len(dfs)} файлов, итоговое число строк: {len(combined_df)}.")
        return self._df

    @property
    def data(self) -> Optional[pd.DataFrame]:
        return self._df


