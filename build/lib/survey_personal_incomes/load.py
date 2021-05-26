import json
import pandas as pd
from typing import Union, List
from survey_personal_incomes.save import SPI_path


def load(
    year: int,
    table: str,
    columns: List[str] = None,
) -> pd.DataFrame:
    year = str(year)
    data_path = SPI_path / "data" / year / "raw"
    if data_path.exists():
        if table is not None:
            df = pd.read_csv(
                data_path / (table + ".csv"), usecols=columns, low_memory=False
            )
        return df
    else:
        raise FileNotFoundError("Could not find the data requested.")


class SPI:
    def __init__(self, year: int):
        self.year = year
        self.tables = {}
        self.data_path = SPI_path / "data" / str(year)
        self.variables = {}
        if not self.data_path.exists():
            raise FileNotFoundError(f"No data for the year {year}.")

    def __getattr__(self, name: str) -> pd.DataFrame:
        if name == "description":
            return self.description
        if name not in self.tables:
            self.tables[name] = load(self.year, name)
        return self.tables[name]

    @property
    def table_names(self):
        return list(
            map(
                lambda p: p.name.split(".csv")[0],
                (self.data_path / "raw").iterdir(),
            )
        )