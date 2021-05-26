import json
import pandas as pd
from typing import Union, List
from survey_personal_incomes.save import SPI_path
import yaml


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


class Uprating:
    affected_by = {
        "labour_income": ["PAY"]
    }

    def __init__(self, base_year: int = None, target_year: int = None):
        if base_year is not None and target_year is not None:
            self.empty = False
            self.multipliers = {}

            with open(SPI_path / "uprating.yaml") as f:
                parameters = yaml.safe_load(f)

            for variable in ("labour_income",):
                if variable not in parameters:
                    raise Exception(f"Uprating parameters do not contain {variable}")
                if base_year not in parameters[variable]:
                    raise Exception(f"Uprating parameters do not contain the rate for {base_year} for {variable}")
                if target_year not in parameters[variable]:
                    raise Exception(f"Uprating parameters do not contain the rate for {target_year} for {variable}")
                self.multipliers[variable] = parameters[variable][target_year] / parameters[variable][base_year]
        else:
            self.empty = True

    def __call__(self, table: pd.DataFrame) -> pd.DataFrame:
        table = table.copy(deep=True)
        if self.empty:
            return table
        for variable in self.multipliers:
            for affected_variable in self.affected_by[variable]:
                if affected_variable in table.columns:
                    table[affected_variable] *= self.multipliers[variable]
        return table

class SPI:
    def __init__(self, year: int):
        self.year = year
        self.tables = {}
        self.data_path = SPI_path / "data" / str(year)
        self.variables = {}
        self.uprater = Uprating()
        if not self.data_path.exists():
            available_years = list(map(lambda path: int(path.name), (SPI_path / "data").iterdir()))
            if len(available_years) == 0:
                raise FileNotFoundError(f"No SPI years stored.")
            try:
                base_year = available_years[-1]
                self.uprater = Uprating(base_year, year)
                self.year = base_year
            except Exception as e:
                raise Exception(f"No data for {year} stored, and uprating failed: {e}")

    def __getattr__(self, name: str) -> pd.DataFrame:
        if name == "description":
            return self.description
        if name not in self.tables:
            self.tables[name] = self.uprater(load(self.year, name))
        return self.tables[name]

    @property
    def table_names(self):
        return list(
            map(
                lambda p: p.name.split(".csv")[0],
                (self.data_path / "raw").iterdir(),
            )
        )