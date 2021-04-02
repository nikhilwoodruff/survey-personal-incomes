import pandas as pd
from pathlib import Path
import os
import shutil
from tqdm import tqdm
import warnings
import json
import re

SPI_path = Path(__file__).parent

def save(folder: str, year: int, zipped: bool = True) -> None:
    """Save the FRS microdata to the package internal storage.

    Args:
        folder (str): A path to the (zipped or unzipped) folder downloaded from the UK Data Archive.
        year (int): The year to store the microdata as.
        zipped (bool, optional): Whether the folder given is zipped. Defaults to True.

    Raises:
        FileNotFoundError: If an invalid path is given.
    """

    # Get the folder ready.

    folder = Path(folder)
    if not os.path.exists(folder):
        raise FileNotFoundError("Invalid path supplied.")
    if zipped:
        new_folder = SPI_path / "data" / "tmp"
        shutil.unpack_archive(folder, new_folder)
        folder = new_folder
    main_folder = next(folder.iterdir())
    year = str(year)
    target_folder = SPI_path / "data" / year / "raw"
    if os.path.exists(target_folder):
        # Overwrite
        shutil.rmtree(target_folder)
    os.makedirs(target_folder)

    # Save the data.

    if (main_folder / "tab").exists():
        data_folder = main_folder / "tab"
        data_files = list(data_folder.glob("*.tab"))
        task = tqdm(data_files, desc="Saving data tables")
        for i, filepath in enumerate(task):
            task.set_description(f"Saving {filepath.name}")
            table_name = "main"
            df = pd.read_csv(filepath, delimiter="\t", low_memory=False).apply(
                pd.to_numeric, errors="coerce"
            )
            df.to_csv(target_folder / (table_name + ".csv"), index=False)
            if i == len(task) - 1:
                task.set_description("Saved all tables")
    else:
        raise FileNotFoundError("Could not find the TAB files.")

    # Clean up tmp storage.

    if (SPI_path / "data" / "tmp").exists():
        shutil.rmtree(SPI_path / "data" / "tmp")
