from typing import Dict, Any, List

import pandas as pd
import numpy as np
import datatable as dt
import pydantic
import typing
import sqlalchemy

def fast_pandas_reader(filename: str):
    return dt.fread(filename).to_pandas()

pd.fast_pandas_reader = fast_pandas_reader


def read_yaml(file_path: str):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)



def build_table_structure_based_on_dict(dictionary: dict) -> dict:
    table_schema: dict[str, list[Any]] = {}
    for key in dictionary.keys():
        table_schema[key] = []
    
    return table_schema

