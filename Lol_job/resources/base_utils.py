import pandas as pd
import numpy as np
import datatable as dt


def fast_pandas_reader(filename):
    return dt.fread(filename).to_pandas()

pd.fast_pandas_reader = fast_pandas_reader


def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)