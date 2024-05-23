#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

from typing import List, Union

import pandas as pd
import polars as pl
import pyarrow as pa


def check_pandas_df_primary_keys_for_null_values(
    self, primary_keys: List[str], dataframe: Union[pd.DataFrame, pl.DataFrame]
):
    """Check if the primary keys in the dataframe have any null values.

    :param primary_keys: The primary keys of the dataframe
    :type primary_keys: `List[str]`
    :param dataframe: The dataframe to check
    :type dataframe: `pd.DataFrame`
    :return: True if the primary keys have null values, False otherwise
    :rtype: `bool`
    """
    null_counts = {}
    arrow_schema = pa.Schema.from_pandas(dataframe, preserve_index=False)
    for key in primary_keys:
        if arrow_schema.field(key).nullable:
            null_count = dataframe[key].isnull().sum()
            if null_count > 0:
                null_counts[key] = null_count

    if len(null_counts) > 0:
        raise ValueError(
            f"Found null values in primary keys: {null_counts}. Aborting insertion."
            "To disable automatic primary key check, set `check_primary_key=False` "
            "in the validation_options of the `insert` method."
        )

    return False
