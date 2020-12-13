"""Various functions to support the complete data processing workload."""


import json
import numpy as np
import pandas as pd
from pathlib import Path


def create_path_list(file_path: str, extension: str = '.json') -> list:
    """Returns a list of Paths of all the files with the extension. All subdirectories of file_path are included.

    Example
        from pathlib import Path

        data_path = Path('.') / 'data'
        csv_path_list = create_path_list(data_path, '.csv')
    """
    return_list = [x for x in file_path.glob(f"**/*{extension}")]
    print(f"{file_path} contains {len(return_list)} {extension} files.")

    return return_list


def create_song_insert_list(file_path_list: list, insert_columns: list, primary_key: str,
                            dtype_dict: dict, not_nullable_columns: list = None) -> list:
    """Takes a raw file_path list as input, performs several validation checks, and returns a list of tuples
    ready for insertion in Postgres."""
    target_df = pd.DataFrame(columns=insert_columns)

    for idx, file in enumerate(file_path_list):
        temp_df = pd.read_json(file, lines=True)

        try:
            _df_assertions(temp_df, insert_columns, not_nullable_columns)
        except AssertionError as error:
            print(f"AssertionError @ file {idx} {file}: {error} NOTE: this file will not be inserted.")
        else:
            try:
                temp_df[insert_columns] = temp_df[insert_columns].astype(dtype_dict)
            except ValueError as error:
                print(f"ValueError @ file {idx} {file}: {error} NOTE: this file will not be inserted")
            else:
                target_df = target_df.append(temp_df[insert_columns], ignore_index=True)

    insert_df = target_df.drop_duplicates(subset=primary_key)
    print(
        f"There were {target_df.shape[0] - insert_df.shape[0]} duplicate primary keys removed from the insert dataframe")

    if not_nullable_columns:
        insert_df = insert_df.replace({'': None, np.nan: None})  # Postgres does not recognize '' or np.nan as NULL

    # This list comprehension converts the numpy dtypes to standard python dtypes which are necessary for Postgres
    return [tuple(row) for row in insert_df.itertuples(index=False)]


def create_log_insert_lists(file_path_list: list, insert_columns: list, primary_keys: list,
                            dtype_dict: dict, not_nullable_columns: list = None) -> list:
    """Takes a raw file_path list as input, performs several validation checks, and returns a list of tuples
    ready for insertion in Postgres."""
    target_df = pd.DataFrame(columns=insert_columns)

    for idx, file in enumerate(file_path_list):
        temp_df = pd.read_json(file, lines=True)

        try:
            _df_assertions(temp_df, insert_columns, not_nullable_columns)
        except AssertionError as error:
            print(f"AssertionError @ file {idx} {file}: {error} NOTE: this file will not be inserted.")
        else:
            try:
                # we do not want to store non logged in users
                temp_df = temp_df[temp_df['auth'] == 'Logged In']
                temp_df[insert_columns] = temp_df[insert_columns].astype(dtype_dict)
            except ValueError as error:
                print(f"ValueError @ file {idx} {file}: {error} NOTE: this file will not be inserted")
            else:
                target_df = target_df.append(temp_df[insert_columns], ignore_index=True)

    insert_users_df = target_df[insert_columns.remove('ts')].drop_duplicates(subset=primary_keys)
    print(
        f"There were {target_df.shape[0] - insert_users_df.shape[0]} duplicate primary keys removed from the insert dataframe")

    insert_time_df = _expand_ms(target_df['ts'])

    if not_nullable_columns:
        insert_users_df = insert_users_df.replace(
            {'': None, np.nan: None})  # Postgres does not recognize '' or np.nan as NULL

    # The list comprehension converts the numpy dtypes to standard python dtypes which are necessary for Postgres
    return ([tuple(row) for row in insert_users_df.itertuples(index=False)],
            [tuple(row) for row in insert_time_df.itertuples(index=False)])


def _df_assertions(df: pd.DataFrame, target_columns: list, not_nullable_columns: list = None) -> None:
    """Assert statements to make sure the retrieved data is valid and clean before insertion into the Postgres table."""
    low_df_columns = [x.lower() for x in df.columns]
    low_target_columns = [x.lower() for x in target_columns]

    found_cols = [x for x in low_df_columns if x in low_target_columns]
    assert sorted(found_cols) == sorted(low_target_columns), f"The columns do not match."

    if not_nullable_columns:
        assert df[
                   not_nullable_columns].isnull().values.any() == False, f"Missing values in not nullable target columns."
    else:
        assert df[
                   target_columns].isnull().values.any() == False, f"Missing values in the target columns, if allowed please specify these columns."

    return None


def _expand_ms(ms_series: pd.Series) -> pd.DataFrame:
    """Expands a Pandas series of milliseconds with several datetime attributes."""
    df = pd.DataFrame({'start_time': pd.to_datetime(ms_series, unit='ms')})

    df['hour'] = df['start_time'].dt.hour
    df['day'] = df['start_time'].dt.day
    df['day'] = df['start_time'].dt.isocalendar().week
    df['month'] = df['start_time'].dt.month
    df['year'] = df['start_time'].dt.year
    df['weekday'] = df['start_time'].dt.weekday

    return df