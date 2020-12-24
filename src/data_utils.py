"""Function and class to support the preprocessing module."""


import pandas as pd
import psycopg2


def create_database(username: str, password: str):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    conn = psycopg2.connect(f"host=127.0.0.1 dbname=studentdb user={username} password={password}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def create_path_list(file_path: str, extension: str = '.json', sort_list: bool = False) -> list:
    """
    Returns a list of Paths of all the files with the extension. All subdirectories of file_path are included.
    Sort list can be invoked in case time matters, e.g. in case of log data.
    
    Example
        from pathlib import Path
        
        data_path = Path('.') / 'data'
        csv_path_list = create_path_list(data_path, '.csv')     
    """
    return_list = [x for x in file_path.glob(f"**/*{extension}")]
    print(f"{file_path} contains {len(return_list)} {extension} files.")

    if sort_list:
        return sorted(return_list)

    return return_list


class DataValidation:
    """
    A class which is purely made to be inherited by the Song and Log preprocessing classes to avoid
    code duplication between the classes.
    """

    def validate_json(self, log_data: bool = False) -> pd.DataFrame:
        """
        If log_data is true, only the logged-in customers are kept.
        """
        target_df = pd.DataFrame(columns=self.columns)

        for idx, file in enumerate(self.path_list):
            temp_df = pd.read_json(file, lines=True)

            try:
                self.df_assertions(temp_df)
            except AssertionError as error:
                print(f"AssertionError @ file {idx} {file}: {error} NOTE: this file will not be inserted.")
            else:
                if log_data:
                    temp_df = temp_df[temp_df['auth'] == 'Logged In']
                try:
                    temp_df[self.columns] = temp_df[self.columns].astype(self.dtypes)
                except ValueError as error:
                    print(f"ValueError @ file {idx} {file}: {error} NOTE: this file will not be inserted")
                else:
                    target_df = target_df.append(temp_df[self.columns], ignore_index=True)

        return target_df

    def df_assertions(self, df: pd.DataFrame) -> None:
        """
        Assert the retrieved data is valid and clean before insertion into the Postgres table.
        """
        low_df_columns = [x.lower() for x in df.columns]
        low_target_columns = [x.lower() for x in self.columns]

        found_cols = [x for x in low_df_columns if x in low_target_columns]
        assert sorted(found_cols) == sorted(low_target_columns), f"The columns do not match."

        if self.not_nullable_columns:
            assert df[self.not_nullable_columns].isnull().values.any() == False, "Missing values in not nullable target columns."
        else:
            assert df[self.columns].isnull().values.any() == False, "Missing values in the target columns, if allowed please specify these columns."

        return None
