"""Function and class to support the preprocessing module."""


import pandas as pd
from pathlib import Path


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
                    temp_df = temp_df[temp_df['auth']=='Logged In']
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
            assert df[self.not_nullable_columns].isnull().values.any() == False, f"Missing values in not nullable target columns."
        else:
            assert df[target_columns].isnull().values.any() == False, f"Missing values in the target columns, if allowed please specify these columns."

        return None