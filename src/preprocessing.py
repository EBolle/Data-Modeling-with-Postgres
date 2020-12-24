"""
This module contains 3 classes, one for each data folder (Song & Log), and one specific for the songplays table,
and takes care of all preprocessing steps.
"""


from .data_utils import create_path_list, DataValidation
import numpy as np
import pandas as pd


class SongPreProcess(DataValidation):
    """
    This class imports, inspects, cleans, and transforms the raw song .json files
    to ready-for-insertion list of tuples.
    """

    def __init__(self, file_path: str):
        self.path_list = create_path_list(file_path)
        self.columns = sorted(['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                               'artist_longitude', 'title', 'song_id', 'year', 'duration'])
        self.dtypes = {'artist_id': str,
                       'artist_name': str,
                       'artist_location': str,
                       'artist_latitude': float,
                       'artist_longitude': float,
                       'song_id': str,
                       'title': str,
                       'year': int,
                       'duration': float}
        self.not_nullable_columns = ['artist_id', 'artist_name', 'song_id']
        self.artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
        self.songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']

    def data_pipeline(self):
        """Executes all necessary methods to return clean insert lists of tuples."""
        return_df = self.validate_json()
        return_df = return_df.replace({'': None, np.nan: None})  # Postgres does not recognize '' or np.nan as NULL
        artists, songs = self.create_postgres_insert_lists(return_df)

        return artists, songs

    def create_postgres_insert_lists(self, df: pd.DataFrame) -> list:
        """
        Returns 2 lists of tuples with standard python dtypes, these dtypes are necessary for Postgres.
        """
        artists_df = df[self.artist_columns]
        artists = [tuple(row) for row in artists_df.itertuples(index=False)]

        songs_df = df[self.songs_columns]
        songs = [tuple(row) for row in songs_df.itertuples(index=False)]

        return artists, songs


class LogPreProcess(DataValidation):
    """
    This class imports, inspects, cleans, and transforms the raw log .json files
    to ready-for-insertion list of tuples.
    """

    def __init__(self, file_path: str):
        self.path_list = create_path_list(file_path, sort_list=True)
        self.columns = sorted(['userId', 'firstName', 'lastName', 'gender', 'level',
                               'ts', 'auth', 'page', 'sessionId', 'location', 'userAgent',
                               'artist', 'length', 'song'])
        self.dtypes = {'userId': int,
                       'firstName': str,
                       'lastName': str,
                       'gender': str,
                       'level': str,
                       'ts': str,  # the timestamp changes when coercing to int, hence the use of str
                       'auth': str,
                       'page': str}
        self.not_nullable_columns = ['userId', 'level']
        self.songplays_help_df_columns = ['ts', 'userId', 'level', 'sessionId', 'location',
                                          'userAgent', 'artist', 'length', 'song']
        self.time_columns = ['ts']
        self.users_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']

    def data_pipeline(self):
        """
        Executes all necessary methods to return clean insert lists of tuples and the songsplays help dataframe.
        """
        return_df = self.validate_json(log_data=True)
        return_df = return_df.replace({'': None, np.nan: None})
        time, users = self.create_postgres_insert_lists(return_df)
        songplays_help_df = self.create_songplays_help_df(return_df)

        return songplays_help_df, time, users

    def create_postgres_insert_lists(self, df: pd.DataFrame) -> list:
        """
        Returns 2 lists of tuples with standard python dtypes, these dtypes are necessary for Postgres.
        """
        users_df = df[self.users_columns]
        users = [tuple(row) for row in users_df.itertuples(index=False)]

        time_df = self.expand_milliseconds(df[df['page'] == 'NextSong']['ts'])
        time = [tuple(row) for row in time_df.itertuples(index=False)]

        return time, users

    def create_songplays_help_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a dataframe which can be combined with artists and songs to create the songplays table in Postgres.
        """
        songplay_help_df = df[df['page'] == 'NextSong']
        songplay_help_df = songplay_help_df[self.songplays_help_df_columns]

        return songplay_help_df

    @staticmethod
    def expand_milliseconds(ms_series: pd.Series) -> pd.DataFrame:
        """
        Expands a Pandas series of milliseconds to a dataframe with several datetime attributes.
        """
        df = pd.DataFrame({'start_time': pd.to_datetime(ms_series, unit='ms')})

        df['hour'] = df['start_time'].dt.hour
        df['day'] = df['start_time'].dt.day
        df['day'] = df['start_time'].dt.week
        df['month'] = df['start_time'].dt.month
        df['year'] = df['start_time'].dt.year
        df['weekday'] = df['start_time'].dt.weekday

        return df


class SongPlaysPreProcess:
    def __init__(self, artists: list, songs: list, songplays_help_df: pd.DataFrame):
        self.artists = artists
        self.columns = ['start_time', 'userId', 'level', 'song_id', 'artist_id_x',
                        'sessionId', 'location_x', 'userAgent']
        self.songs = songs
        self.songplays_help_df = songplays_help_df

    def data_pipeline(self):
        """
        Executes all necessary methods to return a clean insert list of tuples for the Postgres table.
        """
        artists_df = self.transform_artists_to_df()
        songs_df = self.transform_songs_to_df()
        songplays_df = self.merge_dataframes(artists_df, songs_df)
        songplays_df = songplays_df.replace({'': None, np.nan: None})
        songplays = self.create_postgres_insert_lists(songplays_df)

        return songplays

    def transform_artists_to_df(self):
        artists_df = pd.DataFrame(data=self.artists, columns=['artist_id', 'name', 'location', 'latitude', 'longitude'])

        return artists_df

    def transform_songs_to_df(self):
        songs_df = pd.DataFrame(data=self.songs, columns=['song_id', 'title', 'artist_id', 'year', 'duration'])

        return songs_df

    def merge_dataframes(self, artists_df, songs_df):
        """
        Executes the merge methods and transformations to prepare the songplays table for insertion in Postgres.
        """
        songs_artists = self.songplays_help_df.merge(artists_df, how='left', left_on='artist', right_on='name')
        songs_artists['length'] = songs_artists['length'].astype('float')

        songs_artists = songs_artists.merge(songs_df, how='left', left_on=['song', 'length'],
                                            right_on=['title', 'duration'])
        songs_artists['start_time'] = pd.to_datetime(songs_artists['ts'], unit='ms')

        return songs_artists

    def create_postgres_insert_lists(self, df: pd.DataFrame) -> list:
        """
        Returns a list of tuples with standard python dtypes, these dtypes are necessary for Postgres.
        """
        songplays_df = df[self.columns]
        songplays = [tuple(row) for row in songplays_df.itertuples(index=False)]

        return songplays
