from pathlib import Path
import psycopg2
from settings import username, password
from src.preprocessing import LogPreProcess, SongPreProcess, SongPlaysPreProcess
from src.sql_queries import insert_table_queries


def main():
    """
    - connects to the database
    - processes the raw song and log data
    - inserts the processed data into Postgres
    """
    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user={username} password={password}")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    artists_data, songs_data = process_song_file()
    songplays_help_df, time_data, users_data = process_log_file()
    songplays_data = process_songplays_data(artists_data, songs_data, songplays_help_df)

    data_list = [songplays_data, users_data, songs_data, artists_data, time_data]
    for idx, (data, query) in enumerate(zip(data_list, insert_table_queries), start=1):
        print(f"inserting file {idx}/{len(data_list)}")
        for row in data:
            try:
                cur.execute(query, row)
            except psycopg2.Error as error:
                print(f"Psychog2 error @ file {idx} row {row}: {error} NOTE: this file will not be inserted.")

    conn.close()


def process_song_file():
    song_path_list = Path('.') / 'data' / 'song_data'
    songpp_instance = SongPreProcess(file_path=song_path_list)

    artists_data, songs_data = songpp_instance.data_pipeline()

    return artists_data, songs_data


def process_log_file():
    log_path_list = Path('.') / 'data' / 'log_data'
    logpp_instance = LogPreProcess(file_path=log_path_list)

    songsplays_help_df, time_data, users_data = logpp_instance.data_pipeline()

    return songsplays_help_df, time_data, users_data


def process_songplays_data(artists_data, songs_data, songplays_help_df):
    songplays_instance = SongPlaysPreProcess(artists_data, songs_data, songplays_help_df)
    songplays_data = songplays_instance.data_pipeline()

    return songplays_data


if __name__ == "__main__":
    print("start ETL...")
    main()
    print("done")
