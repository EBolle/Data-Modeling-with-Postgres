"""Variables to support the checking and validation of the processed and inserted data."""


artist_dtype_dict = {'artist_id': str,
                     'artist_name': str,
                     'artist_location': str,
                     'artist_latitude': float,
                     'artist_longitude': float}


songs_dtype_dict = {'song_id': str,
                    'title': str,
                    'artist_id': str,
                    'year': int,
                    'duration': float}


log_dtype_dict = {'userId': int,
                  'firstName': str,
                  'lastName': str,
                  'gender': str,
                  'level': str,
                  'ts': int}

