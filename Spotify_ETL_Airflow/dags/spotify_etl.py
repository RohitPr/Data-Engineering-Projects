<<<<<<< HEAD
import datetime
from datetime import datetime, timedelta
from numpy import append
import requests
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import engine

# Function to check for Edge Cases and Abnormalities


def check_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("No Songs Downloaded")
        return False

    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check failed")

    if df.isnull().values.any():
        raise Exception("Null value Found")

    # yesterday = datetime.now()-timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()

    # for a in timestamps:
    #     if datetime.strptime(a, "%Y-%m-%d") != yesterday:
    #         raise Exception("Songs are not from yesterday")

    return True


def run_spotify_etl():

    database_location = "sqlite:///my_played_tracks.sqlite"
    token = "BQDSOtE55jU_G-EsiLc4n3kPOfrtsRNvrbU9wGDqUFrHNau-LdXa8MNpuKwO9r1Q6fkpfv7DVTqRuYQaZ7B7-pKKDhmxLd22x5gnWnlXJyf7oxGajcFdFTMLidMuq5S-ejBAdX_bUg0K_czZ_nYrpLQx9Gpz6HcTdWSq"

    headers_data = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

#  Take Yesterday's Date and change it to miliseconds and pass it as header for the request

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000

    spotify_request = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_timestamp), headers=headers_data)

    spotify_data = spotify_request.json()
    print(spotify_data)

    song_name = []
    artist_name = []
    played_at = []
    timestamp = []

    # Extracting Required data

    for song in spotify_data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamp.append(song["played_at"][:10])

    # Preparing Dict for DF object

    songs_dict = {
        "song_name": song_name,
        "artist_name": artist_name,
        "played_at": played_at,
        "timestamp": timestamp
    }

    songs_df = pd.DataFrame(songs_dict, columns=[
                            "song_name", "artist_name", "played_at", "timestamp"])

    # Validating Data for abnormalities

    if check_valid_data(songs_df):
        print("Data Valid, proceed to loading data")

    #  Creating DB Engine and connecting to the DB

    db_engine = sqlalchemy.create_engine(database_location)
    connection = sqlite3.connect('my_spotify_songs.sqlite')
    cursor = connection.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_spotify_songs(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)
    print("SQL DB Created")

    # Appending the data to the Created DB

    try:
        songs_df.to_sql("my_spotify_songs", engine,
                        index=False, if_exists=append)
    except:
        print("Data already exists in the Database")

    connection.close()
    print("Connection Closed")
=======
import datetime
from datetime import datetime, timedelta
from numpy import append
import requests
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import engine

# Function to check for Edge Cases and Abnormalities


def check_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("No Songs Downloaded")
        return False

    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check failed")

    if df.isnull().values.any():
        raise Exception("Null value Found")

    # yesterday = datetime.now()-timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()

    # for a in timestamps:
    #     if datetime.strptime(a, "%Y-%m-%d") != yesterday:
    #         raise Exception("Songs are not from yesterday")

    return True


def run_spotify_etl():

    database_location = "sqlite:///my_played_tracks.sqlite"
    token = "BQDSOtE55jU_G-EsiLc4n3kPOfrtsRNvrbU9wGDqUFrHNau-LdXa8MNpuKwO9r1Q6fkpfv7DVTqRuYQaZ7B7-pKKDhmxLd22x5gnWnlXJyf7oxGajcFdFTMLidMuq5S-ejBAdX_bUg0K_czZ_nYrpLQx9Gpz6HcTdWSq"

    headers_data = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

#  Take Yesterday's Date and change it to miliseconds and pass it as header for the request

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000

    spotify_request = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_timestamp), headers=headers_data)

    spotify_data = spotify_request.json()
    print(spotify_data)

    song_name = []
    artist_name = []
    played_at = []
    timestamp = []

    # Extracting Required data

    for song in spotify_data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamp.append(song["played_at"][:10])

    # Preparing Dict for DF object

    songs_dict = {
        "song_name": song_name,
        "artist_name": artist_name,
        "played_at": played_at,
        "timestamp": timestamp
    }

    songs_df = pd.DataFrame(songs_dict, columns=[
                            "song_name", "artist_name", "played_at", "timestamp"])

    # Validating Data for abnormalities

    if check_valid_data(songs_df):
        print("Data Valid, proceed to loading data")

    #  Creating DB Engine and connecting to the DB

    db_engine = sqlalchemy.create_engine(database_location)
    connection = sqlite3.connect('my_spotify_songs.sqlite')
    cursor = connection.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_spotify_songs(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)
    print("SQL DB Created")

    # Appending the data to the Created DB

    try:
        songs_df.to_sql("my_spotify_songs", engine,
                        index=False, if_exists=append)
    except:
        print("Data already exists in the Database")

    connection.close()
    print("Connection Closed")
>>>>>>> a36d51d4f01a602def4b5ca237d9cd16ee6cc130
