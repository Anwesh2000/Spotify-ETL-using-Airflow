import pandas as pd 
import requests
import json
from datetime import datetime
import datetime
from airflow.hooks.mysql_hook import MySqlHook

# Generate your token here:  https://developer.spotify.com/console/get-recently-played/
# Note: You need a Spotify account (can be easily created for free)


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")
    return True


def check_table_exists(**kwargs):
    query = 'select count(*) from information_schema.tables where table_name="recent_plays"'
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='spotify_data')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def store_data(song_df):
    
    table_status = check_table_exists()
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='spotify_data')
    if table_status[0][0] == 0:
        print("----- table does not exists, creating it")    
        create_sql = 'create table recent_plays(song_name varchar(100), artist_name varchar(100), played_at varchar(100), timestamp varchar(100))'
        mysql_hook.run(create_sql)
    else:
        print("----- table already exists")
    
    for content_id in range(song_df.shape[0]):
        print(song_df.loc[content_id,"song_name"],song_df.loc[content_id,"artist_name"],song_df.loc[content_id,"played_at"],song_df.loc[content_id,"timestamp"])
        insert_query = "insert into recent_plays (song_name, artist_name, played_at, timestamp) values (%s,%s,%s,%s)"
        values = (song_df.loc[content_id,"song_name"],song_df.loc[content_id,"artist_name"],song_df.loc[content_id,"played_at"],song_df.loc[content_id,"timestamp"],)
        mysql_hook.run(insert_query,parameters = values)
        mysql_hook.run('commit;')
        print(mysql_hook.get_first('select * from recent_plays'))


def run_spotify_etl():

    TOKEN = 'BQAA_cTBvCM3bP2IhVlcUKClbEvqrCBVMNYL-B7CD0x5ByImfS0t2y2Mn2D1mynEA4YXE4a5BYt5-J7XiEO4z7v6Gi__BMHhX7rvCLkpUnQ3l9MD2LsSYRGyeiiwjsohSBrha9OSBf1W1h-8AMzZKB3QrhmZlMobCjR5'

    headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token=TOKEN)
              }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_name = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_name,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
        song_df.to_csv(r'spotify_data_file/file1.csv')
        print(song_df)
        store_data(song_df)
        
        


