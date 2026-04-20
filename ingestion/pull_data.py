import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
DATABRICKS_PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")

FILE_PATH = '/Volumes/hot100/raw/landing'
HOT_100_URL = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/recent.json'
HOT_100_HISTORIC_BASE = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/'
LASTFM_API_BASE = 'http://ws.audioscrobbler.com/2.0/'
LYRIST_API_BASE = 'https://lyrist.vercel.app/api/'

def pull_data(**kwargs):

    #If date_string in kwargs, use that date
    date_string = kwargs.get('date_string', None)
    if date_string:
        url = HOT_100_HISTORIC_BASE + date_string + '.json'
    else:
        url = HOT_100_URL

    response = requests.get(url)
    hot_100 = response.json()

    #For each entry, attempt to grab metadata from last.fm and lyrics from lyrist
    count = 1
    for entry in hot_100['data']:
        
        print(f"Gathering data for song {count} - {entry['song']} by {entry['artist']}")
        count += 1

        response = requests.get(LASTFM_API_BASE + f"?method=track.getInfo&api_key={LASTFM_API_KEY}&artist={entry['artist']}&track={entry['song']}&format=json")
        trackInfo = response.json()
        if 'error' in trackInfo:
            pass
        else:
            entry['duration'] = trackInfo['track']['duration']
            entry['lastfm_listeners'] = trackInfo['track']['listeners']
            entry['lastfm_playcount'] = trackInfo['track']['playcount']
            entry['toptags'] = trackInfo['track']['toptags']
            if 'wiki' in trackInfo['track']:
                entry['summary'] = trackInfo['track']['wiki']['summary']

        try:
            response = requests.get(LYRIST_API_BASE + f"/{entry['artist'].replace(' ', '+')}/{entry['song'].replace(' ', '+')}")
            response.raise_for_status()
            lyrics = response.json()
            if 'lyrics' in lyrics:
                entry['lyrics'] = lyrics['lyrics']
        except:
            pass

    fname = f"hot-100-{hot_100['date']}.json"
    response = requests.put(
        f"https://dbc-215f4522-d794.cloud.databricks.com/api/2.0/fs/files{FILE_PATH}/{fname}?overwrite=true",
        headers={
            "Authorization": f"Bearer {DATABRICKS_PAT_TOKEN}",
            "Content-Type": "application/octet-stream",
        },
        data=json.dumps(hot_100).encode("utf-8")
    )
    response.raise_for_status()

if __name__ == "__main__":
    pull_data()