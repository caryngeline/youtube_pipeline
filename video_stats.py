import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='./.env')
api_key = os.getenv("API_KEY")
channel_handle = 'baekhyun'
max_results = 50

def get_playlist_id(): 

    try: 
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}'

        response = requests.get(url=url)
        response.raise_for_status()

        data = response.json()

        # Access JSON values using brackets
        # data['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 
        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']

        return channel_playlist_id
    
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__" :
    print(get_playlist_id())


