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

def get_video_ids(playlist_id):

    video_ids = []
    next_page_token = None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}'

    try:

        while True:
            url = base_url

            if next_page_token:
                url += f'&pageToken={next_page_token}'

            response = requests.get(url=url)
            response.raise_for_status
            
            data = response.json()

            # Aside from the format used in last function, use get()
            # Each item is each result, max of 50
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            next_page_token = data.get('nextPageToken')

            if not next_page_token:
                break
        
        print(len(video_ids))
        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
        
if __name__ == "__main__" :
    print(get_playlist_id())


