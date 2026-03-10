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
        # Channels -> list
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

    # PlaylistItems -> list
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}'

    try:

        while True:
            url = base_url

            if next_page_token:
                url += f'&pageToken={next_page_token}'

            response = requests.get(url=url)
            response.raise_for_status
            
            data = response.json()

            # Aside from the format used in last function, use get() for main 
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



def extract_video_data(video_id_list):
    extracted_data = []
    
    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]
    
    try:
        # Create a comma separated string containing all video ids until max_result
        for batch in batch_list(video_id_list, max_results):
            video_id_str = ",".join(batch)

            # Videos -> list
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_id_str}&key={api_key}'

            response = requests.get(url=url)
            response.raise_for_status()

            data = response.json()

            # For each batch of videos, create a dictionary and append it to the extracted_data list
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                content_details = item['contentDetails']
                statistics = item['statistics']
            
                video_data = {
                    "video_id" : video_id,
                    "title" : snippet['title'],
                    "publishedAt" : snippet['publishedAt'],
                    "duration" : content_details['duration'],
                    "viewCount" : statistics.get('viewCount', None),
                    "likeCount" : statistics.get('likeCount', None),
                    "commentCount" : statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)
    
        return extracted_data
    
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__" :
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)

