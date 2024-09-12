import os
import requests
import requests_cache
import time
import json

from dotenv import load_dotenv
from utils.utils import read_text, write_to_json, is_cached_result

requests_cache.install_cache()
load_dotenv()

PROJECT_VERSION = read_text('VERSION').strip()
HEADERS = {'user-agent': f'metalexplorer/{PROJECT_VERSION}'}
URL = 'https://ws.audioscrobbler.com/2.0/'
LASTFM_KEY = os.getenv('LASTFM_KEY')


class LastFmRequest:
    def __init__(self, identifiers, save_data=True) -> None:
        self.supported_categories = ("band", "album")
        self.identifiers = identifiers
        self.save_data = save_data
        self.ids = self._extract_ids()
        self.ids.update({'album': ['826c9743-a3f0-3479-bf06-8df2e140ef1d', 'e51e9779-2edc-3b39-959c-299fdb5ed940']})


    def _extract_ids(self):
        ids = {category: set() for category in self.identifiers.keys() if category in self.supported_categories}

        for category, values in self.identifiers.items():
            [ids[category].add(ids_dict["mbid"]) \
                for _, ids_dict in values.items() \
                if "mbid" in ids_dict.keys() and category in self.supported_categories]
        return ids
            
    def get_data(self):
        return self._search_by_id()

    def _search_by_id(self):
        self.data = {}
        for category, ids in self.ids.items():
            category_data = self._fetch_data_for_category(category, ids)
            self.data[category] = category_data
            write_to_json(category_data, f'data/raw/{category}/lastfm.json') \
                if self.save_data else None
            
    def _fetch_data_for_category(self, category, ids):
        category_data = []        
        for id in ids:
            response = self._request_and_extract_data(category, id)
            if response:
                    category_data.append(response)
        return category_data

    def _request_and_extract_data(self, category, id):
        parameters = self._config_parameters(category, 'getInfo', mbid=id)
        response = self._lastfm_request(parameters)
        if not response:
             return None
        time.sleep(0.25) if not is_cached_result(response) else None
        response = self._extract_info_by_category(category, response)
        return response

    def _config_parameters(self, category, method, mbid=None, names_to_search=None):
        category = 'artist' if category == 'band' else category
        parameters = {
            'method': f'{category}.{method}',
            'autocorrect': 1,
            'api_key': LASTFM_KEY,
            'format': 'json'
        }
        parameters.update({'mbid': mbid}) if mbid else parameters.update(names_to_search)
        return parameters

    def _lastfm_request(self, parameters):
        response = requests.get(URL, headers=HEADERS, params=parameters)

        if response.status_code != 200:
            print(f"{response.status_code}: {response.text}")
            return None
        
        return response
    
    def _extract_info_by_category(self, category, response):
        data = json.loads(response.text)
        if category == "band":
            return self._extract_artist_info(data)
        if category == "album":
            return self._extract_album_info(data)

    def _extract_artist_info(self, response_dict):
        artist_info = {}
        artist = response_dict.get('artist', None)
        if not artist:
            return None
        artist_info['name'] = artist.get('name', None)
        artist_info['mbid'] =  artist.get('mbid', None)
        artist_info.update(artist.get('stats', None))
        artist_info.update(artist.get('tags', None))
        return artist_info
    
    def _extract_album_info(self, response_dict):
        album_info = {}
        write_to_json(response_dict, 'data/raw/album/sample_raw_response_lastfm.json')
        album = response_dict.get('album', None)
        if not album:
            return None
        album_info['name'] = album.get('name', None)
        album_info['mbid'] = album.get('mbid', None)
        album_info['artist'] = album.get('artist', None)
        album_info['listeners'] = album.get('listeners', None)
        album_info['playcount'] = album.get('playcount', None)
        album_info.update({'tags': [tag['name'] for tag in album.get('tags', {}).get('tag', [])]})
        return album_info
