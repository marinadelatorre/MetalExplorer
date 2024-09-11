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
        self.identifiers = identifiers
        self.save_data = save_data
        self.ids = self._extract_ids()


    def _extract_ids(self):
        ids = {category: set() for category in self.identifiers.keys() if category in ("band", "album")}

        for category, values in self.identifiers.items():
            [ids[category].add(ids_dict["mbid"]) \
                for _, ids_dict in values.items() \
                if "mbid" in ids_dict.keys() and category in ("band", "album")]
        return ids
    
    def _search_by_id(self):
        # TODO: support album
        # TODO: refactor to reduce complexity
        self.data = {}
        for category, ids in self.ids.items():
            category_data = []
            for id in ids:
                parameters = self._config_parameters(category, 'getInfo', mbid=id)
                response = self._lastfm_request(parameters)
                category_data.append(self._extract_artist_info(json.loads(response.text)))
                time.sleep(0.25) if not is_cached_result(response) else None
            self.data[category] = category_data
            write_to_json(category_data, f'data/raw/{category}/lastfm.json') \
                if self.save_data else None


    def _lastfm_request(parameters):
        response = requests.get(URL, headers=HEADERS, params=parameters)

        if response.status_code != 200:
            print(f"{response.status_code}: {response.text}")
            return None
        
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

    def _extract_artist_info(response_dict):
        artist_info = {}
        artist_info['name'] = response_dict['artist'].get('name', None)
        artist_info['mbid'] =  response_dict['artist'].get('mbid', None)
        artist_info.update(response_dict['artist'].get('stats', None))
        artist_info.update(response_dict['artist'].get('tags', None))
        return artist_info
