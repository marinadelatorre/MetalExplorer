import os
import requests
import requests_cache
import time
import json

from dotenv import load_dotenv
from tqdm import tqdm
from utils.utils import read_text, read_from_json, write_to_json, is_cached_result

requests_cache.install_cache()
load_dotenv()

PROJECT_VERSION = read_text('VERSION').strip()
HEADERS = {'user-agent': f'metalexplorer/{PROJECT_VERSION}'}
URL = 'https://ws.audioscrobbler.com/2.0/'
LASTFM_KEY = os.getenv('LASTFM_KEY')


class LastFmRequest:
    def __init__(self, identifiers={}, save_data=True) -> None:
        self.supported_categories = ("band", "album")
        self.identifiers = identifiers
        self.save_data = save_data
        self.ids = self._extract_ids()


    def _extract_ids(self):
        ids = {category: set() for category in self.identifiers.keys() if category in self.supported_categories}

        for category, values in self.identifiers.items():
            [ids[category].add(ids_dict["mbid"]) \
                for _, ids_dict in values.items() \
                if "mbid" in ids_dict.keys() and category in self.supported_categories]
        return ids
            
    def get_data_by_id(self):
        return self._search_by_id()
            
    def get_data_by_name(self, category):
        return self._search_by_name(category)

    def _search_by_name(self, category):
        self.data = {}
        named_items = self._get_named_items(category)
        category_data = self._fetch_data_for_category(category, named_params=named_items)
        self.data[category] = category_data
        write_to_json(category_data, f'data/raw/{category}/lastfm_by_name.json') \
            if self.save_data else None
            
    def _get_named_items(self, category):
        items = read_from_json(f'data/raw/{category}/wikidata_items_details.json')
        if category == 'album':
            items = [{'album': album, 'artist': v['results']['bindings'][0].get('performerLabel', {}).get('value', None)} \
                    for album, value in items.items() \
                    for id, v in value.items()]
        if category == 'band':
            items = [{'artist': artist} for artist in items.keys()]
        return items

    def _search_by_id(self):
        self.data = {}
        for category, ids in self.ids.items():
            category_data = self._fetch_data_for_category(category, ids=ids)
            self.data[category] = category_data
            write_to_json(category_data, f'data/raw/{category}/lastfm_by_id.json') \
                if self.save_data else None
            
    def _fetch_data_for_category(self, category, ids=None, named_params=None):
        category_data = []
        if ids:
            search_parameters = [self._config_parameters(category, 'getInfo', mbid=id) for id in ids]
        elif named_params:
            search_parameters = [self._config_parameters(category, 'getInfo', named_params=params)
                          for params in named_params]
        for parameters in tqdm(search_parameters, total=len(search_parameters), desc=f"Fetching lastfm {category}s"):
            response = self._lastfm_request(parameters)
            time.sleep(0.25) if not is_cached_result(response) else None
            if response:
                response = self._extract_info_by_category(category, response)
                category_data.append(response) if response else None
        return category_data

    def _config_parameters(self, category, method, mbid=None, named_params=None):
        category = 'artist' if category == 'band' else category
        parameters = {
            'method': f'{category}.{method}',
            'autocorrect': 1,
            'api_key': LASTFM_KEY,
            'format': 'json'
        }
        parameters.update({'mbid': mbid}) if mbid else parameters.update(named_params)
        return parameters

    def _lastfm_request(self, parameters):
        response = requests.get(URL, headers=HEADERS, params=parameters)

        if response.status_code != 200:
            print(f"{response.status_code}: {response.text}")
            del parameters['api_key'], parameters['method'], parameters['autocorrect'], parameters['format']
            # print(parameters)
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
        artist_info.update({'tags': [tag['name'] for tag in artist.get('tags', {}).get('tag', [])]})
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
        album_info['songs'] = self._extract_song_info(album.get('tracks', {}))
        return album_info

    def _extract_song_info(self, songs):
        songs = songs.get('track', {})
        song_info = []
        for song in songs:
            if not isinstance(song, dict):
                continue
            rank = song.get('@attr', {}).get('rank', None)
            name = song.get('name', None)
            duration = song.get('duration', None)
            song_info.append({'name': name, 'duration': duration, 'rank': rank})
        return song_info