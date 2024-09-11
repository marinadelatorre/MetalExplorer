import json
import requests
import requests_cache
import time

from utils.utils import read_text, write_to_json


PROJECT_VERSION = read_text('VERSION').strip()
HEADERS = {
            'accept': 'application/json',
            'user-agent': f'metalexplorer/{PROJECT_VERSION}',
        }
requests_cache.install_cache()

class MetalAPIRequest:

    def __init__(self, identifiers) -> None:
        self.identifiers = identifiers
        self.ids = self._extract_ids()

    def _request_from_api(self, endpoint, id):
        response = requests.get(f'https://metal-api.dev/{endpoint}/{id}', headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error {response.status_code} for endpoint: {endpoint}, id: {id}")
            return None            

        data = json.loads(response._content)
        return data
    

    def _is_cached_result(self, response):
        return getattr(response, 'from_cache', False)


    def get_data(self, save=True):
        self.data = {category: {} for category in self.ids.keys()}

        for category, ids in self.ids.items():
            for id in ids:
                response = self._request_from_api(f'{category}s', id)
                if response:
                    self.data[category].update(response)
                    write_to_json(self.data[category], f'data/raw/{category}/metal_api/{id}.json') \
                        if save else None
                    time.sleep(1) if not self._is_cached_result(response) else None

        return self.data
    

    def _extract_ids(self):
        ids = {category: set() for category in self.identifiers.keys() if category in ("band", "album")}

        for category, values in self.identifiers.items():
            [ids[category].add(ids_dict["emid"]) \
                for _, ids_dict in values.items() \
                if "emid" in ids_dict.keys() and category in ("band", "album")]
        print(ids)
        return ids
