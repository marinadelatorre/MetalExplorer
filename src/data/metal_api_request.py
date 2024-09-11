import json
import requests
import requests_cache
import time

from tqdm import tqdm
from utils.utils import read_text, write_to_json, is_cached_result


PROJECT_VERSION = read_text('VERSION').strip()
HEADERS = {
            'accept': 'application/json',
            'user-agent': f'metalexplorer/{PROJECT_VERSION}',
        }
requests_cache.install_cache('metal_api_cache')


class MetalAPIRequest:

    def __init__(self, identifiers, save_data=True) -> None:
        self.identifiers = identifiers
        self.save_data = save_data
        self.ids = self._extract_ids()


    def _request_from_api(self, endpoint, id):
        response = requests.get(f'https://metal-api.dev/{endpoint}/{id}', headers=HEADERS)
        
        if response.status_code != 200:
            print(f"""{response.status_code}: {json.loads(response.text)['title']} 
                  Endpoint: {endpoint}, id: {id}""")
            return None            

        return response
    

    def _get_data_by_category(self, category):
        category_data = []

        for id in tqdm(self.ids[category], total=len(self.ids[category]), \
                       desc=f"Fetching data for {category}s"):
            response = self._request_from_api(f'{category}s', id)
            data = json.loads(response.text) if response else None
            category_data.append(data) if response else None
            write_to_json(data, f'data/raw/{category}/metal_api/{id}.json') \
                if self.save_data and response else None
            time.sleep(2) if not is_cached_result(response) else None

        return category_data


    def get_data(self):
        self.data = {
            category: self._get_data_by_category(category) for category in self.ids.keys()
            }
        return self.data
    

    def _extract_ids(self):
        ids = {category: set() for category in self.identifiers.keys() if category in ("band", "album")}

        for category, values in self.identifiers.items():
            [ids[category].add(ids_dict["emid"]) \
                for _, ids_dict in values.items() \
                if "emid" in ids_dict.keys() and category in ("band", "album")]
        return ids
