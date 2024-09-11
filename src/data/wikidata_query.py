import sys
import os
import urllib.error

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

from utils.utils import read_from_json, write_to_json, read_text

ENDPOINT_URL = "https://query.wikidata.org/sparql"


class WikidataQuery:
    def __init__(self):
        queries = read_from_json('config/queries.json')
        self.query_genre = read_text(queries.get('query_genre', None))
        self.query_genre_filter_date = read_text(queries.get('query_genre_filter_date', None))
        self.query_items = read_text(queries.get('query_items', None))
        self.query_items_filter_date = read_text(queries.get('query_items_filter_date', None))
        self.query_details = read_text(queries.get('query_details', None))
        self.query_details_filter_date = read_text(queries.get('query_details_filter_date', None))
        self.categories = read_from_json("config/category_data.json")


    def _execute_query(self, query) -> dict:
        """
        Executes a SPARQL query against the specified endpoint URL and returns the results.

        Returns:
            dict: A dictionary containing the results of the SPARQL query.
        """
        project_version = read_text('VERSION').strip()
        user_agent = "MetalProject/%s %s.%s" % (project_version, sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(ENDPOINT_URL, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    
    def _parse_genre_results(self, genre_response: dict) -> list:
        """
        Extracts genre values from the results of a genre query.

        Args:
            genre_response (dict): The query results containing genre information.

        Returns:
            list: A list of genre values extracted from the query results.
        """
        genres = {genre["genre"]["value"].split("/")[-1]: genre["label"]["value"] for genre in genre_response["results"]["bindings"]}
        return genres
    

    def _fetch_items_from_query(self, query, query_parameters: dict) -> dict:
        """
        Executes a SPARQL query with the provided parameters to fetch items from Wikidata.

        Args:
            query (str): The SPARQL query template to execute, with placeholders for parameters.
            query_parameters (dict): A dictionary containing parameters to be substituted into the query.

        Returns:
            dict: A dictionary containing the results of the SPARQL query, with item identifiers as keys.
        """
        query = query.format(**query_parameters)
        results = {query_parameters['item']: self._execute_query(query)} \
            if 'item' in query_parameters.keys() \
            else self._execute_query(query) 
        return results
    

    def _parse_item_results(self, items_response: dict) -> dict:
        """
        Extracts item details from the results of an item query.

        Args:
            items_response (dict): The query results containing item information.

        Returns:
            dict: A dictionary of item identifiers mapped to their corresponding details.
        """
        item_result = {item["typeLabel"]["value"]: {} for item in items_response["results"]["bindings"]}
        [item_result[item["typeLabel"]["value"]].update({item["item"]["value"].split("/")[-1]: item["itemLabel"]["value"]}) \
            for item in items_response["results"]["bindings"]]
        return item_result
    

    def _select_category(self, item_type: str) -> str:
        return self.categories.get(item_type, None)
    

    def get_genres(self, save=True) -> list:
        genre_response = self._execute_query(self.query_genre)
        self.genres = self._parse_genre_results(genre_response)
        write_to_json(self.genres, 'data/raw/wikidata_genres.json') if save else None
        return self.genres


    def _get_item_labels_by_genre(self, genre):
        try:
            items = self._fetch_items_from_query(self.query_items, {'genre': genre})
            items = self._parse_item_results(items)
        except urllib.error.HTTPError as e:
            print(f"HTTP Error fetching items for genre '{genre}': {e}")
        except Exception as e:
            print(f"Error fetching items for genre '{genre}': {e}")
        return items
    

    def _clean_item_labels(self):
        for item in list(self.all_items.keys()):
            if item not in self.categories.keys():
                self.all_items.pop(item)
            elif item not in self.categories.values():
                self.all_items[self.categories[item]].update(self.all_items.get(item)) \
                    if self.categories[item] in self.all_items.keys() \
                    else self.all_items.update({self.categories[item]: self.all_items.get(item)})
                self.all_items.pop(item)

    def get_all_item_labels(self, save=True):
        self.all_items = {}
        {self.all_items.update(self._get_item_labels_by_genre(genre)) \
                          for genre in tqdm(list(self.genres.keys()), \
                                            total=len(self.genres), \
                                            desc="Fetching items by genre")}
        print("Total number of items", len(self.all_items))
        write_to_json(self.all_items, f"data/raw/wikidata_all_item_labels.json")
        self._clean_item_labels()
        
        [write_to_json(item, f"data/raw/{type_label}/wikidata_item_labels.json") \
            for type_label, item in self.all_items.items()] \
            if save else None
        return self.all_items


    def _get_details_by_type(self, items):
        item_details = {}
        for item, label in tqdm(items.items(), total=len(items), desc="Fetching item details"): 
            try:
                details = self._fetch_items_from_query(self.query_details, {'item': item})
                item_details.update({label: details})
            except urllib.error.HTTPError as e:
                print(f"HTTP Error fetching details for item '{item}': {e}")
            except Exception as e:
                print(f"Error fetching details for item '{item}': {e}")
        return item_details


    def get_all_items_details(self, save=True):
        self.all_items_details = {
            type_label: self._get_details_by_type(items)
            for type_label, items in self.all_items.items()
        }
        [write_to_json(item, f"data/raw/{type_label}/wikidata_items_details.json") \
            for type_label, item in self.all_items_details.items()] \
            if save else None
        return self.all_items_details
    

    def _get_identifiers_by_category(self, value):
        index = {}
        for id, details in value.items():
            unpacked_details = details['results']['bindings'][0]
            if unpacked_details.get("mbid", None):
                index.update({id: {}}) if id not in index else None
                index[id].update({'mbid': unpacked_details["mbid"]["value"]})
            if unpacked_details.get("emid", None):
                index.update({id: {}}) if id not in index else None
                index[id].update({'emid': unpacked_details["emid"]["value"]})
        return index
    

    def get_identifiers(self, save=True):
        self.identifiers = {category: {} for category in set(self.categories.values())}

        for category in set(self.categories.values()):
            [self.identifiers[category].update(self._get_identifiers_by_category(value)) \
            for _, value in self.all_items_details[category].items()]

            write_to_json(self.identifiers[category], f"data/raw/{category}/wikidata_identifiers.json") \
            if save and len(self.identifiers[category]) > 0 else self.identifiers.pop(category)
        
        return self.identifiers