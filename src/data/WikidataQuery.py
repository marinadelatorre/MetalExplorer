import sys
import os
import urllib.error

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

from utils.utils import read_from_json, write_to_json, read_text

ENDPOINT_URL = "https://query.wikidata.org/sparql"


class WikidataQuery:
    def __init__(self):
        """
        Args:
            query (str): The SPARQL query string to execute.
        """
        queries = read_from_json('config/queries.json')
        self.query_genre = read_text(queries.get('query_genre', None))
        self.query_genre_filter_date = read_text(queries.get('query_genre_filter_date', None))
        self.query_items = read_text(queries.get('query_items', None))
        self.query_items_filter_date = read_text(queries.get('query_items_filter_date', None))
        self.query_details = read_text(queries.get('query_details', None))
        self.query_details_filter_date = read_text(queries.get('query_details_filter_date', None))


    def _execute_query(self) -> dict:
        """
        Executes a SPARQL query against the specified endpoint URL and returns the results.

        Returns:
            dict: A dictionary containing the results of the SPARQL query.
        """
        project_version = read_text('VERSION').strip()
        user_agent = "MetalProject/% %s.%s" % (project_version, sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(ENDPOINT_URL, agent=user_agent)
        sparql.setQuery(self.query)
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
        genres = [gnr["genre"]["value"].split("/")[-1] for gnr in genre_response["results"]["bindings"]]
        return genres
    

    def _fetch_items_from_query(self, query: str, query_parameters: dict) -> dict:
        """
        Executes a SPARQL query with the provided parameters to fetch items from Wikidata.

        Args:
            query (str): The SPARQL query template to execute, with placeholders for parameters.
            query_parameters (dict): A dictionary containing parameters to be substituted into the query.

        Returns:
            dict: A dictionary containing the results of the SPARQL query, with item identifiers as keys.
        """
        query = query.format(**query_parameters)
        results = self._execute_query(query)
        return results
    

    def _parse_item_results(item_query: dict) -> dict:
        """
        Extracts item details from the results of an item query.

        Args:
            item_query (dict): The query results containing item information.

        Returns:
            dict: A dictionary of item identifiers mapped to their corresponding details.
        """
        item_result = {item["item"]["value"].split("/")[-1]: item["itemLabel"]["value"] for item in item_query["results"]["bindings"]}
        return item_result


    def _fetch_item_details(self, item_query: str, query_parameters: dict):
        """
        Executes a SPARQL query for a specific item from Wikidata based on the provided parameters.

        Args:
            item_query (str): The SPARQL query to execute for fetching details of the item.
            query_parameters (dict): A dictionary containing parameters to be substituted into 
            the query, including the identifier of the item for which details are to be fetched.

        Returns:
            dict: A dictionary containing the fetched details of the item, with the item identifier as the key.
        """
        query = item_query.format(**query_parameters)
        details = {query_parameters['item']: self._execute_query(query)}
        return details
    

    def get_genres(self) -> list:
        genre_response = self._execute_query()
        self.genres = self._parse_genre_results(genre_response)
        return self.genres


    def get_item_labels_by_genre(self, genre):
        genre_items = {}
        try:
            items = self._fetch_items_from_query(self.query_items, {'genre': genre})
            items = self._parse_item_results(items)
            genre_items.update(items)
        except urllib.error.HTTPError as e:
            print(f"HTTP Error fetching items for genre '{genre}': {e}")
        except Exception as e:
            print(f"Error fetching items for genre '{genre}': {e}")
        return items
    

    def get_all_item_labels(self):
        self.all_items = {genre: self.get_items_by_genre(genre) for genre in tqdm(self.genres, total=len(self.genres))}
        return self.all_items


    def get_items_details(self, items):
        item_details = {}
        for item, label in tqdm(items.items(), total=len(items)): 
            try:
                details = self._fetch_item_details(self.query_details, {'item': item})
                item_details.update({label: details})
            except urllib.error.HTTPError as e:
                print(f"HTTP Error fetching details for item '{item}': {e}")
            except Exception as e:
                print(f"Error fetching details for item '{item}': {e}")
        return item_details


    def get_all_items_details(self):
        self.all_items_details = {
            genre: self.get_items_details(items)
            for genre, items in self.all_items.items()
        }
        return self.all_items_details