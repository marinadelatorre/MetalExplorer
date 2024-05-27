import sys
import os
import urllib.error

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

from src.utils.utils import read_from_json, write_to_json

endpoint_url = "https://query.wikidata.org/sparql"


def execute_query(endpoint_url: str, query: str) -> dict:
    """
    Executes a SPARQL query against the specified endpoint URL and returns the results.

    Args:
        endpoint_url (str): The URL of the SPARQL endpoint to query.
        query (str): The SPARQL query string to execute.

    Returns:
        dict: A dictionary containing the results of the SPARQL query.
    """
    user_agent = "MetalProject/0.1 %s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def fetch_items_from_query(query: str, query_parameters: dict) -> dict:
    """
    Executes a SPARQL query with the provided parameters to fetch items from Wikidata.

    Args:
        query (str): The SPARQL query template to execute, with placeholders for parameters.
        query_parameters (dict): A dictionary containing parameters to be substituted into the query.

    Returns:
        dict: A dictionary containing the results of the SPARQL query, with item identifiers as keys.
    """
    query = query.format(**query_parameters)
    results = execute_query(endpoint_url, query)
    return results


def fetch_item_details(item_query: str, item: str):
    """
    Fetches details for a specific item from Wikidata based on the provided query.

    Args:
        item_query (str): The SPARQL query to execute for fetching details of the item.
        item (str): The identifier of the item for which details are to be fetched.

    Returns:
        dict: A dictionary containing the fetched details of the item, with the item identifier as the key.
    """
    query_parameters = {'item': item}
    details = {item: fetch_items_from_query(item_query, query_parameters)}
    return details


def parse_genre_results(genre_query: dict) -> list:
    """
    Extracts genre values from the results of a genre query.

    Args:
        genre_query (dict): The query results containing genre information.

    Returns:
        list: A list of genre values extracted from the query results.
    """
    genre_result = [gnr["genre"]["value"].split("/")[-1] for gnr in genre_query["results"]["bindings"]]
    return genre_result


def parse_item_results(item_query: dict) -> dict:
    """
    Extracts item details from the results of an item query.

    Args:
        item_query (dict): The query results containing item information.

    Returns:
        dict: A dictionary of item identifiers mapped to their corresponding details.
    """
    item_result = {item["item"]["value"].split("/")[-1]: item["itemLabel"]["value"] for item in item_query["results"]["bindings"]}
    return item_result


def main():
    """
    Orchestrates the data gathering process for the MetalExplorer project.

    This function coordinates the execution of the data gathering script, which involves querying Wikidata
    to gather genre information, fetching items based on genres, and retrieving details for each item. The fetched
    data is then processed and saved to JSON files.

    Usage:
        Run this script to initiate the data gathering process for the MetalExplorer project.

    """
    if not os.path.isdir("config/"):
        os.mkdir("config/")
    queries = read_from_json('config/queries.json')
    genre_results = execute_query(endpoint_url, queries['query_genre'])
    genres = parse_genre_results(genre_results)

    assorted_items = {}
    for genre in genres:
        try:
            items = fetch_items_from_query(queries['query_items'], {'genre': genre})
            items = parse_item_results(items)
            assorted_items.update(items)
        except urllib.error.HTTPError as e:
            print(f"HTTP Error fetching items for genre '{genre}': {e}")
        except Exception as e:
            print(f"Error fetching items for genre '{genre}': {e}")
    
    if not os.path.isdir("data/raw/"):
        os.mkdir("data/raw/")
    write_to_json(assorted_items, "data/raw/metal_item_labels.json")

    item_details = {}
    save_threshold = 5
    for item, _ in tqdm(assorted_items.items(), total=len(assorted_items)): 
        try:
            details = fetch_item_details(queries['query_details'], item)
            item_details.update(details)
            if (len(item_details) / len(assorted_items) * 100) > save_threshold:
                write_to_json(item_details, "data/raw/metal_item_details_temp.json")
                save_threshold += 5
        except urllib.error.HTTPError as e:
            print(f"HTTP Error fetching details for item '{item}': {e}")
        except Exception as e:
            print(f"Error fetching details for item '{item}': {e}")

    write_to_json(item_details, "data/raw/metal_item_details.json")
    os.remove("data/raw/metal_item_labels_temp.json")


if __name__ == "__main__":
    main()