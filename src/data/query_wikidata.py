import sys
import json

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

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


def merge_dicts(dict1, dict2):
    merged_dict = {**dict1, **dict2}
    return merged_dict


def read_from_file(file_path: str) -> dict:
    """
    Reads data from a JSON file and returns it as a dictionary.

    Args:
        file_path (str): The path to the JSON file to read.

    Returns:
        dict: A dictionary containing the data read from the JSON file.
    """
    with open(file_path) as json_file:
        queries = json.load(json_file)
    return queries


def write_to_file(data: dict, file_path: str) -> None:
    """
    Writes data to a JSON file.

    Args:
        data (dict): The dictionary containing the data to write.
        file_path (str): The path to the JSON file to write.

    Returns:
        None
    """
    with open(file_path, "w") as outfile: 
        json.dump(data, outfile, indent=2, ensure_ascii=False)


def main():
    """
    Orchestrates the data gathering process for the MetalExplorer project.

    This function coordinates the execution of the data gathering script, which involves querying Wikidata
    to gather genre information, fetching items based on genres, and retrieving details for each item. The fetched
    data is then processed and saved to JSON files.

    Usage:
        Run this script to initiate the data gathering process for the MetalExplorer project.

    """
    queries = read_from_file('config/queries.json')
    genre_results = execute_query(endpoint_url, queries['query_genre'])
    genres = parse_genre_results(genre_results)

    assorted_items = {}
    for genre in genres:
        items = fetch_items_from_query(queries['query_items'], {'genre': genre})
        items = parse_item_results(items)
        assorted_items.update(items)
    
    write_to_file(assorted_items, "raw/metal_item_labels.json")

    item_details = {}
    for item, _ in tqdm(assorted_items.items(), total=len(assorted_items)): 
        details = fetch_item_details(queries['query_details'], item)
        item_details.update(details)

    write_to_file(item_details, "raw/metal_item_details.json")


if __name__ == "__main__":
    main()