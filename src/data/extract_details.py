from utils.utils import read_from_json, write_to_json

     
def extract_data(results: list[dict]) -> dict:
    """
    Extracts relevant data from the results obtained from a SPARQL query.

    Args:
        results (list of dict): The results obtained from the SPARQL query.

    Returns:
        dict: A dictionary containing the extracted data, with item identifiers as keys.

    """
    extracted_data = {}
    item_data = {}
    for result in results:
        item_label = result['item']['value'].split('/')[-1]
        extracted_data[item_label] = item_data
        for label, value in result.items():
            label = label.replace('Label', '') if label.endswith('Label') or \
                label in ('start', 'end', 'duration', 'publicationdate') else label+'_wikidata'
            item_data[label] = {} if label not in item_data.keys() else item_data[label]
            if isinstance(value, dict) and label in ('start', 'end', 'publicationdate'):
                item_data[label][value['value'].split('T')[0]] = {}
            else:
                item_data[label][value.get('value', None).split('/')[-1]] = {}
    return extracted_data


def main():
    """
    Orchestrates heavy metal data extraction from Wikidata files.

    Reads data from a JSON file containing SPARQL query results, extracts relevant information, 
    organizes it into dictionaries based on type of information, and writes the extracted data into separate JSON files.

    The process involves the following steps:
    1. Reading data from the 'metal_item_details.json' file, from the `data/raw` directory.
    2. Extracting relevant data from the results obtained.
    3. Organizing the extracted data into dictionaries based on type of information (e.g., items, genres, performers).
    4. Writing the organized data into separate JSON files for each dictionary.
    5. Writing the overall extracted data into the 'metal_items.json' file, in the `data/processed` directory.

    Note: This function assumes that the 'metal_item_details.json' file contains the SPARQL query results 
    obtained from Wikidata, formatted as JSON.

    """
    raw_results = read_from_json('data/raw/metal_item_details.json')

    result_dict = {}

    item_dict = {}
    genre_dict = {}
    performer_dict = {}
    member_dict = {}
    country_dict = {}
    instrument_dict = {}
    collection_dict = {'item': item_dict, 'genre': genre_dict, 'performer': performer_dict,
                       'member': member_dict, 'country': country_dict, 'instrument': instrument_dict}

    for label, results in raw_results.items():
        result_dict[label] = extract_data(results['results']['bindings'])

    # Loop through the result items and assign item to correct dictionary according to type,
    # using ID from Wikidata as key for the item and the label as its value.
    for data_type, data_dict in collection_dict.items():
        for _, value in result_dict.items():
            for _, v in value.items():
                if any("wikidata" in word for word in v.keys()) and data_type in v.keys():
                    data_dict.update(dict(zip(v.get(f"{data_type}_wikidata").keys(), v.get(data_type, {}).keys())))
                # data_dict[next(iter(v.get(f'{data_type}_wikidata', data_type)))] = v.get(data_type, None)
    
    for data_type, data_dict in collection_dict.items():
        data_dict.pop(None, None)
        write_to_json(data_dict, f"data/processed/{data_type}_details.json")

    write_to_json(result_dict, "data/processed/detailed_items.json")


if __name__ == "__main__":
    main()