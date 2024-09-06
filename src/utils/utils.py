import json
import os


def read_from_json(data_file: str) -> dict:
    """
    Reads data from a JSON file and returns it as a dictionary.

    Args:
        file_path (str): The path to the JSON file to read.

    Returns:
        dict: A dictionary containing the data read from the JSON file.
    """
    with open(data_file) as json_file:
        data = json.load(json_file)
    return data


def write_to_json(data: dict, file_path: str) -> None:
    """
    Writes data to a JSON file.

    Args:
        data (dict): The dictionary containing the data to write.
        file_path (str): The path to the JSON file to write.

    Returns:
        None
    """
    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
    with open(file_path, "w") as outfile: 
        json.dump(data, outfile, indent=2, ensure_ascii=False)


def read_text(file_path):
    with open(file_path, 'r') as file:
        return file.read()
