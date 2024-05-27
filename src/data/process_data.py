def format_items(data: dict) -> dict:
    """
    Formats the items dictionary by removing empty dictionaries and converting
    the inner dictionaries to lists.

    Args:
        data (dict): A dictionary containing item data.

    Returns:
        dict: A formatted dictionary containing item data.
    """
    new_data = {}
    for outer_key, outer_value in data.items():
        new_data[outer_key] = {}
        for key, value in outer_value.items():
            new_data[outer_key][key] = {}
            new_data[outer_key][key] = {k: val.keys() for k, val in value.items()}
            for k, val in value.items():
                if len(val.keys()) > 1:
                    new_data[outer_key][key][k] = [v for v in val.keys()]
                else:
                    new_data[outer_key][key][k] = list(val.keys())[0]
    return new_data



def process_genre(data: dict, genres: list) -> list[tuple]:
    """
    Processes genre data from the provided dictionary and filters genres containing
    specific keywords.

    Args: 
        data (dict): A dictionary containing genre data.
        genres (list): A list of keywords to filter genres by.
    
    Returns:
        A list of tuples containing filtered genre data formatted for feeding the `genre`
        table in the `metal_db` database.
    """
    genre_data = [(v.lower(), k) for k, v in data.items() if any(
        gnr in str(v).lower() for gnr in genres
        )]
    return genre_data


def process_musician(data: dict, labels: dict, musician_codes: list) -> list[tuple]:
    """
    Processes musician data from the provided dictionary.

    Args: 
        data (dict): A dictionary containing data about musicians.
        labels (dict): A dictionary containing label data for musician IDs.
        musician_codes (list): A list of musician codes to filter musicians by.
    
    Returns:
        A list of tuples containing filtered musician data formatted for feeding the `musician`
        table in the `metal_db` database.
    """
    
    musician_data = [
        tuple(
            [
                item_code,  # Wikidata_id
                labels.get(item_code, ""),  # Name
                *(
                    # If only one instrument, extract it from the list and pad with None
                    [instrument] + [None] * (5 - 1) if isinstance(instrument, str) else
                    # If multiple instruments, use them and pad with None
                    instrument + [None] * (5 - len(instrument)) if isinstance(instrument, list) else
                    # If no instrument, fill with None
                    [None] * 5
                )
            ][:7]
        )
    # Iterate over each item in the items dictionary
    # and filter items based on the condition
    for item_code, item_data in data.items()
    if any(
        code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys())
        for code in musician_codes
    )
    # Extract the instrument value from the inner dictionary
    for instrument in (item.get("instrument") for item in item_data.values())
    ]
    return musician_data


def process_band(data: dict, labels: dict, band_codes: list) -> list[tuple]:
    """
    Processes band data from the provided dictionary and filters bands from
    specific categories.

    Args: 
        data (dict): A dictionary containing band data.
        labels (dict): A dictionary containing label data for band IDs.
        band_codes (list): A list of band codes to filter bands by.
    
    Returns:
        A list of tuples containing filtered band data formatted for feeding the `band`
        table in the `metal_db` database.
    """
    band_data = [
    (
        labels.get(item_code, ""),
        item.get("country") if isinstance(item.get("country"), str) else \
            item.get("country", " ")[0] if isinstance(item.get("country"), list) else \
            None, 
        item_code, 
        item.get("start") if isinstance(item.get("start"), str) else \
            item.get("start", " ")[0] if isinstance(item.get("start"), list) else \
            None,
        item.get("end") if isinstance(item.get("end"), str) else \
        item.get("end", " ")[-1] if isinstance(item.get("end"), list) else \
            None
    )
    for item_code, item_data in data.items()
    for item in item_data.values()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) \
           for code in band_codes)
]
    return band_data


def process_album(data: dict, labels: dict, album_codes: list) -> list[tuple]:
    """
    Processes album data from the provided dictionary and filters albums from
    specific categories.

    Args: 
        data (dict): A dictionary containing album data.
        labels (dict): A dictionary containing label data for album IDs.
        album_codes (list): A list of album codes to filter albums by.
    
    Returns:
        A list of tuples containing filtered album data formatted for feeding the `album`
        table in the `metal_db` database.
    """
    album_data = [
    (
        labels.get(item_code, "")[:100],
        item.get("performer_wikidata") if isinstance(item.get("performer_wikidata"), str) else \
            item.get("performer_wikidata", " ")[0] if isinstance(item.get("performer_wikidata"), list) else \
            None, 
        item.get("publicationdate") if isinstance(item.get("publicationdate"), str) else \
            item.get("publicationdate", " ")[0] if isinstance(item.get("publicationdate"), list) else \
            None,
        item.get("duration") if isinstance(item.get("duration"), str) else \
            item.get("duration", " ")[0] if isinstance(item.get("duration"), list) else \
            None,
        item.get("item") if isinstance(item.get("item"), str) else \
            item.get("item", " ")[0] if isinstance(item.get("item"), list) else \
            None,
        item_code
    )
    for item_code, item_data in data.items()
    for item in item_data.values()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") \
           for idx in item_data.keys()) for code in album_codes)
    ]
    return album_data


def process_song(data: dict, labels: dict, song_codes: list) -> list[tuple]:
    """
    Processes song data from the provided dictionary and filters songs from
    specific categories.

    Args: 
        data (dict): A dictionary containing song data.
        labels (dict): A dictionary containing label data for song IDs.
        song_codes (list): A list of song codes to filter songs by.
    
    Returns:
        A list of tuples containing filtered song data formatted for feeding the `song`
        table in the `metal_db` database.
    """
    song_data = [
    (
        labels.get(item_code, "")[:50],
        item.get("performer_wikidata") if isinstance(item.get("performer_wikidata"), str) else \
            item.get("performer_wikidata", " ")[0] if isinstance(item.get("performer_wikidata"), list) else \
            None, 
        item.get("album_wikidata") if isinstance(item.get("album_wikidata"), str) else \
            item.get("album_wikidata", " ")[0] if isinstance(item.get("album_wikidata"), list) else \
            None,
        item.get("duration") if isinstance(item.get("duration"), str) else \
            item.get("duration", " ")[0] if isinstance(item.get("duration"), list) else \
            None,
        item_code
    )
    for item_code, item_data in data.items()
    for item in item_data.values()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") \
           for idx in item_data.keys()) for code in song_codes)
    ]
    return song_data


def process_junction_data(data: dict, keys_1: list, keys_2: list, label: str) -> list[tuple]:
    """
    Processes the data from the provided dictionary to create pairs of keys for the junction table.

    Args:
        data (dict): A dictionary containing the data to process.
        keys_1 (list): A list of keys to use for the first column of the junction table.
        keys_2 (list): A list of keys to use for the second column of the junction table.
        label (list): Label to retrieve data from the inner dictionary.

    Returns:
        A list of tuples containing the key pairs for the junction table. 
    """
    keys_2 = set(keys_2 + keys_1)
    junction_data = [(key, inner_dict[f"{label}_wikidata"])
        for key in keys_2
        for _, inner_dict in data.get(key, {}).items()
        if f"{label}_wikidata" in inner_dict]
    flattened_data = [(str(key), str(value)) for key, values in junction_data for value in (values if isinstance(values, list) else [values])]
    return flattened_data