from utils.utils import read_from_json, write_to_json


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



def prepare_genre(data: dict) -> list[tuple]:
    """
    Prepares genre data from the provided dictionary and filters genres containing
    specific keywords.

    Args: 
        data (dict): A dictionary containing genre data.
    
    Returns:
        A list of tuples containing filtered genre data formatted for feeding the `genre`
        table in the `metal_db` database.
    """
    genre_data = [(v.lower(), k) for k, v in data.items() if any(
        gnr in str(v).lower() for gnr in ("rock", "grindcore", "death", "doom", "gothic", "metal", "djent")
        )]
    return genre_data


def prepare_musician(data: dict, labels: dict) -> list[tuple]:
    """
    Prepares musician data from the provided dictionary.

    Args: 
        data (dict): A dictionary containing data about musicians.
        labels (dict): A dictionary containing label data for musician IDs.
    
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
            ]
        )
    # Iterate over each item in the items dictionary
    # and filter items based on the condition
    for item_code, item_data in data.items()
    if any(
        code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys())
        for code in ("Q1792372", "Q36834", "Q5")
    )
    # Extract the instrument value from the inner dictionary
    for instrument in (item.get("instrument") for item in item_data.values())
    ]
    return musician_data


def prepare_band(data: dict, labels: dict) -> list[tuple]:
    """
    Prepares band data from the provided dictionary and filters bands from
    specific categories.

    Args: 
        data (dict): A dictionary containing band data.
    
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
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in \
           ("Q19351429", "Q7623897", "Q1400264", "Q106581115", "Q105756328", "Q56816954", "Q6942541", "Q9212979", 
            "Q2088357", "Q215380", "Q281643", "Q713200", "Q5741069", "Q7558495", "Q215048", "Q1190668"))
]
    return band_data


def prepare_album(data: dict, labels: dict) -> list[tuple]:
    """
    Prepares album data from the provided dictionary and filters albums from
    specific categories.

    Args: 
        data (dict): A dictionary containing album data.
    
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
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in \
           ("Q482994", "Q108352648", "Q20671381", "Q107154516", "Q217199", "Q169930", "Q368281", "Q60713210", "Q208569", "Q10590726"))
    ]
    return album_data


def prepare_song(data: dict, labels: dict) -> list[tuple]:
    """
    Prepares song data from the provided dictionary and filters songs from
    specific categories.

    Args: 
        data (dict): A dictionary containing song data.
    
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
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in \
           ("Q7302866", "Q155171", "Q220935", "Q59847891", "Q4132319", "Q55850593", "Q55850643", "Q193977", 
           "Q105543609", "Q56599584", "Q134556", "Q108352496", "Q7366", "Q58232557", "Q677466"))
    ]
    return song_data


def prepare_junction_data(data: dict, keys_1: list, keys_2: list, label: str) -> list[tuple]:
    keys_2 = set(keys_2 + keys_1)
    return [
        (key, inner_dict[f"{label}_wikidata"])
        for key in keys_2
        for _, inner_dict in data.get(key, {}).items()
        if f"{label}_wikidata" in inner_dict
    ]


def main() -> dict:
    """
    Executes the main functionality of the script.

    Returns:
        dict: A dictionary containing prepared data for genres, musicians, bands, albums, and songs.
    """

    genre = read_from_json("data/processed/genre_details.json")
    items = read_from_json("data/processed/detailed_items.json")
    labels = read_from_json("data/raw/metal_items.json")
    items = format_items(items)
    write_to_json(items, "data/processed/formatted_detailed_items.json")
    genres = prepare_genre(genre)
    musicians = prepare_musician(items, labels)
    bands = prepare_band(items, labels)
    albums = prepare_album(items, labels)
    songs = prepare_song(items, labels)
    
    return {'genres': genres, 'musicians': musicians, 'bands': bands, 'albums': albums, 'songs': songs}


if __name__ == '__main__':
    main()