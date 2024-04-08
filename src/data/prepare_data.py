from src.utils.utils import read_from_json


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
    (
        item_code, # Wikidata_id
        labels.get(item_code, ""), # Name
        # Unique Instruments (filtered to remove None values and then converted to a list to preserve order)     
        *list(dict.fromkeys(filter(None, [item.get("instrument") for item in item_data.values()])))
    )
    # Iterate over each item in the items dictionary and
    # Filter items based on the condition
    for item_code, item_data in data.items()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) 
           for code in ("Q1792372", "Q36834", "Q5"))
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
        item_code,
        *list(dict.fromkeys(filter(None, [(labels.get(item_code, ""), item.get("country"), item_code, item.get("start"), \
                                           item.get("end")) for item in item_data.values()])))
    )
    for item_code, item_data in data.items()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in \
           ("Q19351429", "Q7623897", "Q1400264", "Q106581115", "Q105756328", "Q56816954", "Q6942541", "Q9212979", 
            "Q2088357", "Q215380", "Q281643", "Q713200", "Q5741069", "Q7558495", "Q215048", "Q1190668"))
]
    band_data = [band[1] for band in band_data]
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
        item_code,
        *list(dict.fromkeys(filter(None, [(labels.get(item_code, ""), item.get("performer_wikidata"), \
                                           item.get("publication_date"), item.get("duration"), item.get("item"), item_code) \
                                            for item in item_data.values()])))
    )
    for item_code, item_data in data.items()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in (
        "Q482994", "Q108352648", "Q20671381", "Q107154516", "Q217199", "Q368281", "Q60713210", "Q208569", "Q10590726"
        ))
]
    album_data = [album[1] for album in album_data]
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
        item_code,
        *list(dict.fromkeys(filter(None, [(labels.get(item_code, ""), item.get("performer_wikidata"), \
                                           item.get("album_wikidata"), item.get("duration"), item_code) \
                                            for item in item_data.values()])))
    )
    for item_code, item_data in data.items()
    if any(code in (item_data.get(idx, {}).get("item_wikidata", "") for idx in item_data.keys()) for code in (
        "Q7302866", "Q155171", "Q220935", "Q59847891", "Q4132319", "Q169930", "Q55850593", "Q55850643", "Q193977", 
        "Q105543609", "Q56599584", "Q134556", "Q108352496", "Q7366", "Q58232557", "Q677466"))
]
    song_data = [song[1] for song in song_data]
    return song_data


def main() -> dict:
    """
    Executes the main functionality of the script.

    Returns:
        dict: A dictionary containing prepared data for genres, musicians, bands, albums, and songs.
    """

    genre = read_from_json("data/processed/genre_details.json")
    items = read_from_json("data/processed/detailed_items.json")
    labels = read_from_json("data/raw/metal_items.json")

    genres = prepare_genre(genre)
    musicians = prepare_musician(items, labels)
    bands = prepare_band(items, labels)
    albums = prepare_album(items, labels)
    songs = prepare_song(items, labels)

    return {'genres': genres, 'musicians': musicians, 'bands': bands, 'albums': albums, 'songs': songs}


if __name__ == '__main__':
    main()