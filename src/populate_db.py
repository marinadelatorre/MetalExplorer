import os
import mysql.connector

from dotenv import load_dotenv

from src.data.process_data import (
    format_items,
    process_genre, 
    process_musician, 
    process_band, 
    process_album, 
    process_song,
    process_junction_data)
from src.utils.utils import read_from_json


def connect_to_database():
    """Connect to the MySQL database."""
    load_dotenv('config/.env')
    
    try:
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            database=os.environ.get("DB_NAME")
        )        
        return conn
    
    except mysql.connector.Error as e:
        print("Connection error:", e)
        return None


def fetch_referenced_ids(cursor, table: str, identifiers: list) -> dict:
    """
    Fetch the auto-incremented ids from the referenced table based on a condition.

    Args:
        cursor: The MySQL cursor object.
        table: The name of the table to fetch the foreign keys from.
        identifiers: A list of values to use in the condition for fetching IDs.

    Returns:
        A dictionary of IDs retrieved from the referenced table in the format
        {wikidata_id: id}.
    """


    sql = f"SELECT wikidata_id, id FROM {table} WHERE wikidata_id IN ({', '.join(['%s'] * len(identifiers))})"
    cursor.execute(sql, tuple(identifiers))
    results = cursor.fetchall()
    return {result[0]: str(result[1]) for result in results}


def replace_foreign_keys(cursor, table: str, idx: int, data: list[tuple]) -> list[tuple]:
    """
    Replace wikidata_ids as foreign keys to the data based on the referenced table's IDs.

    Args:
        cursor: The MySQL cursor object.
        table: The name of the table to fetch the foreign keys from.
        idx: The column index to fetch the foreign keys for.
        data: The data to add the foreign keys to.

    Returns:
        The data with the correct foreign keys replacing the wikidata_ids.
    """

    foreign_keys = fetch_referenced_ids(cursor, table, [row[idx] for row in data])
    data = [tuple((list(row)[:idx] + [foreign_keys.get(row[idx], None)] + list(row)[idx+1:])) for row in data]
    return data


def replace_junction_table_fk(cursor, tables: tuple, data: list[tuple]) -> list[tuple]:
    """
    Prepares junction table data with correct foreign keys.

    Args:
        cursor: The MySQL cursor object.
        tables (list): A list containing the names of the two tables referenced by the junction table.
        data (list): A list of tuples representing the original data with `wikidata_id` values.

    Returns:
        list: A list of tuples representing the prepared junction table data with correct foreign keys.
    """

    foreign_keys1 = fetch_referenced_ids(cursor, tables[0], [row[0] for row in data])
    foreign_keys2 = fetch_referenced_ids(cursor, tables[1], [row[1] for row in data])
    try:
        junction_data = [(foreign_keys1[row[0]], foreign_keys2[row[1]]) for row in data if row[0] in foreign_keys1 and row[1] in foreign_keys2]
        junction_data = [row for row in junction_data if None not in row]
        junction_data = list(set(tuple(item) for item in junction_data))
        return junction_data
    except KeyError as e:
        print('Error:', e)

    


def batch_insert(cursor, table: str, columns: tuple, data: list[tuple]):
    """
    Batch inserts data into the specified table.

    Args:
        cursor: The MySQL cursor object.
        table (str): The name of the table to insert data into.
        columns (list): A list of column names in the same order as the data.
        data (list of tuples): A list of tuples representing the data to be inserted.
    """

    column_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    try:
        sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        cursor.fetchall()
        cursor.executemany(sql, data)
        print(f"Successfully inserted {cursor.rowcount} rows into {table}")
    except mysql.connector.Error as err:
        print(f"Insertion error: {err}")


def main():
    """
    Connects to the MySQL database, prepares and inserts data, and creates junction tables.

    This function orchestrates the process of connecting to the MySQL database,
    preparing data from JSON files, replacing wikidata_ids with correct foreign keys,
    inserting the prepared data into respective tables, and creating and inserting data
    into junction tables.
    """

    conn = connect_to_database()
    cursor = conn.cursor()
    try:
        # Test a simple query to check if the cursor is connected
        cursor.execute("SELECT 1")
        cursor.fetchall()
        print("Cursor is connected to the DB.")
    except mysql.connector.Error as err:
        print(f"Cursor is not connected to a database: {err}")
        return
    
    # Read the data from the JSON files
    raw_genre_data = read_from_json('data/processed/genre_details.json')
    items = read_from_json("data/processed/detailed_items.json")
    items = format_items(items)
    labels = read_from_json("data/raw/metal_items.json")
    performers = read_from_json("data/processed/performer_details.json")
    data_codes = read_from_json("config/data_codes.json")

    # Prepare and inser data into the database
    genre_data = process_genre(raw_genre_data, data_codes['genres'])   
    batch_insert(cursor, 'genre', ('genre_name', 'wikidata_id'), genre_data)
    musician_data = process_musician(items, labels, data_codes['musician_codes'])
    batch_insert(cursor, 'musician', ('wikidata_id', 'name', 'instrument', 'additional_instrument', 'additional_instrument2', 'additional_instrument3', 'additional_instrument4'), musician_data)
    band_data = process_band(items, labels, data_codes['band_codes'])
    batch_insert(cursor, 'band', ('name', 'country', 'wikidata_id', 'start_date', 'end_date'), band_data)
    album_data = process_album(items, labels, data_codes['album_codes'])
    album_data = replace_foreign_keys(cursor, 'band', 1, album_data)
    batch_insert(cursor, 'album', ('name', 'band_id', 'release_date', 'duration', 'type', 'wikidata_id'), album_data)
    song_data = process_song(items, labels, data_codes['song_codes'])
    song_data = replace_foreign_keys(cursor, 'band', 1, song_data)
    song_data = replace_foreign_keys(cursor, 'album', 2, song_data)
    batch_insert(cursor, 'song', ('name', 'band_id', 'album_id', 'duration', 'wikidata_id'), song_data)

    # Prepare the junction tables
    band_wikidata_ids = [band[2] for band in band_data]
    album_wikidata_ids = [album[5] for album in album_data]

    band_membership = process_junction_data(items, band_wikidata_ids, list(performers.keys()), label='member')
    band_genre = process_junction_data(items, band_wikidata_ids, list(raw_genre_data.keys()), label='genre')
    album_genre = process_junction_data(items, album_wikidata_ids, list(raw_genre_data.keys()), label='genre')
    
    band_membership = replace_junction_table_fk(cursor, ('band', 'musician'), band_membership)
    band_genre = replace_junction_table_fk(cursor, ('band', 'genre'), band_genre)
    album_genre = replace_junction_table_fk(cursor, ('album', 'genre'), album_genre)
    
    # Insert the junction tables
    batch_insert(cursor, 'band_membership', ('band_id', 'musician_id'), band_membership)
    batch_insert(cursor, 'band_genre', ('band_id', 'genre_id'), band_genre)
    batch_insert(cursor, 'album_genre', ('album_id', 'genre_id'), album_genre)

    conn.commit()

if __name__ == '__main__':
    main()