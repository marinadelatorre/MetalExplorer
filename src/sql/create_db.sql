CREATE DATABASE IF NOT EXISTS metal_db;

USE metal_db;

CREATE TABLE musician (
    id INT AUTO_INCREMENT PRIMARY KEY,
    wikidata_id VARCHAR(12),
    name VARCHAR(50) NOT NULL,
    instrument VARCHAR(50),
    additional_instrument VARCHAR(50),
    additional_instrument2 VARCHAR(50),
    additional_instrument3 VARCHAR(50),
    additional_instrument4 VARCHAR(50)
);

CREATE TABLE band (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(55) NOT NULL,
    country VARCHAR(50),
    wikidata_id VARCHAR(12),
    start_date DATE,
    end_date DATE
);

CREATE TABLE band_membership (
    band_id INT,
    musician_id INT,
    PRIMARY KEY (band_id, musician_id),
    FOREIGN KEY (band_id) REFERENCES band(id),
    FOREIGN KEY (musician_id) REFERENCES musician(id)
);

CREATE TABLE genre (
    id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(50) NOT NULL,
    wikidata_id VARCHAR(12)
);

CREATE TABLE band_genre (
    band_id INT,
    genre_id INT,
    PRIMARY KEY (band_id, genre_id),
    FOREIGN KEY (band_id) REFERENCES band(id),
    FOREIGN KEY (genre_id) REFERENCES genre(id)
);

CREATE TABLE album (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    band_id INT,
    release_date DATE,
    duration INT,
    type VARCHAR(50),
    wikidata_id VARCHAR(12),
    FOREIGN KEY (band_id) REFERENCES band(id)
);

CREATE TABLE album_genre (
    album_id INT,
    genre_id INT, 
    PRIMARY KEY (album_id, genre_id),
    FOREIGN KEY (album_id) REFERENCES album(id),
    FOREIGN KEY (genre_id) REFERENCES genre(id)
);

CREATE TABLE song (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    band_id INT,
    album_id INT,
    duration INT,
    wikidata_id VARCHAR(12),
    FOREIGN KEY (band_id) REFERENCES band(id),
    FOREIGN KEY (album_id) REFERENCES album(id)
);