#!/bin/bash

source config/.env

mysql -u $DB_USER -p < src/sql/create_db.sql
python3 src/query_wikidata.py
python3 src/extract_details.py
python3 src/populate_db.py