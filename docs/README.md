# Metal Explorer

Explore metal subgenres and bands!


## Run from the terminal
- Set your database credentials at `config/.env`
- Run `run.sh`
    
    This will:
        
    - Create a MySQL database (you will need to enter your MySQL password when prompted)
    - Connect to SPARQL endpoint, query, and download data from Wikidata 
    - Extract and process relevant data
    - Populate the database.


## Requirements
- Python > 3.10
- MySQL
- Python libraries:
```pip install -r requirements.txt```


## Troubleshooting
You may need to add the project path to your python environment to correctly import the modules:

```export PYTHONPATH=/path/to/project/MetalExplorer/src```


## Acknowledgements

Data Source: Wikidata


## Relevant Resources

[Wikidata](https://www.wikidata.org/wiki/Wikidata:Data_access/en)

[About SPARQL](https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service)

[SPARQL tutorial](https://www.wikidata.org/wiki/Wikidata:SPARQL_tutorial)