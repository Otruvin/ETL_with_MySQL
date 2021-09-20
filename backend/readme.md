# Top rated movies by genres - backend

This part of the project is intended for administrators of this information system.

### Backend part folders structure

    .
    ├── files
    │   ├── movies.csv
    │   ├── ratings.csv
    │   ├── results
    │   │   └── allRates.csv
    │   └── tmp
    ├── loaderCsv-mp.py
    ├── loaderCsv.py
    ├── readme.md
    └── requests
        ├── createDatabases
        │   ├── createMoviesDB.sql
        │   └── createStgDB.sql
        ├── createStoredProcedures
        │   ├── createExportProcedure.sql
        │   ├── createGetAllGenresProcedure.sql
        │   └── createSelectProcedure.sql
        └── createTables
            ├── createMoviesTable.sql
            ├── createMoviesTableStg.sql
            └── createRatingsTableStg.sql


Folder `files` contains downloaded data from `grouplens.org`.
Subfield `result` contains the results of the information system.
`tmp` contains temporary files.

`loaderCsv.py` - script for load data from `.csv` files to database.

`loaderCsv-mp.py` - mostly same as `loaderCsv.py`, but using multiprocessing. `loaderCsv-mp.py` have additional 
argument `-c | --cpu` - count of cpu to use.

Folder `requests` contains database queries distributed across `.sql` files.

Scripts from `createDatabases` are used to create staging tables in databases. `createStoredProcedures` - to 
create stored procedures in all created databases. `createTables` - to create all tables.

Scripts to create tables:

 - `createMoviesTableStg.sql` - create staging (landing) table for movies.
 - `createRatingsTableStg.sql` - create staging (landing) table for ratings.
 - `createMoviesTable.sql` - create table for storing results with rated movies.

Scripts to create stores procedures:

 - `createExportProcedure.sql` - to preprocess all data and insert results to `moviesTable` from `moviesDB`.
 - `createSelectProcedure.sql` - to select movies from `moviesDB` according to the specified search parameters.
 - `createGetAllGenresProcedure.sql` - to get all possible genres.

### Usage loaderCsv.py

### Arguments

`-fm | --file_movies` - path to `.csv` with movies data.

`-fr | --file_rating` - path to `.csv` with ratings.

`-hs | --host` - host address with database.

`-u | --user` - login registered user.

`-pas | --password` - password registered user.

`-p | --port` - specifying a port number with DNS SRV.

Example of usage:

    python3 loaderCsv.py -fm files/movies.csv -fr files/ratings.csv -hs 127.0.0.1 -u root -pas password -p 3306
