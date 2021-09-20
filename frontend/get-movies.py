import argparse
import os
from datetime import date
from typing import Optional, Sequence
import logging
from dotenv import load_dotenv
from mysql.connector import connect, Error


def parseArgs(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-N',
                        type=int,
                        help="Count of films to selected")
    parser.add_argument('-reg', '--regex',
                        type=str,
                        help="Regular expression for search films by name")
    parser.add_argument('-yf', '--year_from',
                        type=int,
                        help="Oldest year release of selected films")
    parser.add_argument('-yt', '--year_to',
                        type=int,
                        help='Latest year release of selected films')
    parser.add_argument('-g', '--genres',
                        type=str,
                        help='Genres of films to search')
    parser.add_argument('-l', '--err_log',
                        action='store_true',
                        help='log the errors')

    args = parser.parse_args(argv)
    return args


def printResults(movies: list) -> None:
    """
    Print all results

    Parameters
    __________
    movies:  list   list with all movies

    Returns
    _______
    None
    """
    print("genre,title,year,rating")
    for row in movies:
        print('{0},"{1}",{2},{3}'.format(row[2], row[1], row[3], row[4]))


def preprocessArgsForSearchMovies(connectData: dict, conditions: list) -> list:
    """
    Prepare arguments to search

    Parameters
    __________
    connectData:  dict   connection data for user
    conditions    list   list with conditions to search

    Returns
    _______
    list
    """
    N, regexpr, yearFrom, yearTo, genres = conditions
    if genres is None:
        try:
            with connect(
                    host=connectData["host"],
                    port=connectData["port"],
                    user=connectData["user"],
                    password=connectData["password"]
            ) as connection:
                with connection.cursor() as cursor:
                    genres = []
                    cursor.callproc("moviesDB.getAllGenres")
                    for result in cursor.stored_results():
                        tempGenres = result.fetchall()
                    for genre in tempGenres:
                        genres.append(genre[0])
                    connection.commit()
        except (Error, Exception) as e:
            raise ValueError("Error while getting all genres. Error: {0}".format(e))
    else:
        genres.sort()

    if N is None:
        N = 'NULL'
    if regexpr is None:
        regexpr = 'NULL'
    if yearFrom is None:
        yearFrom = 'NULL'
    if yearTo is None:
        yearTo = 'NULL'

    return [N, regexpr, yearFrom, yearTo, genres]


def getConnectData(pathFile: str) -> dict:
    """
    Get connection data from .env file

    Parameters
    __________
    pathFile:  str   path to .env file

    Returns
    _______
    dict
    """
    try:
        load_dotenv(pathFile)
        connectData = {
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("PASSWORD"),
            "host": os.environ.get("HOST"),
            "port": os.environ.get("PORT")
        }
        return connectData
    except OSError as e:
        raise ValueError("Error while getting connection data.")


def getMoviesByGenre(connectData: dict, conditions: list) -> list:
    """
    Get movies from database by genre

    Parameters
    __________
    connectData:  dict   connection data for user
    conditions    list   list with conditions to search with single genre

    Returns
    _______
    list
    """
    N, regexpr, yearFrom, yearTo, genre = conditions
    try:
        with connect(
            host=connectData["host"],
            port=connectData["port"],
            user=connectData["user"],
            password=connectData["password"]
        ) as connection:
            with connection.cursor() as cursor:
                if regexpr == 'NULL':
                    cursor.execute('CALL moviesDB.getAllRatedFilms({0}, {1}, {2}, {3}, "{4}");'.format(N, regexpr, yearFrom, yearTo, genre))
                    return cursor.fetchall()
                else:
                    cursor.execute('CALL moviesDB.getAllRatedFilms({0}, "{1}", {2}, {3}, "{4}");'.format(N, regexpr, yearFrom, yearTo, genre))
                    return cursor.fetchall()
    except Error as e:
        raise ValueError("Error with reading genre {0}. Error: {1}".format(genre, e))
    except Exception as e:
        raise ValueError("System error with reading genre {0}. Error: {1}".format(genre, e))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parseArgs(argv)
    if args.err_log:
        logging.basicConfig(filename='logs.log', level=logging.ERROR)

    connectData = getConnectData("../.env")

    if args.genres:
        genres = args.genres.split("|")
    else:
        genres = None

    conditions = [args.N, args.regex, args.year_from, args.year_to, genres]

    try:
        conditions = preprocessArgsForSearchMovies(connectData, conditions)
        movies = []
        N, regexpr, yearFrom, yearTo, genres = conditions
        for genre in genres:
            movies.extend(getMoviesByGenre(connectData, [N, regexpr, yearFrom, yearTo, genre]))
        printResults(movies)
    except Error as e:
        if args.err_log:
            logging.error('Date: {0}. Error while connection. Error: {1}'.format(date.today().strftime("%d/%m/%Y"), e))
        return 1
    except Exception as e:
        if args.err_log:
            logging.error('Date: {0}. System error while working with results. Error: {1}'.format(date.today().strftime("%d/%m/%Y"), e))
        return 1

    return 0


if __name__ == '__main__':
    exit(main())