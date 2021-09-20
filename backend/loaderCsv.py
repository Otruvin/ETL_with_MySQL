import csv
import argparse
from typing import Sequence, Optional
from mysql.connector import connect, Error


def parseArgs(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-fm", "--file_movies",
                        required=True,
                        type=str,
                        help="path to movies csv file")
    parser.add_argument("-fr", "--file_rating",
                        required=True,
                        type=str,
                        help="path to ratings csv file")
    parser.add_argument("-hs", "--host",
                        required=True,
                        type=str,
                        help="host address with database")
    parser.add_argument("-u", "--user",
                        required=True,
                        type=str,
                        help="user login for database")
    parser.add_argument("-pas", "--password",
                        required=True,
                        type=str,
                        help="user password for database")
    parser.add_argument("-p", "--port",
                        required=True,
                        type=str,
                        help="specifying a port number with DNS SRV")
    args = parser.parse_args(argv)
    return args


def loadMoviesToDb(filePath: str,
                 host: str,
                 user: str,
                 port: str,
                 password: str) -> None:
    """
    Load data from movies csv to database

    Parameters
    __________
    filePath:  str   path to file with all movies
    host:      str   host address of database
    user:      str   user login
    port:      str   port number
    password:  str   user password

    Returns
    _______
    None
    """
    try:
        with connect(
                host=host,
                port=port,
                user=user,
                password=password
        ) as connection:
            with open(filePath, 'r') as file:
                next(file)
                reader = csv.reader(file)
                query = "INSERT IGNORE INTO stagingDB.moviesStg (movieId, title, genres) VALUES (%s, %s, %s);"
                with connection.cursor() as cursor:
                    for line in reader:
                        cursor.execute(query, line)
            connection.commit()
    except (OSError, Exception) as e:
        raise ValueError("Error while reading csv. Error: {0}".format(e))


def loadRatingsToDB(pathFile: str,
                    host: str,
                    user: str,
                    port: str,
                    password: str) -> None:
    """
    Load average ratings from ratings csv to database

    Parameters
    __________
    pathFile: str    path to file with all ratings
    host:      str   host address of database
    user:      str   user login
    port:      str   port number
    password:  str   user password

    Returns
    _______
    dict
    """
    try:
        ratings = {}
        with open(pathFile) as f:
            next(f)
            csv_reader = csv.reader(f)
            for _, idMovie, rating, _ in csv_reader:
                idMovie = int(idMovie)
                if idMovie not in ratings:
                    ratings[idMovie] = [0, 0]
                ratings[idMovie][0] += float(rating)
                ratings[idMovie][1] += 1
        for key, value in ratings.items():
            ratings[key] = value[0] / value[1]
    except (EnvironmentError, ZeroDivisionError) as e:
        raise ValueError("Env error. Error: {0}".format(e))

    try:
        query = "INSERT IGNORE INTO stagingDB.ratingsStg (movieId, rating) VALUES (%s, %s);"
        with connect(
                host=host,
                port=port,
                user=user,
                password=password
        ) as connection:
            with connection.cursor() as cursor:
                for k, v in ratings.items():
                    cursor.execute(query, [k, v])
                connection.commit()
    except (OSError, Exception) as e:
        raise ValueError("Error while reading csv. Error: {0}".format(e))

    


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parseArgs(argv)

    try:
        # load movies to database
        loadMoviesToDb(args.file_movies, args.host, args.user, args.port, args.password)
        # load ratings to database
        loadRatingsToDB(args.file_rating, args.host, args.user, args.port, args.password)
    except (Error, OSError) as e:
        raise Error("Error while inserting data to database. Error: {0}".format(e))

    return 0


if __name__ == '__main__':
    exit(main())
