import csv
import argparse
import math
from typing import Sequence, Optional
from mysql.connector import connect, Error
import multiprocessing as mp
from functools import partial


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
    parser.add_argument("-c", "--cpu",
                        type=int,
                        default=mp.cpu_count() - 1,
                        help="count of cpu to use")
    args = parser.parse_args(argv)
    return args


def getChunks(reader, chunkSize: int = 100) -> list:
    """
    Parse read data from csv to list with chunks

    Parameters
    __________
    reader:    list   csv reader (csv iterator)
    chunkSize: int    size of chunk for data

    Returns
    _______
    list
    """
    allChunks = []
    chunk = []
    for i, line in enumerate(reader):
        if (i % chunkSize == 0 and i > 0):
            allChunks.append(chunk)
            chunk = []
        chunk.append(line)
    allChunks.append(chunk)
    return allChunks


def getFileLen(fileName: str) -> int:
    """
    Get length of file

    Parameters
    __________
    fileName: str   path to file

    Returns
    _______
    int
    """

    with open(fileName) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def parseMoviesFileToChunks(pathFile: str, cpuCount: int = None) -> list:
    """
    Parse read movies data from csv file to list with chunks with set count of cpus

    Parameters
    __________
    pathFile:  str   path to csv file
    cpuCount:  int   count of cpu to use

    Returns
    _______
    list
    """
    if cpuCount is None:
        cpuCount = mp.cpu_count() - 1

    try:
        lenFile = getFileLen(pathFile)
        chunkSize = math.ceil(lenFile / cpuCount)
        with open(pathFile, 'r') as file:
            reader = csv.reader(file)
            return getChunks(reader, chunkSize)
    except Exception as e:
        raise ValueError("Error while parsing file on chunks. Error: {0}.".format(e))


def parseRatingsFileToChunks(pathFile: str, cpuCount: int = None) -> list:
    """
    Parse read ratings data from csv file to list with chunks with set count of cpus

    Parameters
    __________
    pathFile:  str   path to csv file
    cpuCount:  int   count of cpu to use

    Returns
    _______
    list
    """
    if cpuCount is None:
        cpuCount = mp.cpu_count() - 1

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
        ratings = ratings.items()
        chunkSize = math.ceil(len(ratings) / cpuCount)
        return getChunks(ratings, chunkSize)
    except (EnvironmentError, ZeroDivisionError) as e:
        raise ValueError("Env error. Error: {0}".format(e))
    except Exception as e:
        raise ValueError("Error while parsing file on chunks. Error: {0}.".format(e))


def worker(
        host: str,
        port: str,
        user: str,
        password: str,
        query: str,
        chunk: list) -> None:
    """
    Load data from chunk to database

    Parameters
    __________
    filePath:  str   list with all movies
    host:      str   host address of database
    user:      str   user login
    port:      str   port number
    password:  str   user password
    query:     str   the query to be executed
    chunk:     list  chunk with elements

    Returns
    _______
    None
    """
    with connect(
            host=host,
            port=port,
            user=user,
            password=password
    ) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, chunk)
            connection.commit()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parseArgs(argv)

    moviesChunks = parseMoviesFileToChunks(args.file_movies)
    ratingsChunks = parseRatingsFileToChunks(args.file_rating)

    importMovies = partial(
        worker,
        args.host,
        args.port,
        args.user,
        args.password,
        "INSERT IGNORE INTO stagingDB.moviesStg (movieId, title, genres) VALUES (%s, %s, %s);")
    importRatings = partial(
        worker,
        args.host,
        args.port,
        args.user,
        args.password,
        "INSERT IGNORE INTO stagingDB.ratingsStg (movieId, rating) VALUES (%s, %s);"
    )

    try:
        with mp.Pool(args.cpu) as pool:
            try:
                pool.map(importMovies, moviesChunks)
            except Error as e:
                raise ValueError("Error while inserting into movies. Error: {0}".format(e))
            try:
                pool.map(importRatings, ratingsChunks)
            except Error as e:
                raise ValueError("Error while inserting into ratings. Error: {0}".format(e))
    except Exception as e:
        raise ValueError("Error while importing data. Error: {0}".format(e))

    return 0


if __name__ == '__main__':
    exit(main())
