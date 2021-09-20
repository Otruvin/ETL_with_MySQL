DROP TABLE IF EXISTS stagingDB.moviesStg;
CREATE TABLE stagingDB.moviesStg
(
    movieId INT NOT NULL,
    title VARCHAR(200),
    genres VARCHAR(200),
    PRIMARY KEY (movieId)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;