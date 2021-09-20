DROP TABLE IF EXISTS moviesDB.moviesTable;
CREATE TABLE moviesDB.moviesTable
(
    id INT NOT NULL AUTO_INCREMENT,
    movieId INT NOT NULL,
    title VARCHAR(190) NOT NULL,
    year SMALLINT NOT NULL,
    genre VARCHAR(60) NOT NULL,
    rating FLOAT(6,5) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;