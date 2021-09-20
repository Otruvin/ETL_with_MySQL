DROP TABLE IF EXISTS stagingDB.ratingsStg;
CREATE TABLE stagingDB.ratingsStg
(
    movieId INT NOT NULL,
    rating FLOAT4(6,5) NOT NULL,
    PRIMARY KEY (movieId)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;