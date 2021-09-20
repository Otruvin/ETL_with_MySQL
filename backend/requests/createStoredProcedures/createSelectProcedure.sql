DELIMITER $$
DROP PROCEDURE IF EXISTS moviesDB.getAllRatedFilms;
CREATE PROCEDURE moviesDB.getAllRatedFilms(IN N INT, IN regex VARCHAR(50), IN yf INT, IN yt INT, IN genre VARCHAR(50))
BEGIN
    WITH
    selectByConditions AS (
        SELECT
            m.movieId,
            m.title,
            m.genre,
            m.year,
            ROUND(m.rating, 1) AS rating,
            ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rowNumber
        FROM moviesDB.moviesTable AS m
        WHERE
                ((yf IS NULL) OR (m.year >= yf))
            AND ((yt IS NULL) OR (m.year <= yt))
            AND ((regex IS NULL) OR (REGEXP_LIKE(m.title, regex, 'c')))
            AND ((genre IS NULL) OR (m.genre = genre))
    )
    SELECT
        movieId,
        title,
        genre,
        year,
        rating
    FROM selectByConditions
    WHERE ((N IS NULL) OR (rowNumber <= N));
END $$
DELIMITER ;