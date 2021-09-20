DELIMITER $$
DROP PROCEDURE IF EXISTS stagingDB.exportToMoviesDB;
CREATE PROCEDURE stagingDB.exportToMoviesDB()
BEGIN
	INSERT INTO moviesDB.moviesTable(movieId, title, year, genre, rating)
	WITH RECURSIVE
	ratedMovies AS (
		SELECT
			m.movieId AS movieId,
			REGEXP_REPLACE(m.title, '\\s\\([0-9]{4}\\)[[:blank:]]*$', '') as title,
			REGEXP_REPLACE(REGEXP_SUBSTR(m.title, '\\s\\([0-9]{4}\\)[[:blank:]]*$'), '[\\)\\(\\s]', '') as year,
			m.genres AS genres,
			r.rating AS rating
		FROM stagingDB.moviesStg AS m
		INNER JOIN stagingDB.ratingsStg AS r ON m.movieId = r.movieId
	),
	dropedBadData AS (
		SELECT * FROM ratedMovies
		WHERE year IS NOT NULL AND genres <> '(no genres listed)'
	),
	splitedGenres AS (
		SELECT
			movieId,
			title,
			year,
			rating,
			SUBSTRING(genres, 1, LOCATE('|', genres) - 1) genre,
			SUBSTRING(CONCAT(genres, '|'), LOCATE('|', genres) + 1) rest
		FROM dropedBadData
		UNION ALL
		SELECT
			movieId,
			title,
			year,
			rating,
			SUBSTRING(rest, 1, LOCATE('|', rest) - 1),
			SUBSTRING(rest, LOCATE('|', rest) + 1)
		FROM splitedGenres
		WHERE LOCATE('|', rest) > 0
	)
	SELECT
		movieId,
		title,
		year,
        genre,
		rating
	FROM splitedGenres
	WHERE genre <> ""
	ORDER BY
	    genre,
	    rating DESC,
	    year DESC,
	    title;
END $$
DELIMITER ;