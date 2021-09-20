DELIMITER $$
DROP PROCEDURE IF EXISTS moviesDB.getAllGenres;
CREATE PROCEDURE moviesDB.getAllGenres()
BEGIN
    SELECT DISTINCT genre FROM moviesDB.moviesTable ORDER BY genre;
END $$
DELIMITER ;
