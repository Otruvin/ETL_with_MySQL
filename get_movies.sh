#!/bin/bash

CONFIG_FILE=.env


loadConfigFile () {
	if [ -f "$CONFIG_FILE" ];
	then 
		export $(grep -v '^#' $CONFIG_FILE | xargs)
	else
		echo "Config file doesn't exists." >&2
		exit 1
	fi
}


executeSqlFileDockerDB () {
	docker exec -i $DOCKER_ID mysql -h$HOST -u$DB_USER -p$PASSWORD mysql < $1 2>&1 | grep -v "Using a password"
}


executeQueryDockerDB () {
	docker exec -i $DOCKER_ID mysql -h$HOST -u$DB_USER -p$PASSWORD <<< "$1" 2>&1 | grep -v "Using a password"
}


executeSqlFile () {
	mysql -h$HOST -u$DB_USER -p$PASSWORD mysql < $1 2>&1 | grep -v "Using a password"
}


executeQuery () {
	mysql -h$HOST -u$DB_USER -p$PASSWORD <<< "$1" 2>&1 | grep -v "Using a password"
}


executeAllSqlFilesInFolder () {
	for i in $1/*.sql; do
		[ -e "$i" ] || continue
		$2 "$i"
	done
}


createDataBase () {
	#create datbases
	executeAllSqlFilesInFolder backend/requests/createDatabases $1
	#create tables
	executeAllSqlFilesInFolder backend/requests/createTables $1
	#create stored procedures
	executeAllSqlFilesInFolder backend/requests/createStoredProcedures $1
}


downloadData () {
	curl -s $URL_MOVIES -o "${TMP_PATH}data.zip" > /dev/null
	unzip -o "${TMP_PATH}data.zip" "ml-latest-small/movies.csv" -d "${TMP_PATH}/data_temp"> /dev/null
	unzip -o "${TMP_PATH}data.zip" "ml-latest-small/ratings.csv" -d "${TMP_PATH}/data_temp"> /dev/null
	mv "${TMP_PATH}data_temp/ml-latest-small/ratings.csv" "${TMP_PATH}data_temp/ml-latest-small/movies.csv" backend/files/
	rm -rf "${TMP_PATH}data_temp"
	rm "${TMP_PATH}data.zip"
}


readLoadCsv () {
	python3 backend/loaderCsv-mp.py -fm $MOVIES_PATH -fr $RATINGS_PATH -hs $HOST -u $DB_USER -pas $PASSWORD -p $PORT
}


#define default values for params
docker=false
rewriteDB=false
N=""
regexp=""
yearFrom=""
yearTo=""
genres=""
logs=false

#parse params from console
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -reg|--regex) 		regexp="$2"; shift ;;
		-N)					N="$2"; shift ;;
		-yf|--year_from)	yearFrom="$2"; shift;;
		-yt|--year_to)		yearTo="$2"; shift ;;
		-g|--genres)		genres="$2"; shift ;;
		-l|--err_log)		logs=true ;;
		-rw|--rewrite) 		rewriteDB=true ;;
        -d|--docker) 		docker=true ;;
        *) echo "Error with parameters: $1" >&2; exit 1 ;;
    esac
    shift
done

#constract params string for get-movies.py script
getMovieArgs=""
if [ "$N" != "" ]; 
then
	getMovieArgs+="-N $N "
fi
if [ "$regexp" != "" ]; 
then
	getMovieArgs+="-reg $regexp "
fi
if [ "$yearFrom" != "" ]; 
then
	getMovieArgs+="-yf $yearFrom "
fi
if [ "$yearTo" != "" ]; 
then
	getMovieArgs+="-yt $yearTo "
fi
if [ "$genres" != "" ]; 
then
	getMovieArgs+="-g $genres "
fi
if $logs ; 
then
	getMovieArgs+="-l "
fi


#get env values
loadConfigFile

#download data and upload to database
if $rewriteDB ;
then
	if $docker ;
	then
		downloadData
		createDataBase executeSqlFileDockerDB
		readLoadCsv
		executeQueryDockerDB "CALL stagingDB.exportToMoviesDB();"
	else
		downloadData
		createDataBase executeSqlFile
		readLoadCsv
		executeQuery "CALL stagingDB.exportToMoviesDB();"
	fi
fi

#execute get-movies.py script
python3 frontend/get-movies.py $getMovieArgs

exit 0