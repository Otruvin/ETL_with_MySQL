
# Top rated movies by genres - frontend

This part of the project is intended for users of this information system.

### Syntax for using the script

    python get-movies.py [commands]

### Usage

### Help command

`--h | --help` - Show help message.

Example of using command:

    python converter.py --help

It returns:

    usage: get-movies.py [-h] [-N N] [-reg REGEX] [-yf YEAR_FROM] [-yt YEAR_TO] [-g GENRES] [-tp] [-l]

    optional arguments:
      -h, --help            show this help message and exit
      -N N                  Count of films to selected
      -reg REGEX, --regex REGEX
                            Regular expression for search films by name
      -yf YEAR_FROM, --year_from YEAR_FROM
                            Oldest year release of selected films
      -yt YEAR_TO, --year_to YEAR_TO
                            Latest year release of selected films
      -g GENRES, --genres GENRES
                            Genres of films to search
      -tp, --time_print     Print time of work program
      -l, --err_log         log the errors


### Get all movies sorted by ratings

Example of using command:
    
    python get-movies.py

It returns:

    genre,title,year,rating
    Action,"Tokyo Tribe",2014,5.0
    Action,"Justice League: Doom",2012,5.0
    Action,"On the Other Side of the Tracks (De l'autre côté du périph)",2012,5.0
    Action,"Faster",2010,5.0
    Action,"Superman/Batman: Public Enemies",2009,5.0
    Action,"Wonder Woman",2009,5.0
    Action,"Love Exposure (Ai No Mukidashi)",2008,5.0
    Action,"Max Manus",2008,5.0
    Action,"Battle Royale 2: Requiem (Batoru rowaiaru II: Chinkonka)",2003,5.0
    Action,"Sisters (Syostry)",2001,5.0
    
    ...

### Add results to csv

Example of using the command:

    python get-movies.py > AllMoviesRates.csv

This command will create a `AllMoviesRates.csv` file.

### Get N most rated films by all genres

`-N <count of films>` - Number of selectable films.

Example of using the command:

    python get-movies.py -N 3

It returns:

    genre,title,year,rating
    Action,"Tokyo Tribe",2014,5.0
    Action,"Justice League: Doom",2012,5.0
    Action,"On the Other Side of the Tracks (De l'autre côté du périph)",2012,5.0
    Adventure,"Ice Age: The Great Egg-Scapade",2016,5.0
    Adventure,"Delirium",2014,5.0
    Adventure,"Dragons: Gift of the Night Fury",2011,5.0
    Animation,"Loving Vincent",2017,5.0
    Animation,"Ice Age: The Great Egg-Scapade",2016,5.0
    Animation,"Cosmic Scrat-tastrophe",2015,5.0
    Children,"Ice Age: The Great Egg-Scapade",2016,5.0
    Children,"Cosmic Scrat-tastrophe",2015,5.0
    Children,"Saving Santa",2013,5.0
    Comedy,"All Yours",2016,5.0
    Comedy,"Ice Age: The Great Egg-Scapade",2016,5.0
    Comedy,"Tom Segura: Mostly Stories",2016,5.0
    Crime,"Loving Vincent",2017,5.0
    Crime,"L.A. Slasher",2015,5.0
    Crime,"Tokyo Tribe",2014,5.0
    Documentary,"Won't You Be My Neighbor?",2018,5.0

    ...

### Show films with regular expression

`-reg | -regex <regular expression>` - Regular expression for search films by name.

Example of using the command:

    python get-movies.py -N 3 -reg "war"

It returns:

    genre,title,year,rating
    Action,"Why Don't You Play In Hell? (Jigoku de naze warui)",2013,4.2
    Action,"Swarm, The",1978,2.2
    Action,"Hardware",1990,1.5
    Adventure,"Homeward Bound II: Lost in San Francisco",1996,3.6
    Adventure,"Homeward Bound: The Incredible Journey",1993,3.0
    Adventure,"Howard the Duck",1986,2.2
    Animation,"Snow White and the Seven Dwarfs",1937,3.6
    Children,"Snow White and the Seven Dwarfs",1937,3.6
    Children,"Homeward Bound II: Lost in San Francisco",1996,3.6
    Children,"Homeward Bound: The Incredible Journey",1993,3.0
    Comedy,"Great Buck Howard, The",2008,3.5
    Comedy,"Benchwarmers, The",2006,2.8
    Comedy,"That Awkward Moment",2014,2.6
    Crime,"Assassination of Jesse James by the Coward Robert Ford, The",2007,4.1
    Documentary,"Zeitgeist: Moving Forward",2011,5.0
    Documentary,"Internet's Own Boy: The Story of Aaron Swartz, The",2014,4.5
    Documentary,"Haunted World of Edward D. Wood Jr., The",1996,4.0
    Drama,"Why Don't You Play In Hell? (Jigoku de naze warui)",2013,4.2
    Drama,"Assassination of Jesse James by the Coward Robert Ford, The",2007,4.1
    
    ...

### Sorting films by ears of release

`-yf | year_from <year>` - The Oldest year release of selected films. <p>
`-yt | year_to <year>` - the Latest year release of selected films.

Example of using the command:

    python get-movies.py -N 10 -reg war -yf 1990 -yt 1995

It returns:

    genre,title,year,rating
    Action,"Hardware",1990,1.5
    Adventure,"Homeward Bound: The Incredible Journey",1993,3.0
    Children,"Homeward Bound: The Incredible Journey",1993,3.0
    Drama,"Howards End",1992,4.0
    Drama,"Edward Scissorhands",1990,3.7
    Drama,"Homeward Bound: The Incredible Journey",1993,3.0
    Fantasy,"Edward Scissorhands",1990,3.7
    Horror,"Hardware",1990,1.5
    Romance,"Edward Scissorhands",1990,3.7
    Sci-Fi,"Hardware",1990,1.5

    _

### Choose genres

`-g <genre_1|genre_2|...|genre_n>` - Select films by genres.

Example of using the command:

    python get-movies.py -N 10 -reg war -yf 1990 -yt 1995 -g "Drama"

It returns:

    genre,title,year,rating
    Drama,"Howards End",1992,4.0
    Drama,"Edward Scissorhands",1990,3.7
    Drama,"Homeward Bound: The Incredible Journey",1993,3.0

    _

Another example with multiple genres.

Example of using the command:

    python get-movies.py -N 10 -reg war -yf 1990 -g "Drama|Children"

It returns:

    genre,title,year,rating
    Children,"Homeward Bound II: Lost in San Francisco",1996,3.6
    Children,"Homeward Bound: The Incredible Journey",1993,3.0
    Drama,"Why Don't You Play In Hell? (Jigoku de naze warui)",2013,4.2
    Drama,"Assassination of Jesse James by the Coward Robert Ford, The",2007,4.1
    Drama,"Howards End",1992,4.0
    Drama,"Edward Scissorhands",1990,3.7
    Drama,"Pay It Forward",2000,3.4
    Drama,"Homeward Bound: The Incredible Journey",1993,3.0
    Drama,"Black Book (Zwartboek)",2006,2.5
    Drama,"Spring Forward",1999,2.0

    _

This command returns all films that have any of chosen genres begin from all drama films and ends on films for childrens.

### Additional commands

### Log errors

`-l` - command to log errors.

Example of using the command:

    python get-movies.py -l

If an error occurs, a `logs.log` file will be created, in which the error will be recorded.