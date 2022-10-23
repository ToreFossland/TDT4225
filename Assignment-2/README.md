# TDT4225

I deleted most of the database files, just fill the data with user data again.
First you need to download the requirements, preferably in a virtual environment like conda. After activating the environment run the command:
pip install -r requirements.txt

After that we have to make a docker container to place our database in:
docker volume create mysql8-data
docker run --name mysql8 -d -e MYSQL_ROOT_PASSWORD=root -p 13306:3306 -v mysql8-data:/var/lib/mysql mysql:8

First you have to run DatabaseManager.py, which cleans the data, sets up a new database and inserts the data.
Then you can start querying from the Queries.py file.

