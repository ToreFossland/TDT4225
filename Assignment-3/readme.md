conda create --name assignment-3
conda activate assignment-3

cd into directory "Assignment 3 and perform the following commands:
docker-compose up -d
docker exec -it assignment-3-mongodb-1 bash
mongosh --username root --password root
use admin
db.createUser({ user: 'admin', pwd: 'admin', roles: ['root'] })
show users
use my_db
db.createUser({user: 'user', pwd: 'user', roles: ['readWrite']})




