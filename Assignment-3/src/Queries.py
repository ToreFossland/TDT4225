from DatabaseManager import DatabaseManager
from haversine import haversine
from tabulate import tabulate
import pandas as pd
from pprint import pprint 
from collections import Counter 


class Queries:
   
    def __init__(self):
        self.db_manager = DatabaseManager()
   
    def question1(self):
        counts = {}
        for col in self.db_manager.collections:
            collection = self.db_manager.db[col]
            count = collection.count()
            counts[col] = count
        print(f"User count: {counts['User']}, activity count: {counts['Activity']}, trackpoint count: {counts['TrackPoint']}.")
    
    def question2(self):
        counts = {}
        for col in self.db_manager.collections:
            collection = self.db_manager.db[col]
            count = collection.count()
            counts[col] = count
        print(f"Average number of activities per user: {counts['Activity'] // counts['User']}. ")

    def question3(self):
        collection = self.db_manager.db['Activity']
        documents = collection.aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 20
            }
        ])
        for doc in documents: 
            pprint(doc)

    def question4(self):
        collection = self.db_manager.db['Activity']
        documents = collection.find({'transportation_mode' : 'taxi'})
        documents = documents.distinct("user_id")
        print(f"Users who have taken a taxi: {list(documents)}. ")

    def question5(self):
        collection = self.db_manager.db['Activity']
        documents = collection.aggregate( [
        {
            "$match": {
            "transportation_mode": {"$ne": None}
            }
        },
        {
            "$group" : { "_id" : '$transportation_mode', "count" : {"$sum" : 1 }}
        }
        ] )
        for d in documents:
            print("Transportation mode: ", d["_id"], "Count: ", d["count"])

    def question6(self):
        collection = self.db_manager.db['Activity']
        documents = collection.aggregate([
            {
                '$group': {
                    '_id': { '$year': {"$dateFromString": {"dateString": '$start_date_time'}}},
                    "count": {"$sum": 1}
                }
            },
            {
                '$sort': { 'count': -1 }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    'year': '$_id',
                    'count': '$count'
                }
            }
        ])

        for d in documents:
            print("Year with most activities: ", d["year"], "Activity count: ", d["count"])

        documents = collection.aggregate( [
            {
                '$group': {
                    '_id': { '$year': {"$dateFromString": {"dateString": '$start_date_time'}}},
                    'count': { '$sum': { '$dateDiff': {'startDate': {"$dateFromString": {"dateString": '$start_date_time'}}, 'endDate': {"$dateFromString": {"dateString": '$end_date_time'}}, 'unit': "hour"}}}
                }
            },
            {
                '$sort': {
                    'count': -1
                }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    'year': '$_id',
                    'count': '$count'
                }
            }
        ])

        for d in documents:
            print("Year with most recorded hours: ", d["year"], "Hour count: ", d["count"])
        
        print("The year with the most activities is not also the year with the most recorded hours.")


    def question7(self):
        activity_collection = self.db_manager.db['Activity']
        activities = activity_collection.aggregate([
            {
                "$match": {
                    "user_id": 112,
                    "start_date_time": {"$gte": '2008-01-01 00:00:00'},
                    "end_date_time": {"$lt": '2009-01-01 00:00:00'},
                    "transportation_mode": "walk"
                }
            },
            {
                "$project": {
                    "activity_id": 1
                }
            }
        ])


        trackpoint_collection = self.db_manager.db['TrackPoint']
        distance = 0
        for a in activities:
            trackpoints = trackpoint_collection.aggregate([
                {
                    "$match": {
                        "activity_id" : a["_id"]
                    }
                },
                {
                    "$project": {
                        "activity_id": 1,
                        "lat" : 1,
                        "lon" : 1
                    }
                }
            ])
            previousRow = None
            for row in trackpoints:
                if previousRow == None:
                    previousRow = row
                    continue
                if previousRow['activity_id'] == row['activity_id']:
                    distance += haversine([row['lat'], row['lon']],  [previousRow['lat'], previousRow['lon']], unit="km")
        print(distance, 'km') 

        
    def question11(self):
        collection = self.db_manager.db['Activity']
        activities = collection.find({})
        userActivities = {}
        for activity in activities:
            transportation_mode = activity["transportation_mode"]
            if not activity["transportation_mode"]:
                continue
            user_id = activity["user_id"]
            if user_id not in userActivities:
                userActivities[user_id] = []
            userActivities[user_id].append(transportation_mode)
        for user in userActivities:
            occurence_count = Counter(userActivities[user])
            userActivities[user] = occurence_count.most_common(1)[0][0]
        pprint(userActivities)


def main():
    program = None
    try:
        program = Queries()
        #Change number to run different functions
        program.question7()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()
        
