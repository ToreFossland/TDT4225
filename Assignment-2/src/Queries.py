from DatabaseManager import DatabaseManager
from haversine import haversine
from tabulate import tabulate
import pandas as pd

class Queries:
   
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def question1(self):
        users = self.db_manager.fetch_data("SELECT COUNT(*) AS count FROM data.users").first()
        activities = self.db_manager.fetch_data("SELECT COUNT(*) AS count FROM data.activities").first()
        trackpoints = self.db_manager.fetch_data("SELECT COUNT(*) AS count FROM data.trackpoints").first()
        print(f"User count: {users['count']}, activity count: {activities['count']}, trackpoint count: {trackpoints['count']}.")
    
    def question2(self):
        users = self.db_manager.fetch_data("SELECT COUNT(*) AS count FROM data.users").first()
        activities = self.db_manager.fetch_data("SELECT COUNT(*) AS count FROM data.activities").first()
        print(f"Average number of activities per user: {activities['count'] // users['count']}. ")

    def question3(self):
        query = """
                    SELECT user_id, COUNT(*) AS activity_count
                    FROM data.activities 
                    GROUP BY user_id
                    ORDER BY activity_count DESC
                    LIMIT 20
                """

        users = self.db_manager.fetch_data(query)
        for row in users:
            print('User ID: ', row['user_id'], " Activity Count: ",  row['activity_count'])

    def question4(self):
        query = """
                    SELECT DISTINCT user_id
                    FROM data.activities 
                    WHERE transportation_mode='taxi'
                """

        users = self.db_manager.fetch_data(query)
        for row in users:
            print('User ID: ', row['user_id'])
    
    def question5(self):
        query = """
                    SELECT transportation_mode, COUNT(*) AS transportation_count
                    FROM data.activities 
                    WHERE NOT transportation_mode='None'
                    GROUP BY transportation_mode
                    ORDER BY transportation_count DESC
                """

        modes = self.db_manager.fetch_data(query)
        for row in modes:
            print(row['transportation_mode'],": ", row['transportation_count'])
    
    def question6(self):
        query = """
                    SELECT EXTRACT(YEAR FROM start_date_time) AS year, COUNT(*) AS year_count
                    FROM data.activities 
                    GROUP BY year
                    ORDER BY year_count DESC
                    LIMIT 1
                """

        year = self.db_manager.fetch_data(query)
        for row in year:
            print(row['year'],": ", row['year_count'])
        
        query = """
                    SELECT EXTRACT(YEAR FROM start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS hour_count
                    FROM data.activities 
                    GROUP BY year
                    ORDER BY hour_count DESC
                    LIMIT 1
                """

        year = self.db_manager.fetch_data(query)
        for row in year:
            print(row['year', 'hour_count'],": ", row['hour_count'])

    def question7(self):
        query = """
                    SELECT activity_id, lat, lon
                    FROM data.trackpoints
                    INNER JOIN data.activities ON data.trackpoints.activity_id = data.activities.id
                    WHERE EXTRACT(YEAR FROM date_time) = 2008 AND transportation_mode = 'walk' AND user_id = 112
                """

        activities = self.db_manager.fetch_data(query)
        distance = 0
        previousRow = None
        for row in activities:
            if previousRow == None:
                previousRow = row
                continue
            if previousRow['activity_id'] == row['activity_id']:
                distance += haversine([row['lat'], row['lon']],  [previousRow['lat'], previousRow['lon']], unit="km")
        print(distance, 'km') 

    def question8(self):
        query = """
                SELECT user_id, (SUM(temp) * 0.3048) AS feetInMeters
                FROM (SELECT TP1.activity_id, SUM(TP2.altitude-TP1.altitude) AS temp 
                FROM data.trackpoints AS TP1 INNER JOIN data.trackpoints AS TP2 ON TP1.id=TP2.id-1 
                WHERE  TP1.altitude < TP2.altitude AND TP1.altitude != -777 AND TP2.altitude != -777
                GROUP BY TP1.activity_id) AS tempTable, data.activities
                WHERE data.activities.id = activity_id
                GROUP BY user_id
                ORDER BY feetInMeters DESC 
                LIMIT 20
                """
        users = self.db_manager.fetch_data(query)
        for row in users:
            print(row['user_id'], ": ", row['feetInMeters'])
    
    def question9(self):
        query = """
                SELECT activities.user_id AS users, COUNT(DISTINCT(activities.id)) as invalidActivities
                FROM data.activities AS activities INNER JOIN (
                    SELECT TP1.activity_id
                    FROM data.trackpoints as TP1 INNER JOIN data.trackpoints as TP2 ON TP1.activity_id = TP2.activity_id AND TP1.id = TP2.id-1
                    WHERE TIMESTAMPDIFF(MINUTE, TP1.date_time, TP2.date_time) >= 5
                ) AS InvalidTrackPoints ON activities.id = InvalidTrackPoints.activity_id
                GROUP BY activities.user_id
                ORDER BY COUNT(DISTINCT(activities.id)) DESC
                """
        users = self.db_manager.fetch_data(query)
        for row in users:
            print("User id: ", row['users'], " Invalid Activities: ", row['invalidActivities'])
    
    def question10(self):
        query = """
                SELECT DISTINCT data.activities.user_id AS users
                FROM data.trackpoints 
                INNER JOIN data.activities ON data.trackpoints.activity_id = data.activities.id 
                WHERE data.trackpoints.lon BETWEEN 116.396 AND 116.398 AND data.trackpoints.lat BETWEEN 39.915 AND 39.917 
                """

        users = self.db_manager.fetch_data(query)
        for row in users:
            print("User id: ", row['users'])
        
    def question11(self):
        query = """
                SELECT data.users.id, data.activities.transportation_mode
                FROM data.users INNER JOIN data.activities ON data.users.id = data.activities.user_id
                WHERE NOT transportation_mode = 'NULL' 
                ORDER BY data.users.id
                """

        users = self.db_manager.fetch_data(query)
        transportation_counter = {}
        for row in users:
            if row['id'] not in transportation_counter:
                transportation_counter[row['id']] = []
            transportation_counter[row['id']].append(row['transportation_mode'])
        for user in transportation_counter:
            print(user, ": ", max(set(transportation_counter[user]), key=transportation_counter[user].count))
    


def main():
    program = None
    try:
        program = Queries()
        #Change number to run different functions
        program.question1()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()
        
