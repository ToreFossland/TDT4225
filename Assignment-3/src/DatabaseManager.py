from pprint import pprint 
from DbConnector import DbConnector
from CleanData import CleanData
import pandas as pd
import json

class DatabaseManager:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.collections = ['User', 'Activity', 'TrackPoint'] 
        self.data = None
        self.collection_path = {
                    'User': "/Users/torefossland/VSCode/TDT4225/Assignment-3/files/users.csv",
                    'Activity' : "/Users/torefossland/VSCode/TDT4225/Assignment-3/files/activities.csv",
                    'TrackPoint' : "/Users/torefossland/VSCode/TDT4225/Assignment-3/files/trackpoints.csv"
                }

    def create_colls(self):
        for col in self.collections:
            collection = self.db.create_collection(col)    
            print('Created collection: ', collection)        

    def populate_database(self):
        try:
            user_coll = self.db['User']
            activity_coll = self.db['Activity']
            trackpoint_coll = self.db['TrackPoint']
            
            user_data = pd.read_csv(self.collection_path['User'])
            activity_data = pd.read_csv(self.collection_path['Activity'])
            trackpoint_data = pd.read_csv(self.collection_path['TrackPoint'])

            user_payload = json.loads(user_data.to_json(orient='records'))
            activity_payload = json.loads(activity_data.to_json(orient='records'))
            trackpoint_payload = json.loads(trackpoint_data.to_json(orient='records'))
            for user in user_payload:
                user_activities = list(filter(lambda x: (x['user_id'] == user['_id']), activity_payload)) 
                user_activities = list(map(lambda d: d['_id'], user_activities))
                user.update({"activities": user_activities})
            
            user_coll.insert_many(user_payload)
            activity_coll.insert_many(activity_payload)
            trackpoint_coll.insert_many(trackpoint_payload)
            
        except Exception as e:
            print("ERROR: Failed to use database:", e)
            
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents[:100]: 
            pprint(doc)


    def drop_colls(self):
        for col in self.collections:
            collection = self.db[col]
            collection.drop()

        
    def show_colls(self):
        collections = self.client['my_db'].list_collection_names()
        print("Collections: ", collections)
    

    def load_data(self):
        self.data = CleanData()
        #self.data.load_all_data()



         
def main():
    program = None
    try:
        program = DatabaseManager()
        #program.drop_colls()
        #program.create_colls()
        #program.show_colls()
        #program.populate_database()
        #program.fetch_documents(collection_name="User")
        #program.fetch_documents(collection_name="Activity")
        #program.fetch_documents(collection_name="TrackPoint")
        # Check that the table is dropped
        #program.show_colls()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
