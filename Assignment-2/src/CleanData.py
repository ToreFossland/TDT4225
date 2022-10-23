from datetime import datetime
import pandas as pd
import numpy as np
import glob
from pathlib import Path  

class CleanData:
    def __init__(self):
        self.cwd = "/Users/torefossland/VSCode/TDT4225/"
        self.labels = self.get_labels()
        self.users = []
        self.activities = []
        self.trackpoints = []
        self.date_format = "%Y-%m-%d %H:%M:%S"
        self.userColumns = ["id", 'has_labels']
        self.activityColumns = ['user_id','transportation_mode', 'start_date_time', 'end_date_time']
        self.trackPointColumns = ['lat','lon', 'null', 'alt' ,'date_days','date','time']
        self.labelColumns = ['start_date_time' , 'end_date_time', 'transportation_mode']

    def load_all_data(self, make_csv=False):
        self.make_df_user()
        self.make_df_activity_trackPoints()

        if make_csv:
            #Users
            filepath = Path(self.cwd + "files/users.csv")  
            filepath.parent.mkdir(parents=True, exist_ok=True)  
            self.users.to_csv(filepath, index=False)
            #Activities
            filepath = Path(self.cwd + "files/activities.csv")  
            filepath.parent.mkdir(parents=True, exist_ok=True)  
            self.activities.to_csv(filepath, index=False)
            #TrackPoints
            filepath = Path(self.cwd + "files/trackpoints.csv")  
            filepath.parent.mkdir(parents=True, exist_ok=True)  
            self.trackpoints.to_csv(filepath, index=False)

    def get_labels(self):
        temp = []
        for item in glob.glob(self.cwd + "/files/dataset/labeled_ids.txt"):
            with open(item, "r") as f:
                for line in f:
                    temp.append(line.strip('\n'))
        return temp

    def make_df_user(self):
        for folder in sorted(glob.glob(self.cwd + "files/dataset/Data/*")):
            user = folder[-3:]
            if user in self.labels:
                self.users.append([user, 1])
            else:
                self.users.append([user, 0])
        self.users = pd.DataFrame(self.users, columns=self.userColumns)     

    def make_df_activity_trackPoints(self):
        path = self.cwd + "files/dataset/Data/"
        for i, row in self.users.iterrows():
            id, has_labels = row['id'], row['has_labels']
            folder = path + id
            file_path = folder + "/labels.txt"
            labels = []
            if(has_labels):
                with open(file_path) as file:
                     file.readline()
                     for line in file:
                        start_date_time, end_date_time, transportation_mode = [i.strip().replace("/", "-") for i in line.split("\t")]
                        labels.append([datetime.strptime(start_date_time, self.date_format), datetime.strptime(end_date_time, self.date_format), transportation_mode])
         
            for i, file in enumerate(glob.glob(folder +'/Trajectory/*.plt')):
                new_df = pd.read_csv(file, skiprows=6,names=self.trackPointColumns)
                if(len(new_df) > 2500):
                    continue                
                startDate, startTime = new_df.iloc[0, 5:]
                endDate, endTime = new_df.iloc[-1, 5:]
                start_date = datetime.strptime(startDate + " " + startTime, self.date_format)
                end_date = datetime.strptime(endDate + " " + endTime, self.date_format)
                inserted = False
                for start_date_time, end_date_time, transportation_mode in labels:
                    if start_date_time == start_date and end_date_time == end_date:
                        activity_df = pd.DataFrame([[row['id'], transportation_mode,  start_date, end_date]], columns=self.activityColumns)
                        self.activities.append(activity_df)
                        inserted = True
                        break
                if not inserted:
                    activity_df = pd.DataFrame([[row['id'], None,  start_date, end_date]], columns=self.activityColumns)
                    self.activities.append(activity_df)
                new_df.insert(0, 'activity_id', pd.DataFrame({'activity_id': [len(self.activities)] * len(new_df)}))
                new_df['date_time'] = pd.to_datetime(new_df['date'] + " " + new_df['time'], format=self.date_format)
                new_df = new_df.drop(['date', 'time', 'null'], axis=1)  
                self.trackpoints.append(new_df)
        self.activities = pd.concat(self.activities)
        self.trackpoints = pd.concat(self.trackpoints)

def main():
    obj = CleanData()
    obj.load_all_data()
    
if __name__ == "__main__":
    main()





