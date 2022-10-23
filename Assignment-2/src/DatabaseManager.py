import contextlib
import time
from sqlalchemy import Column, String, Float, Boolean, DateTime, create_engine, ForeignKey, select, text
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database

from CleanData import CleanData

Base = declarative_base()

TABLES = {}
TABLES['User'] = (
             """
                INSERT INTO data.users
                    (id, has_labels)
                VALUES (%(id)s, %(has_labels)s)
            """
            )
TABLES['Activity'] = (
             """
                INSERT INTO data.activities
                    (id, user_id, transportation_mode, start_date_time, end_date_time)
                VALUES (%(id)s, %(user_id)s,  %(transportation_mode)s,  %(start_date_time)s,  %(end_date_time)s)
            """
            )
TABLES['TrackPoint'] = (
             """
                INSERT INTO data.trackpoints
                    (id, activity_id, lat, lon, altitude, date_days, date_time)
                VALUES (%(id)s, %(activity_id)s,  %(lat)s,  %(lon)s,  %(altitude)s, %(date_days)s, %(date_time)s)
            """
            )

class User(Base):
    __tablename__ = "users"
    id = Column(String(255), primary_key=True)
    has_labels = Column(Boolean)

class Activity(Base):
    __tablename__ = "activities"
    id = Column(INTEGER, primary_key=True)
    user_id = Column(String(255))
    transportation_mode = Column(String(255))
    start_date_time = Column(DateTime)
    end_date_time = Column(DateTime)

class TrackPoint(Base):
    __tablename__ = "trackpoints"
    id = Column(INTEGER, primary_key=True)
    activity_id = Column(INTEGER) #, ForeignKey('activities.id')
    lat = Column(Float)
    lon = Column(Float)
    altitude = Column(INTEGER)
    date_days = Column(Float)
    date_time = Column(DateTime)

class DatabaseManager:
    def __init__(self):
        self.db_url = "mysql+mysqldb://root:root@127.0.0.1:13306/data"
        self.engine = create_engine(self.db_url, pool_size=5, pool_recycle=3600)
        self.validate_database()
        self.tables = ['User', 'Activity', 'TrackPoint']
        self.data = None
        
    def validate_database(self):
     engine = create_engine(self.db_url)
     if not database_exists(engine.url): # Checks for the first time  
         create_database(engine.url)     # Create new DB    
         print("New Database Created"+database_exists(engine.url)) # Verifies if database is there or not.
     else:
         print("Database Already Exists")

    @contextlib.contextmanager
    def get_session(self, cleanup=False):
        session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)
        try:
            yield session
        except Exception:
            session.rollback()
        finally:
            session.close()

        if cleanup:
            Base.metadata.drop_all(self.engine)

    @contextlib.contextmanager
    def get_conn(self, cleanup=False):
        conn = self.engine.connect()
        Base.metadata.create_all(self.engine)

        yield conn
        conn.close()

        if cleanup:
            Base.metadata.drop_all(self.engine)

    def load_data(self):
        self.data = CleanData()
        self.data.load_all_data()

    def insert_data(self, table, cleanup=False):
        start_time = time.time()
        sql_query = TABLES[table]
        with self.get_conn(cleanup) as conn:
            match table:
                case 'User':
                    conn.exec_driver_sql(
                    sql_query,
                    [ 
                    {"id": self.data.users.iloc[i]["id"], "has_labels": self.data.users.iloc[i]["has_labels"]} for i in range(1, len(self.data.users))
                    ],
                )
                case 'Activity':
                    conn.exec_driver_sql(
                    sql_query,
                    [ 
                    {"id": i, "user_id": self.data.activities.iloc[i]["user_id"],  "transportation_mode": self.data.activities.iloc[i]["transportation_mode"], "start_date_time": self.data.activities.iloc[i]["start_date_time"], "end_date_time": self.data.activities.iloc[i]["end_date_time"]} for i in range(1, len(self.data.activities))
                    ],
                )
                case 'TrackPoint':
                    conn.exec_driver_sql(
                    sql_query,
                    [ 
                    {"id": i, "activity_id": self.data.trackpoints.iloc[i]["activity_id"],  "lat": self.data.trackpoints.iloc[i]["lat"], "lon": self.data.trackpoints.iloc[i]["lon"], "altitude": self.data.trackpoints.iloc[i]["alt"], "date_days": self.data.trackpoints.iloc[i]["date_days"], "date_time": self.data.trackpoints.iloc[i]["date_time"]} for i in range(1, len(self.data.trackpoints))
                    ],
                )
        duration = time.time() - start_time
        print(f"Insert with plain query to {table}: {duration:.2f} seconds.")

    def fetch_data(self, sql_query, cleanup=False):
        start_time = time.time()
        params = {}
        with self.get_conn(cleanup) as conn:
            result = conn.exec_driver_sql(sql_query, params)
        duration = time.time() - start_time
        print(f"Fetch with plain query: {duration:.2f} seconds.")
        return result

    def drop_all(self, cleanup=True):
        start_time = time.time()
        with self.get_conn(cleanup) as conn:
            table1 = User.__table__.delete()
            table2 = Activity.__table__.delete()
            table3 = TrackPoint.__table__.delete()

            conn.execute(
                table1
            )
            conn.execute(
                table2
            )            
            conn.execute(
                table3
            )
        duration = time.time() - start_time
        print(f"Drop all tables: {duration:.2f} seconds.")

    def populate_database(self):
        try:
            self.drop_all()
            self.load_data()
            for table in self.tables:
                self.insert_data(table)

        except Exception as e:
            print("ERROR: Failed to use database:", e)


def main():
    program = None
    try:
        program = DatabaseManager()
        program.populate_database()
    except Exception as e:  
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()