import os

from google_sheets import GoogleSheets

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

from sqlalchemy import Table, MetaData, create_engine, text
from sqlalchemy.dialects.postgresql import insert

google_sheets = GoogleSheets()
workouts = google_sheets.get_all_workouts()

instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
db_user = os.environ["DB_USER"]
db_pass = os.environ["DB_PASS"]
db_name = os.environ["DB_NAME"]

ip_type = IPTypes.PUBLIC

# initialize Cloud SQL Python Connector object
connector = Connector()

def getconn() -> pg8000.dbapi.Connection:
    conn: pg8000.dbapi.Connection = connector.connect(
        instance_connection_name,
        "pg8000",
        user=db_user,
        password=db_pass,
        db=db_name,
        ip_type=ip_type,
    )
    return conn

# The Cloud SQL Python Connector can be used with SQLAlchemy
# using the 'creator' argument to 'create_engine'
pool = create_engine(
    "postgresql+pg8000://",
    creator=getconn,
    # ...
)

with pool.connect() as db_conn:
    # delete current data
    table = Table('gravityformworkouts',MetaData(), autoload_with=pool)
    #db_conn.execute(sqlalchemy.text("delete from only gravityformworkouts"))
    db_conn.execute(table.delete())
    try:
        for workout in workouts:
            db_conn.execute(table.insert().values(workout))
    finally:
        db_conn.commit()
    
    #db_conn.execute(table.insert().values(workouts))
    #db_conn.commit()