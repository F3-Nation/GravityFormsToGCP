import os
import logging

from google_sheets import GoogleSheets

from google.cloud.sql.connector import Connector, IPTypes
import google.cloud.logging
import pg8000

from sqlalchemy import Table, MetaData, create_engine

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
googleLoggingClient = google.cloud.logging.Client()
googleLoggingClient.setup_logging()

def app():
    logging.info("Pulling workouts from Google Sheets.")
    google_sheets = GoogleSheets()
    workouts = google_sheets.get_all_workouts()
    logging.info("Pulled " + str(len(workouts)) + ".")

    logging.info("Getting input variables.")
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]

    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=IPTypes.PUBLIC,
        )
        return conn

    logging.info("Creating PostgreSQL engine.")
    pool = create_engine(
        "postgresql+pg8000://",
        creator=getconn
    )

    logging.info("Connecting to '" + instance_connection_name + "' with username '" + db_user + "', database '" + db_name + "'.")
    with pool.connect() as db_conn:
        
        logging.info("Loading table 'gravityformworkouts'.")
        table = Table('gravityformworkouts',MetaData(), autoload_with=pool)
        
        logging.info("Deleting workouts currently in GCP.")
        db_conn.execute(table.delete())
        logging.info("Deletion complete (not yet committed).")

        logging.info("Writing workouts one at a time.")
        try:
            for workout in workouts:
                db_conn.execute(table.insert().values(workout))
        except Exception as error:
            logging.error("Could not write data to GCP. Not updating table. Error: " + str(error))
        else:
            logging.info("All workouts written. Committing.")
            db_conn.commit()

    logging.info("Done.")

if __name__ == "__main__":
    try:
        app()
    except Exception as error:
        logging.error("Could not run job. Error: " + str(error))