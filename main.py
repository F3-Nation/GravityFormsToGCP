import os
import logging

from google_sheets import GoogleSheets
from gravity_forms import GravityForms, OrgTypes

from google.cloud.sql.connector import Connector, IPTypes
import google.cloud.logging
import pg8000

from sqlalchemy import Table, MetaData, create_engine
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
googleLoggingClient = google.cloud.logging.Client()
googleLoggingClient.setup_logging()

def app():
    gravity_forms = GravityForms()
    
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
        
        logging.info("Loading table 'org_types'.")
        orgTypes = pd.read_sql_table("org_types", db_conn, index_col="name")
        orgTypeSectorId = orgTypes.loc["Sector", "id"]
        orgTypeAreaId = orgTypes.loc["Area", "id"]
        orgTypeRegionId = orgTypes.loc["Region", "id"]

        #############################################
        # Sectors

        logging.info("Loading metadata for table 'orgs'.")
        tableOrgs = Table('orgs',MetaData(), autoload_with=pool)
        
        sectors = [
            {'org_type_id': orgTypeSectorId, 'name': "West", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "North Central", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "Northeast", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "South Central", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "Southeast", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "Mid Southeast", 'is_active': True},
            {'org_type_id': orgTypeSectorId, 'name': "International", 'is_active': True}
        ]

        logging.info("Inserting Sectors into 'orgs' table")
        for sector in sectors:
            #db_conn.execute(tableOrgs.insert().values(sector))
            a=1

        db_conn.commit()

        #############################################
        # Areas

        logging.info("Reading 'orgs' table to get sector IDs")
        orgSectors = pd.read_sql_query("SELECT * FROM orgs WHERE org_type_id = " + str(orgTypeSectorId), db_conn, "name")
        
        areas_gf = gravity_forms.get_entries(OrgTypes.Area)
        areas = []
        for area_gf in areas_gf:
            areas.append({
                'org_type_id': orgTypeAreaId,
                'parent_id': orgSectors.loc[area_gf["Sector"], "id"],
                'name': area_gf["Area Name"],
                'is_active': True
            })

        for area in areas:
            #db_conn.execute(tableOrgs.insert().values(area))
            a=1

        db_conn.commit()

        #############################################
        # Regions

        logging.info("Reading 'orgs' table to get area IDs")
        orgAreas = pd.read_sql_query("SELECT * FROM orgs WHERE org_type_id = " + str(orgTypeAreaId), db_conn, "name")

        regions_gf = gravity_forms.get_entries(OrgTypes.Region)
        regions = []
        for region_gf in regions_gf:
            regions.append({
                'org_type_id': orgTypeSectorId,
                'parent_id': orgAreas.loc[region_gf["Area"], "id"],
                'name': region_gf["Region Name"],
                'is_active': True,
                'website': None if region_gf["Region Website"] == '' or 'f3nation.com' in region_gf["Region Website"] or 'facebook.com' in region_gf["Region Website"] or 'fb.me' in region_gf["Region Website"] else region_gf["Region Website"],
                'email': None if region_gf["General Email"] == '' else region_gf["General Email"],
                'twitter': None if region_gf["Region Twitter Handle"] == '' else region_gf["Region Twitter Handle"],
                'facebook': region_gf["Region Website"] if 'facebook.com' in region_gf["Region Website"] or 'fb.me' in region_gf["Region Website"] else None
            })

        for region in regions:
            #db_conn.execute(tableOrgs.insert().values(region))
            a=1

        db_conn.commit()

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

app()

# try:
#     app()
# except Exception as error:
#     logging.error("Could not run job. Error: " + str(error))