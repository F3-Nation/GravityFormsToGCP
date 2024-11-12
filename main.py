import os
import logging

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

        logging.info("Loading metadata (columns) for tables.")
        tableOrgs = Table('orgs',MetaData(), autoload_with=pool)
        tableLocations = Table('locations',MetaData(), autoload_with=pool)
        tableEvents = Table('events',MetaData(), autoload_with=pool)

        #############################################
        # Sectors

        logging.info("Loading metadata for table 'orgs'.")
        
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

        logging.info("Inserting Areas into 'orgs' table")
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
                'org_type_id': orgTypeRegionId,
                'parent_id': orgAreas.loc[region_gf["Area"], "id"],
                'name': region_gf["Region Name"],
                'is_active': True,
                'website': None if region_gf["Region Website"] == '' or 'f3nation.com' in region_gf["Region Website"] or 'facebook.com' in region_gf["Region Website"] or 'fb.me' in region_gf["Region Website"] else region_gf["Region Website"],
                'email': None if region_gf["General Email"] == '' else region_gf["General Email"],
                'twitter': None if region_gf["Region Twitter Handle"] == '' else region_gf["Region Twitter Handle"],
                'facebook': region_gf["Region Website"] if 'facebook.com' in region_gf["Region Website"] or 'fb.me' in region_gf["Region Website"] else None
            })

        logging.info("Inserting Regions into 'orgs' table")
        for region in regions:
            #db_conn.execute(tableOrgs.insert().values(region))
            a=1

        db_conn.commit()

        #############################################
        # Locations

        logging.info("Reading 'orgs' table to get region IDs")
        orgRegions = pd.read_sql_query("SELECT * FROM orgs WHERE org_type_id = " + str(orgTypeRegionId), db_conn, "name")
        orgRegionsa = pd.read_sql_query("SELECT * FROM orgs WHERE org_type_id = " + str(orgTypeRegionId) + " AND name = 'Richmond (TX)'", db_conn, "name")

        aos_gf = gravity_forms.get_entries(OrgTypes.AO)
        locations = []
        for location_gf in aos_gf:
            locations.append({
                'org_id': orgRegions.loc[location_gf["Region"], "id"],
                'name': location_gf["Workout Name"] + " (" + location_gf["Day of the Week"] + ")",
                'is_active': True,
                'lat': location_gf["Latitude"],
                'lon': location_gf["Longitude"],
                'address_street': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address Street Address"] + " " + location_gf["Address Address Line 2"],
                'address_city': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address City"],
                'address_state': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address State / Province / Region"],
                'address_zip': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address ZIP / Postal Code"],
                'address_country': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address Country"]
            })

        logging.info("Inserting Locations into 'locations' table")
        for location in locations:
            db_conn.execute(tableLocations.insert().values(location))
            a=1

        db_conn.commit()

    logging.info("Done.")

app()

# try:
#     app()
# except Exception as error:
#     logging.error("Could not run job. Error: " + str(error))