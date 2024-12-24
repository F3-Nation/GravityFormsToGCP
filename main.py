import os
import logging

from gravity_forms import GravityForms, OrgTypes

from google.cloud.sql.connector import Connector, IPTypes
import google.cloud.logging
import pg8000

from sqlalchemy import Table, MetaData, create_engine, insert
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
googleLoggingClient = google.cloud.logging.Client()
googleLoggingClient.setup_logging()

import_sectors = False
import_areas = False
import_regions = False
import_locations = False
import_events = False
import_event_types = False
import_event_types_mapping = True

def format_time(raw_time: str) -> str:

    ampm_indicator = raw_time[6].lower()
    hours = int(raw_time[:2])
    colon_minutes = raw_time[2:5]

    if ampm_indicator == "p" and hours < 12:
        hours = hours + 12
    
    return str(hours).zfill(2) + colon_minutes

def format_time_start(raw_time: str) -> str:
    return format_time(raw_time=raw_time[:8])

def format_time_end(raw_time: str) -> str:
    return format_time(raw_time=raw_time[11:])

def format_day_of_week(day_str: str) -> int:
    if day_str == "Monday":
        return 0
    elif day_str == "Tuesday":
        return 1
    elif day_str == "Wednesday":
        return 2
    elif day_str == "Thursday":
        return 3
    elif day_str == "Friday":
        return 4
    elif day_str == "Saturday":
        return 5
    elif day_str == "Sunday":
        return 6

def format_start_date(raw_date: str) -> str:
    just_date = raw_date[:10]
    
    if just_date == '0000-00-00':
        return '2024-01-01'
    else:
        return just_date

def format_event_type(event_type_raw: str) -> str:
    if event_type_raw == "Cycling":
        return "Bike"
    elif event_type_raw == "Strength/Conditioning/Tabata/WIB" or event_type_raw == "CORE":
        return "Bootcamp"
    elif event_type_raw == "Obstacle Training" or event_type_raw == "Sandbag":
        return "Gear"
    elif event_type_raw == "Mobility/Stretch":
        return "Mobility"
    elif event_type_raw == "Run with Pain Stations" or event_type_raw == "Speed/Strength Running":
        return "Run"
    else:
        return event_type_raw

def remove_duplicates(raw_list: list) -> list:
    seen = set()
    new_list = []
    for d in raw_list:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            new_list.append(d)
    return new_list

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
        tableEventTypes = Table('event_types',MetaData(), autoload_with=pool)
        tableEventTypesXOrgs = Table('event_types_x_org',MetaData(), autoload_with=pool)
        tableEventTypesXEvents = Table('events_x_event_types',MetaData(), autoload_with=pool)

        #############################################
        # Sectors

        if import_sectors:
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
                db_conn.execute(tableOrgs.insert().values(sector))

            db_conn.commit()

        #############################################
        # Areas

        if import_areas:
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
                db_conn.execute(tableOrgs.insert().values(area))

            db_conn.commit()

        #############################################
        # Regions

        if import_regions:
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
                db_conn.execute(tableOrgs.insert().values(region))

            db_conn.commit()

        #############################################
        # Interlude
        
        if import_locations or import_events or import_event_types or import_event_types_mapping:
            logging.info("Reading 'orgs' table to get region IDs")
            orgRegions = pd.read_sql_query("SELECT * FROM orgs WHERE org_type_id = " + str(orgTypeRegionId), db_conn, "name")
            
            aos_gf = gravity_forms.get_entries(OrgTypes.AO)
        
        #############################################
        # Locations

        if import_locations:

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
                    'address_country': None if location_gf["Is this address accurate?"] == "No" else location_gf["Address Country"],
                    'meta' : {'gravity_form_id' : location_gf["id"]}
                })
        
            logging.info("Inserting Locations into 'locations' table")
            for location in locations:
                db_conn.execute(tableLocations.insert().values(location))
            
            db_conn.commit()

        #############################################
        # Events

        if import_events:  

            location_ids_and_gf = pd.read_sql_query("SELECT id, meta ->> 'gravity_form_id' as gf_id FROM locations", db_conn, "gf_id")

            events = []
            for event_gf in aos_gf:
                events.append({
                    'org_id': orgRegions.loc[event_gf["Region"], "id"],
                    'location_id' : location_ids_and_gf.loc[event_gf["id"], "id"],
                    'name': event_gf["Workout Name"],
                    'description' : event_gf["Workout Notes"],
                    'is_series': True,
                    'is_active': True,
                    'highlight' : False,
                    'start_date': format_start_date(event_gf["date_created"]),
                    'start_time': format_time_start(event_gf["Time of Day"]),
                    'end_time': format_time_end(event_gf["Time of Day"]),
                    'day_of_week' : format_day_of_week(event_gf["Day of the Week"]),
                    'recurrence_pattern' : 'weekly'
                })
            
            logging.info("Inserting Events into 'events' table")
            db_conn.execute(insert(tableEvents), events)           
            db_conn.commit()

        #############################################
        # Event Types

        if import_event_types:
            event_types = [
                {'name': "Bike", 'acronym': "BK", 'category_id': 1},
                {'name': "Bootcamp", 'acronym': "BC", 'category_id': 1},
                {'name': "Gear", 'acronym': "GE", 'category_id': 1},
                {'name': "Mobility", 'acronym': "MO", 'category_id': 1},
                {'name': "Ruck", 'acronym': "RK", 'category_id': 1},
                {'name': "Run", 'acronym': "RN", 'category_id': 1},
                {'name': "Swimming", 'acronym': "SW", 'category_id': 1},
                {'name': "Wild Card", 'acronym': "WC", 'category_id': 1}
            ]
        
            logging.info("Inserting predefined Event Types into 'event_types' table")
            db_conn.execute(insert(tableEventTypes), event_types)
            db_conn.commit()

        #############################################
        # Event Type Mapping

        if import_event_types_mapping:
            logging.info("Reading 'event_types' table to get event type IDs")
            event_type_ids = pd.read_sql_query("SELECT * FROM event_types", db_conn, "name")
            event_ids_by_gf = pd.read_sql_query("select e.id as event_id, l.id as location_id, l.org_id as region_id, l.meta ->> 'gravity_form_id' as gf_id from events e  left join locations l on l.id = e.location_id", db_conn, "gf_id")

            event_types_x_orgs = []
            event_types_x_events = []
            for event_gf in aos_gf:
                event_types_x_orgs.append({
                    'event_type_id': event_type_ids.loc[format_event_type(event_gf["Workout Type"]), "id"],
                    'org_id' : event_ids_by_gf.loc[event_gf["id"], "region_id"],
                    'is_default': False
                })

                event_types_x_events.append({
                    'event_type_id': event_type_ids.loc[format_event_type(event_gf["Workout Type"]), "id"],
                    'event_id' : event_ids_by_gf.loc[event_gf["id"], "event_id"]
                })
            
            event_types_x_orgs = remove_duplicates(event_types_x_orgs)

            logging.info("Inserting Event Type Mappings into 'event_types_x_org' table")
            db_conn.execute(insert(tableEventTypesXOrgs), event_types_x_orgs)
            db_conn.commit()

            logging.info("Inserting Event Type Mappings into 'events_x_event_types' table")
            db_conn.execute(insert(tableEventTypesXEvents), event_types_x_events)
            db_conn.commit()

    logging.info("Done.")

app()

# try:
#     app()
# except Exception as error:
#     logging.error("Could not run job. Error: " + str(error))