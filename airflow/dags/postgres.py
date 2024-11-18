import psycopg2
from psycopg2 import sql

# Define connection parameters for the PostgreSQL server
SERVER_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'mypassword',
    'host': 'myhost',  
    'port': 'mypost'          
}

# Name of the database to create
DATABASE_NAME = 'weather_api'

# Define the SQL query to create the table
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS weather_data_chicago (
    event_timestamp INTEGER,
    name TEXT,
    coord_lat FLOAT,
    coord_lon FLOAT,
    dt INTEGER,
    dt_chicago TEXT,
    dt_thailand TEXT,
    sys_sunrise INTEGER,
    sys_sunrise_chicago TEXT,
    sys_sunrise_thailand TEXT,
    sys_sunset INTEGER,
    sys_sunset_chicago TEXT,
    sys_sunset_thailand TEXT,
    main_temp FLOAT,
    main_temp_celsius INTEGER,
    main_feels_like FLOAT,
    main_feels_like_celsius INTEGER,
    main_temp_min FLOAT,
    main_temp_min_celsius INTEGER,
    main_temp_max FLOAT,
    main_temp_max_celsius INTEGER,
    main_pressure INTEGER,
    main_humidity INTEGER,
    main_sea_level INTEGER,
    main_grnd_level INTEGER,
    wind_speed FLOAT,
    base TEXT,
    visibility INTEGER,
    weather_id INTEGER,
    weather_main TEXT,
    weather_description TEXT
);
"""

def create_database():
    try:
        # Connect to the PostgreSQL server
        connection = psycopg2.connect(**SERVER_PARAMS)
        connection.autocommit = True
        cursor = connection.cursor()

        # Check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE_NAME}';")
        exists = cursor.fetchone()

        if not exists:
            # Create the database if it doesn't exist
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(DATABASE_NAME)))
            print(f"Database '{DATABASE_NAME}' created successfully.")
        else:
            print(f"Database '{DATABASE_NAME}' already exists.")

    except Exception as e:
        print(f"Error while creating database: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def create_table():
    try:
        # Define connection parameters for the specific database
        DB_PARAMS = SERVER_PARAMS.copy()
        DB_PARAMS['dbname'] = DATABASE_NAME

        # Connect to the specific database
        connection = psycopg2.connect(**DB_PARAMS)
        cursor = connection.cursor()

        # Execute the CREATE TABLE query
        cursor.execute(CREATE_TABLE_QUERY)
        connection.commit()

        print("Table created successfully or already exists.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_database()
    create_table()
