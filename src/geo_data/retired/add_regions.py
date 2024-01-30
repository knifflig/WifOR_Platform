"""
This script saves region data from a GeoJSON file to a SQL database. 
It takes the path of the GeoJSON file as an input and logs the process 
and errors into a file with a timestamp.

Run for example with:
poetry run python add_regions.py 'ref-nuts-2021\\NUTS_RG_01M_2021_4326.geojson'
"""

import os
import argparse
import geopandas as gpd
from geo_data import Regions
from wifor_db import setup_logger

# set up logger
script_dir = os.path.dirname(os.path.abspath(__file__))
logger = setup_logger("add_regions", script_dir)
if logger:
    logger.info("Logging setup complete")
else:
    print("Logger setup failed.")

def save_regions_to_db(geo_df):
    """
    Saves region data from a GeoDataFrame to the SQL database.

    Parameters:
    geo_df (GeoDataFrame): A GeoDataFrame containing region data.
    """
    try:
        init_session = Regions.init_db()
        if init_session is None:
            logger.error("Database initialization failed.")
            return

        with init_session() as session:
            for _, row in geo_df.iterrows():
                region = Regions(
                    NUTS_ID=row['NUTS_ID'],
                    LEVL_CODE=row['LEVL_CODE'],
                    CNTR_CODE=row['CNTR_CODE'],
                    NAME_LATN=row['NAME_LATN'],
                    NUTS_NAME=row['NUTS_NAME'],
                    MOUNT_TYPE=row['MOUNT_TYPE'],
                    URBN_TYPE=row['URBN_TYPE'],
                    COAST_TYPE=row['COAST_TYPE'],
                    FID=row['FID']
                )
                session.add(region)
            session.commit()
            logger.info("Data successfully saved to the database.")

    # pylint: disable=broad-except, logging-fstring-interpolation
    except Exception as e:
        logger.exception(f"Failed to save regions to database: {e}")
    # pylint: enable=broad-except, logging-fstring-interpolation

def main():
    """
    Main function to parse command line arguments and initiate data saving process.
    """
    parser = argparse.ArgumentParser(description='Save GeoJSON data to SQL database.')
    parser.add_argument('geojson_path', type=str, help='Path to the GeoJSON file')

    args = parser.parse_args()

    try:
        geo_df = gpd.read_file(args.geojson_path)
        save_regions_to_db(geo_df)

    # pylint: disable=broad-except, logging-fstring-interpolation
    except Exception as e:
        logger.exception(f"Failed to process the file {args.geojson_path}: {e}")
    # pylint: enable=broad-except, logging-fstring-interpolation

if __name__ == '__main__':
    main()
