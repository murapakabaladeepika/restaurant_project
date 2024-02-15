import logging
import csv
from datetime import datetime, timedelta
import config
from logger import setup_logging
import database
import geolocation
import mysql.connector
from data_fetcher import fetch_data_with_retry
from csv_handler import write_json_data_to_csv
from database import (
    get_last_updated_time,
    delete_and_update_data,
    insert_data_into_database,
    fetch_and_display_top_restaurants
)

class RestaurantFinder:
    def __init__(self, config_file_path, csv_file_path):
        self.config = config.load_config(config_file_path)
        self.csv_file_path = csv_file_path
        self.logger = logging.getLogger(__name__)
        setup_logging()

    def fetch_data(self, lat_long, radius, category, user_limit):
        URL = "https://api.foursquare.com/v3/places/search"
        params = {
            "query": category,
            "ll": lat_long,
            "sort": "rating",
            "fields": "name,geocodes,distance,rating",
            "radius": radius,
            "limit": user_limit,
        }
        headers = {
            "Accept": "application/json",
            "Authorization": self.config['API_KEY'],
        }
        return fetch_data_with_retry(URL, params, headers)

    def main(self):
        try:
            address = input("Enter an address (landmark/area, state, country): ")
            coordinates = geolocation.get_location_coordinates(address)
            if coordinates:
                user_lat, user_long = coordinates
                lat_long = geolocation.string_coordinates(coordinates)
                target_data = [address, radius, user_lat, user_long]
                data_found = False
                with open(self.csv_file_path, "w+", newline="", encoding="utf-8") as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        if all(item in row for item in map(str, target_data)):
                            data_found = True
                            break
                if not data_found:
                    try:
                        with database.connect_to_database(self.config) as connection:
                            database.create_table_if_not_exists(connection)
                            json_data = self.fetch_data(lat_long, radius, category, user_limit)
                            if json_data is not None:
                                write_json_data_to_csv(
                                    self.csv_file_path,
                                    json_data,
                                    address,
                                    radius,
                                    user_limit,
                                    user_lat,
                                    user_long,
                                    category,
                                )

                                last_updated_time = get_last_updated_time(connection, address)

                                if last_updated_time is None or (datetime.now() - last_updated_time) > timedelta(weeks=4):
                                    delete_and_update_data(connection, json_data, address, radius, user_limit, user_lat, user_long, category)
                                else:
                                    fetch_and_display_top_restaurants(connection, address)

                    except mysql.connector.Error as conn_err:
                        self.logger.error(f"Database Connection Error: {conn_err}")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    config_file_path = ".config"
    csv_file_path = "restaurant-1.csv"
    radius = 5000
    category = "Restaurants"
    user_limit = 50

    restaurant_finder = RestaurantFinder(config_file_path, csv_file_path)
    restaurant_finder.main()
