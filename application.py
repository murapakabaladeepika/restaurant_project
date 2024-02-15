import logging
import time
import configparser
import csv
import mysql.connector
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import requests

class RestaurantFinder:
    def __init__(self, config_file_path, csv_file_path):
        self.config = self.load_config(config_file_path)
        self.csv_file_path = csv_file_path
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def load_config(self, config_file_path):
        config = configparser.ConfigParser()
        config.read(config_file_path)
        return config['Credentials']

    def get_location_coordinates(self, address):
        geolocator = Nominatim(user_agent="my-app")
        try:
            location = geolocator.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude
            else:
                return None
        except Exception as e:
            self.logger.error(f"Geolocation Error: {e}")
            return None

    def string_coordinates(self, location_coordinates):
        lat, long = location_coordinates
        return f"{lat},{long}"

    def fetch_data_with_retry(self, lat_long, radius, category, user_limit, max_retries=3, initial_delay=1):
        for attempt in range(max_retries):
            try:
                return self.fetch_data(lat_long, radius, category, user_limit)
            except requests.exceptions.RequestException as req_err:
                if isinstance(req_err, requests.exceptions.HTTPError) and req_err.response.status_code == 503:
                    delay = initial_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(f"503 Service Unavailable. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to fetch data. Error: {req_err}")
                    break
        else:
            self.logger.error("Failed to fetch data after multiple retries.")
            return None


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
        try:
            response = requests.get(URL, params=params, headers=headers)
            if response.status_code == 503:
                self.logger.warning("503 Service Unavailable. Retrying...")
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"API Request Error: {req_err}")
            return None

    def write_json_data_to_csv(self, address, radius, user_limit, user_lat, user_long, category, json_data, csv_file_path):
        with open(csv_file_path, "a", newline="", encoding="utf-8") as csvfile:
            csv_writer = csv.writer(csvfile)
            for i in range(len(json_data.get("results", []))):
                rating = json_data["results"][i].get("rating", 0)
                csv_writer.writerow(
                    [
                        address,
                        radius,
                        user_limit,
                        user_lat,
                        user_long,
                        category,
                        json_data["results"][i]["name"],
                        json_data["results"][i]["geocodes"]["main"]["latitude"],
                        json_data["results"][i]["geocodes"]["main"]["longitude"],
                        json_data["results"][i]["distance"],
                        rating,
                    ]
                )

    def connect_to_database(self):
        try:
            return mysql.connector.connect(
                host=self.config['HOST'],
                user=self.config['USER'],
                password=self.config['PASSWORD'],
                database=self.config['DATABASE']
            )
        except mysql.connector.Error as conn_err:
            self.logger.error(f"Database Connection Error: {conn_err}")
            return None

    def create_table_if_not_exists(self, connection):
        try:
            with connection.cursor() as cursor:
                table_schema = (
                    "CREATE TABLE IF NOT EXISTS restaurant_data ("
                    "id INT AUTO_INCREMENT PRIMARY KEY,"
                    "address VARCHAR(255),"
                    "radius INT,"
                    "user_limit INT,"
                    "user_lat FLOAT,"
                    "user_long FLOAT,"
                    "category VARCHAR(255),"
                    "name VARCHAR(255),"
                    "latitude FLOAT,"
                    "longitude FLOAT,"
                    "distance INT,"
                    "rating FLOAT,"
                    "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
                )
                cursor.execute(table_schema)
                connection.commit()
        except mysql.connector.Error as table_err:
            self.logger.error(f"Table Creation Error: {table_err}")

    def get_last_updated_time(self, connection, address):
        try:
            with connection.cursor(dictionary=True) as cursor:
                query = "SELECT last_updated FROM restaurant_data WHERE address = %s ORDER BY last_updated DESC LIMIT 1"
                cursor.execute(query, (address,))
                result = cursor.fetchone()
                return result['last_updated'] if result else None
        except mysql.connector.Error as last_updated_err:
            self.logger.error(f"Last Updated Time Retrieval Error: {last_updated_err}")
            return None

    def delete_and_update_data(self, connection, json_data, address, radius, user_limit, user_lat, user_long, category):
        try:
            with connection.cursor() as cursor:
                # Delete existing data for the given address
                delete_query = "DELETE FROM restaurant_data WHERE address = %s"
                cursor.execute(delete_query, (address,))
                # Insert new data
                for i in range(len(json_data.get("results", []))):
                    rating = json_data["results"][i].get("rating", 0)
                    data = (
                        address,
                        radius,
                        user_limit,
                        user_lat,
                        user_long,
                        category,
                        json_data["results"][i]["name"],
                        json_data["results"][i]["geocodes"]["main"]["latitude"],
                        json_data["results"][i]["geocodes"]["main"]["longitude"],
                        json_data["results"][i]["distance"],
                        rating,
                    )
                    self.insert_data_into_database(connection, data)
            self.fetch_and_display_top_restaurants(connection, address)
        except mysql.connector.Error as delete_update_err:
            self.logger.error(f"Delete and Update Error: {delete_update_err}")

    def insert_data_into_database(self, connection, data):
        try:
            with connection.cursor() as cursor:
                insert_query = (
                    "INSERT INTO restaurant_data "
                    "(address, radius, user_limit, user_lat, user_long, category, name, latitude, longitude, distance, rating) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                cursor.execute(insert_query, data)
                connection.commit()
        except mysql.connector.Error as insert_err:
            self.logger.error(f"Insertion Error: {insert_err}")

    def fetch_and_display_top_restaurants(self, connection, user_location):
        try:
            # Query to get top 5 restaurant names based on ratings for the specific user location
            top_restaurants_query = (
                "SELECT name, rating "
                "FROM restaurant_data "
                "WHERE address = %s "
                "ORDER BY rating DESC LIMIT 5"
            )

            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(top_restaurants_query, (user_location,))
                top_restaurants = cursor.fetchall()

            if top_restaurants:
                print("\nTop 5 Restaurants based on Ratings in", user_location, ":")
                for idx, restaurant in enumerate(top_restaurants, 1):
                    print(f"{idx}. {restaurant['name']} - Rating: {restaurant['rating']}")
            else:
                print("No data found in the database for", user_location)

        except mysql.connector.Error as top_restaurants_err:
            self.logger.error(f"Top Restaurants Retrieval Error: {top_restaurants_err}")

    def main(self):
        try:
            address = input("Enter an address (landmark/area, state, country): ")
            coordinates = self.get_location_coordinates(address)
            if coordinates:
                user_lat, user_long = coordinates
                lat_long = self.string_coordinates(coordinates)
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
                        with self.connect_to_database() as connection:
                            self.create_table_if_not_exists(connection)
                            json_data = self.fetch_data_with_retry(lat_long, radius, category, user_limit)
                            if json_data is not None:
                                self.write_json_data_to_csv(
                                    address,
                                    radius,
                                    user_limit,
                                    user_lat,
                                    user_long,
                                    category,
                                    json_data,
                                    self.csv_file_path,
                                )

                                last_updated_time = self.get_last_updated_time(connection, address)

                                if last_updated_time is None or (datetime.now() - last_updated_time) > timedelta(weeks=4):
                                    self.delete_and_update_data(connection, json_data, address, radius, user_limit, user_lat, user_long, category)
                                else:
                                    self.fetch_and_display_top_restaurants(connection, address)

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
