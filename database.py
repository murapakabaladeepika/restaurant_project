import mysql.connector

def connect_to_database(config):
    try:
        return mysql.connector.connect(
            host=config['HOST'],
            user=config['USER'],
            password=config['PASSWORD'],
            database=config['DATABASE']
        )
    except mysql.connector.Error as conn_err:
        print(f"Database Connection Error: {conn_err}")
        return None

def create_table_if_not_exists(connection):
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
        print(f"Table Creation Error: {table_err}")

def get_last_updated_time(connection, address):
    try:
        with connection.cursor(dictionary=True) as cursor:
            query = "SELECT last_updated FROM restaurant_data WHERE address = %s ORDER BY last_updated DESC LIMIT 1"
            cursor.execute(query, (address,))
            result = cursor.fetchone()
            return result['last_updated'] if result else None
    except mysql.connector.Error as last_updated_err:
        print(f"Last Updated Time Retrieval Error: {last_updated_err}")
        return None

def delete_and_update_data(connection, json_data, address, radius, user_limit, user_lat, user_long, category):
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
                insert_data_into_database(connection, data)
        fetch_and_display_top_restaurants(connection, address)
    except mysql.connector.Error as delete_update_err:
        print(f"Delete and Update Error: {delete_update_err}")

def insert_data_into_database(connection, data):
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
        print(f"Insertion Error: {insert_err}")

def fetch_and_display_top_restaurants(connection, user_location):
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
        print(f"Top Restaurants Retrieval Error: {top_restaurants_err}")
