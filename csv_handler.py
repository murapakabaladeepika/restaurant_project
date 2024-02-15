import csv

def write_json_data_to_csv(csv_file_path, json_data, address, radius, user_limit, user_lat, user_long, category):
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
