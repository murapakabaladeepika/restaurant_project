from geopy.geocoders import Nominatim

def get_location_coordinates(address):
    geolocator = Nominatim(user_agent="my-app")
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return None
    except Exception as e:
        print(f"Geolocation Error: {e}")
        return None

def string_coordinates(location_coordinates):
    lat, long = location_coordinates
    return f"{lat},{long}"
