import requests
import time

def fetch_data_with_retry(URL, params, headers, max_retries=3, initial_delay=1):
    for attempt in range(max_retries):
        try:
            response = requests.get(URL, params=params, headers=headers)
            if response.status_code == 503:
                print("503 Service Unavailable. Retrying...")
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as req_err:
            print(f"API Request Error: {req_err}")
            if isinstance(req_err, requests.exceptions.HTTPError) and req_err.response.status_code == 503:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to fetch data. Error: {req_err}")
                break
    else:
        print("Failed to fetch data after multiple retries.")
        return None
