import requests
import json
import psycopg2
from datetime import datetime

# API URL
base_url = "https://api.octopus.energy/v1/electricity-meter-points/1800015386601/meters/23J0516478/consumption/"

# Database connection parameters
db_params = {
    "dbname": "energy",
    "user": "admin",
    "password": ".password",
    "host": "localhost",
    "port": "5432"
}

headers = {'Authorization': 'Basic c2tfbGl2ZV8zb0h2aHRRNkNCQmdHeWFYNTZFc2pEUXY6'}


def fetch_all_data(url):
    all_results = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_results.extend(data['results'])
            url = data.get('next')
            print(f"Fetched {len(data['results'])} records. Next URL: {url}")
        else:
            raise Exception(
                f"API request failed with status code {response.status_code}")
    return all_results


def save_to_database(data):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS electricity_consumption (
        id SERIAL PRIMARY KEY,
        consumption FLOAT,
        interval_start TIMESTAMP UNIQUE,
        interval_end TIMESTAMP
    )
    """)

    # Insert data
    for result in data:
        cur.execute("""
        INSERT INTO electricity_consumption (consumption, interval_start, interval_end)
        VALUES (%s, %s, %s)
        ON CONFLICT (interval_start) DO UPDATE
        SET consumption = EXCLUDED.consumption,
            interval_end = EXCLUDED.interval_end
        """, (
            result['consumption'],
            datetime.fromisoformat(result['interval_start']),
            datetime.fromisoformat(result['interval_end'])
        ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    print("Fetching data from API...")
    all_data = fetch_all_data(base_url)
    print(f"Total records fetched: {len(all_data)}")

    print("Saving data to database...")
    save_to_database(all_data)
    print("Data successfully saved to database.")


if __name__ == "__main__":
    main()
