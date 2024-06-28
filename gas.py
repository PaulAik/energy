import requests
import json
import psycopg2
from datetime import datetime

# Initial API URL
base_url = "https://api.octopus.energy/v1/gas-meter-points/56743804/meters/E6E11808992323/consumption/"

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
            # Get the next URL, or None if it doesn't exist
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
    CREATE TABLE IF NOT EXISTS gas_consumption (
        id SERIAL PRIMARY KEY,
        consumption FLOAT,
        interval_start TIMESTAMP UNIQUE,
        interval_end TIMESTAMP
    )
    """)

    # Insert data
    for result in data:
        cur.execute("""
        INSERT INTO gas_consumption (consumption, interval_start, interval_end)
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
    all_data = fetch_all_data(base_url)
    print(f"Total records fetched: {len(all_data)}")
    save_to_database(all_data)
    print("Data successfully saved to database.")


if __name__ == "__main__":
    main()
