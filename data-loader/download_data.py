import datetime
import os
import requests
from concurrent.futures import ThreadPoolExecutor

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

SENSORS = [
    {
        "id": "86772",
        "type": "bme280",
        "dest": os.path.join(PROJECT_ROOT, "clickhouse-bme-data")
    },
    {
        "id": "86771",
        "type": "sds011",
        "dest": os.path.join(PROJECT_ROOT, "clickhouse-sds-data")
    }
]

BASE_URL = "https://archive.sensor.community"
YEAR = 2026
START_DATE = datetime.date(YEAR, 1, 1)
END_DATE = datetime.date(YEAR, 4, 27)  # Current date in simulation

def download_file(date_str, sensor):
    # Try both standard and _indoor patterns
    patterns = [
        f"{date_str}_{sensor['type']}_sensor_{sensor['id']}.csv",
        f"{date_str}_{sensor['type']}_sensor_{sensor['id']}_indoor.csv"
    ]
    
    os.makedirs(sensor['dest'], exist_ok=True)
    
    for filename in patterns:
        url = f"{BASE_URL}/{date_str}/{filename}"
        dest_path = os.path.join(sensor['dest'], filename)
        
        if os.path.exists(dest_path):
            print(f"Skipping {filename} (already exists)")
            return True

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(dest_path, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded: {filename}")
                return True
            elif response.status_code == 404:
                continue # Try next pattern
            else:
                print(f"Failed to download {filename}: HTTP {response.status_code}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
    
    # print(f"No data found for {sensor['type']} {sensor['id']} on {date_str}")
    return False

def main():
    dates = []
    curr = START_DATE
    while curr <= END_DATE:
        dates.append(curr.strftime("%Y-%m-%d"))
        curr += datetime.timedelta(days=1)

    print(f"Starting download for {len(dates)} days...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        for date_str in dates:
            for sensor in SENSORS:
                executor.submit(download_file, date_str, sensor)

if __name__ == "__main__":
    main()
