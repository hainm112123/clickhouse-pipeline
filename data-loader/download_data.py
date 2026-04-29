import datetime
import os
import requests
import gzip
import io
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

SENSORS = [
    {
        "id": "829",
        "type": "bme280",
        "dest": os.path.join(PROJECT_ROOT, "clickhouse-bme-data")
    },
    {
        "id": "828",
        "type": "sds011",
        "dest": os.path.join(PROJECT_ROOT, "clickhouse-sds-data")
    }
]

BASE_URL = "https://archive.sensor.community"
START_DATE = datetime.date(2017, 4, 1)
END_DATE = datetime.date(2026, 4, 27)  # Current date in simulation

def download_file(date_obj, sensor):
    date_str = date_obj.strftime("%Y-%m-%d")
    year = date_obj.year
    
    if year <= 2024:
        ext = ".csv.gz"
    else:
        ext = ".csv"

    patterns = [
        f"{date_str}_{sensor['type']}_sensor_{sensor['id']}{ext}",
        f"{date_str}_{sensor['type']}_sensor_{sensor['id']}_indoor{ext}"
    ]
    
    os.makedirs(sensor['dest'], exist_ok=True)
    
    for filename in patterns:
        if year <= 2024:
            url = f"{BASE_URL}/{year}/{date_str}/{filename}"
        else:
            url = f"{BASE_URL}/{date_str}/{filename}"
            
        csv_filename = filename[:-3] if filename.endswith(".gz") else filename
        dest_path = os.path.join(sensor['dest'], csv_filename)
        
        if os.path.exists(dest_path):
            return "skipped"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                if filename.endswith(".gz"):
                    try:
                        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f_in:
                            content = f_in.read()
                        with open(dest_path, "wb") as f_out:
                            f_out.write(content)
                        return "downloaded_gz"
                    except Exception as gz_err:
                        return f"error_gz: {gz_err}"
                else:
                    with open(dest_path, "wb") as f:
                        f.write(response.content)
                    return "downloaded"
            elif response.status_code == 404:
                continue 
            else:
                return f"http_error: {response.status_code}"
        except Exception as e:
            return f"error: {e}"
    
    return "not_found"

def main():
    print("Initializing date list...", flush=True)
    date_list = []
    curr = START_DATE
    while curr <= END_DATE:
        date_list.append(curr)
        curr += datetime.timedelta(days=1)
    
    total_dates = len(date_list)
    total_tasks = total_dates * len(SENSORS)
    print(f"Total tasks to schedule: {total_tasks} ({total_dates} days x {len(SENSORS)} sensors)", flush=True)

    print(f"Starting ThreadPoolExecutor with 10 workers...", flush=True)
    futures = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        print("Submitting tasks to executor...", flush=True)
        for i, date_obj in enumerate(date_list):
            for sensor in SENSORS:
                fut = executor.submit(download_file, date_obj, sensor)
                futures[fut] = (date_obj, sensor)
            if (i + 1) % 500 == 0:
                print(f"  Scheduled { (i+1) * len(SENSORS) } / {total_tasks} tasks...", flush=True)
        
        print("All tasks scheduled. Waiting for first results...", flush=True)
        
        completed = 0
        skipped = 0
        downloaded = 0
        failed = 0
        
        for future in as_completed(futures):
            completed += 1
            try:
                res = future.result()
            except Exception as e:
                res = f"critical_error: {e}"
            
            if res == "skipped":
                skipped += 1
            elif res.startswith("downloaded"):
                downloaded += 1
                date_obj, sensor = futures[future]
                print(f"[{completed}/{total_tasks}] Downloaded {sensor['type']} for {date_obj}", flush=True)
            elif res == "not_found":
                pass 
            else:
                failed += 1
                date_obj, sensor = futures[future]
                print(f"[{completed}/{total_tasks}] Failed {sensor['type']} for {date_obj}: {res}", flush=True)

            if completed % 100 == 0:
                print(f"--- Status: {completed}/{total_tasks} (Down: {downloaded}, Skip: {skipped}, Fail: {failed}) ---", flush=True)

    print(f"\nFinal Summary: Total={total_tasks}, Downloaded={downloaded}, Skipped={skipped}, Failed={failed}", flush=True)

if __name__ == "__main__":
    main()
