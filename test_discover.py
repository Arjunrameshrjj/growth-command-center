import requests
import pandas as pd
from datetime import datetime

CONTENT_CALENDAR_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

resp = requests.get(CONTENT_CALENDAR_URL, timeout=30)
data = resp.json()
print(f"Total records: {len(data)}")

today = datetime.now()
m1_start = today.replace(day=1).date()
# Get last day of month
if today.month == 12:
    m1_end = today.replace(year=today.year + 1, month=1, day=1).date() - pd.Timedelta(days=1)
else:
    m1_end = (today.replace(month=today.month + 1, day=1) - pd.Timedelta(days=1)).date()

print(f"Current range: {m1_start} to {m1_end}")

matches = 0
for row in data:
    date_val = row.get("Scheduled Date") or row.get("Published Date") or ""
    if date_val:
        try:
            parsed_date = pd.to_datetime(date_val, errors='coerce').date()
            if not pd.isna(parsed_date):
                if m1_start <= parsed_date <= m1_end:
                    matches += 1
        except:
            pass

print(f"Records matching this month: {matches}")
if matches == 0:
    print("\nSample dates from data:")
    for row in data[:5]:
        print(f"Scheduled: {row.get('Scheduled Date')}, Published: {row.get('Published Date')}")
