import requests
import pandas as pd
CONTENT_CALENDAR_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

resp = requests.get(CONTENT_CALENDAR_URL, timeout=30)
data = resp.json()
print(f"Total records: {len(data)}")
for i, row in enumerate(data[:10]):
    print(f"Row {i}: Scheduled: {row.get('Scheduled Date')}, Published: {row.get('Published Date')}")
    
# Test parsing
for i, row in enumerate(data[:10]):
    date_val = row.get("Scheduled Date") or row.get("Published Date") or ""
    try:
        parsed = pd.to_datetime(date_val, errors='coerce')
        print(f"Row {i} parsed: {parsed}")
    except:
        print(f"Row {i} parse FAILED")
