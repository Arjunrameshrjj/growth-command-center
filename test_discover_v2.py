import requests
import pandas as pd
from datetime import datetime

CONTENT_CALENDAR_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

resp = requests.get(CONTENT_CALENDAR_URL, timeout=30)
data = resp.json()

# Mimic get_comparison_dates(0)
today = datetime.now()
m1_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
m1_end = today
print(f"MTD Range: {m1_start} to {m1_end}")

matches = 0
for i, row in enumerate(data):
    date_val = row.get("Scheduled Date") or row.get("Published Date") or ""
    if date_val:
        try:
            # APPLY FIX: Strip timezone suffix
            date_val_clean = date_val.split('(')[0].strip()
            
            parsed_date = pd.to_datetime(date_val_clean, errors='coerce')
            if pd.isna(parsed_date):
                # if i < 5: print(f"Row {i}: Still NaT for {date_val}")
                pass
            else:
                parsed_date_only = parsed_date.date()
                if m1_start.date() <= parsed_date_only <= m1_end.date():
                    matches += 1
                    if matches <= 5:
                        print(f"Match {matches}: {row.get('Content Topic')} on {parsed_date_only}")
        except Exception as e:
            pass

print(f"Total Matches: {matches}")
