import requests
import pandas as pd
from datetime import datetime
import pytz

try:
    # 1. Fetch raw data
    RENEW_SHEET_URL = "https://script.google.com/macros/s/AKfycbyid7u5OIJbemqEyawmvRsJyF6XmplsjNw-u9DqDuI7dm59hxSuykJOk2Yeeyc5riDtfg/exec"
    resp = requests.get(RENEW_SHEET_URL, timeout=30)
    raw_rows = resp.json()
    print(f"Total raw rows: {len(raw_rows)}")

    # 2. Simulate fixed Logic
    def parse_date_fixed(val):
        dt = pd.to_datetime(val, errors='coerce')
        if pd.isna(dt): return pd.NaT
        if dt.tz is None:
            dt = dt.tz_localize('UTC')
        return dt.tz_convert('Asia/Kolkata').date()

    print("\n--- Raw Data Check ---")
    targets = ['NIMMY MOL', 'JOMON C CHERIAN']
    for row in raw_rows:
        name = str(row.get('Student Name', ''))
        if any(t in name.upper() for t in targets):
            raw_date = row.get('Payment Paid Date/ Initial Amount Paid Date')
            parsed = parse_date_fixed(raw_date)
            print(f"Match found: {name}, Raw: {raw_date}, Parsed: {parsed}")

    # 3. Check current API result
    api_resp = requests.get('http://127.0.0.1:5000/api/renew?offset=0', timeout=60)
    data = api_resp.json()
    print(f"\nAPI M1 Count: {data.get('m1_count')}")
    print(f"API M1 Val: {data.get('m1_val').replace('₹', 'Rs ')}")
    
    recent = data.get('recent_renewals', [])
    print("\n--- Details of rows in API ---")
    for r in recent:
        print(f"Name: {r.get('Student Name')}, Fee: {r.get('Fee Amount')}, Date: {r.get('Payment Paid Date/ Initial Amount Paid Date')}")

except Exception as e:
    print(f"Error: {e}")
