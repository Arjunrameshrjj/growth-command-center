import requests
import pandas as pd
from datetime import datetime

CONTENT_CALENDAR_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

def test_cc():
    print("Fetching Content Calendar...")
    resp = requests.get(CONTENT_CALENDAR_URL, timeout=30)
    data = resp.json()
    print(f"Total rows: {len(data)}")
    
    zumba_rows = [r for r in data if r.get("Sheet") == "ZUMBA"]
    print(f"Zumba rows: {len(zumba_rows)}")
    
    m1_start = datetime(2026, 2, 1).date()
    m1_end = datetime(2026, 2, 21).date()
    
    p_count = 0
    t_count = 0
    for i, row in enumerate(zumba_rows):
        topic = row.get("Content Topic")
        sched = row.get("Scheduled Date")
        pub = row.get("Published Date")
        status_raw = (row.get("Status") or "").strip().lower()
        
        # New priority: Published first
        date_val = pub or sched or ""
        parsed = None
        if date_val:
            try:
                date_val_clean = date_val.split('(')[0].strip()
                parsed = pd.to_datetime(date_val_clean, errors='coerce', dayfirst=True)
                if not pd.isna(parsed):
                    parsed = parsed.date()
            except:
                pass
        
        # Consistent with flask_app.py skip logic
        if not any([topic, row.get("Status"), sched]):
            continue
            
        in_range = False
        if parsed and m1_start <= parsed <= m1_end:
            in_range = True
            
        if in_range:
            t_count += 1
            if status_raw == "published":
                p_count += 1
            print(f"Row {i+1}: {topic[:20]} | Sched: {sched[:10]}... | Status: {status_raw} | Parsed: {parsed} | IN RANGE")
        else:
            print(f"Row {i+1}: {topic[:20]} | Sched: {sched[:10] if sched else 'N/A'}... | Status: {status_raw} | Parsed: {parsed} | OUT")

    print(f"\nFinal Calculated Counts for February (1-21):")
    print(f"Total: {t_count}")
    print(f"Published: {p_count}")

if __name__ == "__main__":
    test_cc()
