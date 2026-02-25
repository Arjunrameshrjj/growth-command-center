import pandas as pd
import numpy as np

# Sample data mimicking the JSON response
data = [
    {'Student Name': 'NIMMY JOHNY', 'Payment Paid Date/ Initial Amount Paid Date': '2026-01-15T18:30:00.000Z'},
    {'Student Name': 'NIMMY MOL', 'Payment Paid Date/ Initial Amount Paid Date': '2026-01-31T18:30:00.000Z'}
]
df = pd.DataFrame(data)
date_col = 'Payment Paid Date/ Initial Amount Paid Date'

print("--- Dataframe Info ---")
print(df.info())

print("\n--- Parsing Attempt 1 (Current Logic in Flask) ---")
dt_series = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True, utc=True)
parsed = dt_series.dt.tz_convert('Asia/Kolkata').dt.date
print(f"Results 1: {parsed.tolist()}")

print("\n--- Parsing Attempt 2 (Simpler) ---")
dt_series = pd.to_datetime(df[date_col], errors='coerce')
print(f"Results 2: {dt_series.tolist()}")
