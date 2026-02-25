import pandas as pd
date_val = "Sun Feb 01 2026 00:00:00 GMT+0530 (India Standard Time)"

# Try stripping ( ... )
clean_val = date_val.split('(')[0].strip()
print(f"Original: {date_val}")
print(f"Cleaned: {clean_val}")

try:
    parsed = pd.to_datetime(clean_val, errors='coerce')
    print(f"Parsed Cleaned: {parsed}")
except Exception as e:
    print(f"Failed Cleaned: {e}")

# Try just the first 24 chars (Sun Feb 01 2026 00:00:00)
short_val = " ".join(date_val.split()[:4])
print(f"Short (first 4): {short_val}")
try:
    parsed = pd.to_datetime(short_val, errors='coerce')
    print(f"Parsed Short: {parsed}")
except Exception as e:
    print(f"Failed Short: {e}")
