import requests
import json

RENEW_URL = "https://script.google.com/macros/s/AKfycbyid7u5OIJbemqEyawmvRsJyF6XmplsjNw-u9DqDuI7dm59hxSuykJOk2Yeeyc5riDtfg/exec"
DISCOVER_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

def check_url(url, label):
    print(f"\n--- Checking {label} ---")
    try:
        resp = requests.get(url, timeout=20)
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type')}")
        if "application/json" in resp.headers.get('Content-Type', ''):
            data = resp.json()
            print(f"Success! Data is list: {isinstance(data, list)}")
            if isinstance(data, list):
                print(f"Count: {len(data)}")
                if len(data) > 0:
                    print("First item keys:", list(data[0].keys()))
            else:
                print("Data preview:", str(data)[:200])
        else:
            print("Response is NOT JSON.")
            print("Preview:", resp.text[:500])
    except Exception as e:
        print(f"Error: {e}")

check_url(RENEW_URL, "Renew Phase")
check_url(DISCOVER_URL, "Discover Phase")
