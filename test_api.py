import requests
import sys

BASE_URL = "http://127.0.0.1:5000"

def test_root():
    try:
        url = f"{BASE_URL}/"
        print(f"Testing {url}...")
        resp = requests.get(url, timeout=5)
        print(f"Status Code: {resp.status_code}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_root()
