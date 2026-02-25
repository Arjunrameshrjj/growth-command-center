import requests
import json

BASE_URL = "http://127.0.0.1:5000"

def test_endpoint(path, name):
    print(f"Testing {name} ({path})...")
    try:
        resp = requests.get(f"{BASE_URL}{path}", timeout=60)
        if resp.status_code == 200:
            print(f"[OK] {name} SUCCESS")
            data = resp.json()
            if "rows" in data:
                print(f"   Found {len(data['rows'])} rows")
            if "m1_val" in data:
                print(f"   M1 Value: {data['m1_val']}")
            return True
        else:
            print(f"[FAIL] {name} FAILED: {resp.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] {name} ERROR: {e}")
        return False

if __name__ == "__main__":
    test_endpoint("/api/discover?offset=0", "Discover API")
    test_endpoint("/api/content-calendar?offset=0", "Content Calendar API")
    test_endpoint("/api/renew?offset=0", "Renew API")
