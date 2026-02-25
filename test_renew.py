import requests
RENEW_SHEET_URL = "https://script.google.com/macros/s/AKfycbyid7u5OIJbemqEyawmvRsJyF6XmplsjNw-u9DqDuI7dm59hxSuykJOk2Yeeyc5riDtfg/exec"

print(f"--- Testing Renew URL ---")
try:
    resp = requests.get(RENEW_SHEET_URL, timeout=30)
    print(f"Status Code: {resp.status_code}")
    print(f"Content Type: {resp.headers.get('Content-Type')}")
    try:
        data = resp.json()
        print(f"JSON Success. Record count: {len(data)}")
    except:
        print("JSON Parse Failed.")
        print(f"Preview: {resp.text[:500]}")
except Exception as e:
    print(f"Error: {e}")
