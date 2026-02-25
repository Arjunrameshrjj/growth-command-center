import requests
import json

try:
    response = requests.get("http://127.0.0.1:5000/api/renew?offset=0")
    if response.status_code == 200:
        data = response.json()
        print("API Status: OK")
        print(f"Renewal Count: {data.get('m1_count')}")
        if data.get('recent_renewals'):
            print("Recent Renewals Column Keys:")
            print(list(data['recent_renewals'][0].keys()))
            print("\nExample Record:")
            print(json.dumps(data['recent_renewals'][0], indent=2))
        else:
            print("No recent renewals found in response.")
    else:
        print(f"API Error: {response.status_code}")
except Exception as e:
    print(f"Error connecting to API: {e}")
