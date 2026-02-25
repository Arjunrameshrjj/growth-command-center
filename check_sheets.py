import requests
import json

NEW_URL = "https://script.google.com/macros/s/AKfycbzLNRI2lZ2o11BD6jtOh_QY8054APeQnIMj7stFJ0DWCLw0g3TIlt0nWn4V6B01hQfjjg/exec"

def check_sheets():
    print(f"Fetching data from NEW URL...")
    try:
        resp = requests.get(NEW_URL, timeout=30)
        data = resp.json()
        print(f"Total rows fetched: {len(data)}")
        
        sheets = set()
        for row in data:
            sheet_name = row.get("Sheet")
            if sheet_name:
                sheets.add(sheet_name)
        
        print("\nSheets found in data:")
        for s in sorted(list(sheets)):
            player_rows = [r for r in data if r.get("Sheet") == s]
            print(f"- {s} ({len(player_rows)} rows)")
            
        if "FLUENCY" in sheets:
            print("\nSUCCESS: 'FLUENCY' sheet found!")
        else:
            print("\nFAILURE: 'FLUENCY' sheet NOT found in data.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_sheets()
