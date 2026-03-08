import requests
import zipfile
import io
import os

def test_zip_download():
    # Use the internal service name if running in docker, or localhost if testing locally
    # Since I'm an agent on the host, I'll try localhost:8000 (backend)
    url = "http://localhost:8000/downloads/valid-zip"
    
    print(f"Testing zip download from {url}...")
    try:
        response = requests.get(url)
        if response.status_code == 404:
            print("No valid files found (expected if DB is empty).")
            return
            
        response.raise_for_status()
        
        # Verify it's a zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            print("Zip file downloaded successfully!")
            print("Contents:")
            for name in z.namelist():
                print(f" - {name}")
                if not name.endswith("_valid.xlsx"):
                    print(f"FAILED: Filename {name} does not match convention.")
                else:
                    print(f"PASSED: Filename {name} matches convention.")
                    
    except Exception as e:
        print(f"Error testing zip download: {e}")

if __name__ == "__main__":
    test_zip_download()
