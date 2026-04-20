"""Quick smoke test of critical API endpoints."""
import urllib.request, urllib.error, urllib.parse, json, sys

BASE = "http://localhost:8000"

def test():
    errors = []
    
    # 1. Health
    try:
        resp = urllib.request.urlopen(f"{BASE}/health")
        body = json.loads(resp.read())
        print(f"1. Health: {body['status']} / DB: {body['db']}")
        assert body["status"] == "ok"
    except Exception as e:
        errors.append(f"Health: {e}")
        print(f"1. Health: FAIL - {e}")
    
    # 2. Login
    token = None
    try:
        data = urllib.parse.urlencode({"username": "prog", "password": "Prog@9089"}).encode()
        req = urllib.request.Request(f"{BASE}/auth/login", data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp = urllib.request.urlopen(req)
        body = json.loads(resp.read())
        token = body.get("access_token", "")
        print(f"2. Login: OK (token {len(token)} chars)")
    except urllib.error.HTTPError as e:
        errors.append(f"Login: {e.code}")
        print(f"2. Login: FAIL - {e.code} {e.read().decode()[:200]}")
    except Exception as e:
        errors.append(f"Login: {e}")
        print(f"2. Login: FAIL - {e}")
    
    if not token:
        print("Cannot continue without auth token")
        sys.exit(1)
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # 3. Statistics — GET /statistics
    try:
        req = urllib.request.Request(f"{BASE}/statistics", headers=headers)
        resp = urllib.request.urlopen(req)
        stats = json.loads(resp.read())
        entries = stats.get("entries", [])
        total_valid = stats.get("total_valid", 0)
        print(f"3. Statistics: {len(entries)} entries, total_valid={total_valid:,}")
        assert len(entries) > 0, "No stats entries"
    except Exception as e:
        errors.append(f"Statistics: {e}")
        print(f"3. Statistics: FAIL - {e}")
    
    # 4. Geo hierarchy — GET /geo/info
    try:
        req = urllib.request.Request(f"{BASE}/geo/info", headers=headers)
        resp = urllib.request.urlopen(req)
        geo = json.loads(resp.read())
        divs = len(geo) if isinstance(geo, list) else len(geo.get("divisions", []))
        print(f"4. Geo info: {divs} items")
    except Exception as e:
        errors.append(f"Geo: {e}")
        print(f"4. Geo: FAIL - {e}")
    
    # 5. Search — GET /search?q=123
    try:
        req = urllib.request.Request(f"{BASE}/search?q=1234567890&limit=3", headers=headers)
        resp = urllib.request.urlopen(req)
        results = json.loads(resp.read())
        print(f"5. Search: OK ({resp.status}, {len(results)} results)")
    except urllib.error.HTTPError as e:
        if e.code == 422:
            print(f"5. Search: OK (422 validation)")
        else:
            errors.append(f"Search: {e.code}")
            print(f"5. Search: FAIL - {e.code}")
    except Exception as e:
        errors.append(f"Search: {e}")
        print(f"5. Search: FAIL - {e}")

    # 6. Refresh all stats — POST /statistics/refresh-all
    try:
        req = urllib.request.Request(f"{BASE}/statistics/refresh-all", data=b"", method="POST", headers=headers)
        resp = urllib.request.urlopen(req)
        body = json.loads(resp.read())
        print(f"6. Refresh stats: updated={body.get('updated',0)}, ghosts={body.get('ghost_entries_zeroed',0)}")
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode()[:200]
        if e.code == 403:
            print(f"6. Refresh stats: 403 (need manage_geo permission — expected for non-admin)")
        else:
            errors.append(f"Refresh: {e.code}")
            print(f"6. Refresh stats: FAIL - {e.code} {resp_body}")
    except Exception as e:
        errors.append(f"Refresh: {e}")
        print(f"6. Refresh stats: FAIL - {e}")

    print()
    if errors:
        print(f"FAILED: {len(errors)} endpoints failed")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("ALL SMOKE TESTS PASSED")

if __name__ == "__main__":
    test()
