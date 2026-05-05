import sqlite3
import requests
import os
import jwt
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import storage
from google_play_scraper import app as play_scraper_app
from flask import Flask, request, jsonify, render_template

# override=False ensures real server environment variables take priority over .env file
load_dotenv(override=False)

app = Flask(__name__)
DB_NAME = "database.db"

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS app_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_name TEXT NOT NULL,
                platform TEXT NOT NULL,
                downloads INTEGER NOT NULL,
                rating REAL NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS yearly_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_name TEXT NOT NULL,
                platform TEXT NOT NULL,
                year TEXT NOT NULL,
                downloads INTEGER NOT NULL DEFAULT 0,
                uninstalls INTEGER NOT NULL DEFAULT 0,
                subscriptions INTEGER NOT NULL DEFAULT 0,
                UNIQUE(app_name, platform, year)
            )
        ''')
        # Migrate: subscriptions column
        try:
            cursor.execute('ALTER TABLE yearly_metrics ADD COLUMN subscriptions INTEGER NOT NULL DEFAULT 0')
        except Exception:
            pass
        # Store Apple Analytics report request IDs (GET_COLLECTION is blocked, must use GET_INSTANCE)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics_request_ids (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id  TEXT    NOT NULL UNIQUE,
                app_id      TEXT    NOT NULL,
                access_type TEXT    NOT NULL,
                created     TEXT    NOT NULL
            )
        ''')
        # Table for manually-set iOS download overrides (when Analytics API is blocked)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ios_download_override (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                year    TEXT    NOT NULL UNIQUE,
                count   INTEGER NOT NULL DEFAULT 0,
                updated TEXT    NOT NULL
            )
        ''')
        conn.commit()

# Initialize the database on startup
init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM app_metrics')
        rows = cursor.fetchall()
        
        metrics = []
        for row in rows:
            metrics.append({
                'id': row['id'],
                'app_name': row['app_name'],
                'platform': row['platform'],
                'downloads': row['downloads'],
                'rating': row['rating']
            })
        return jsonify(metrics)

@app.route('/api/sync/germania', methods=['POST'])
def sync_germania():
    try:
        # 1. Fetch Google Play Data
        play_data = play_scraper_app('com.germania.mobile.app')
        android_rating = round(play_data.get('score', 0), 1)
        
        # approximate installs e.g., "10,000+" -> 10000
        installs_str = play_data.get('installs', '0')
        android_downloads = int(''.join(filter(str.isdigit, installs_str))) if any(c.isdigit() for c in installs_str) else 0

        # 2. Fetch Apple App Store Data
        ios_response = requests.get('https://itunes.apple.com/lookup?id=1535269629')
        ios_data = ios_response.json()
        ios_rating = 0.0
        if ios_data.get('resultCount', 0) > 0:
            ios_rating = round(ios_data['results'][0].get('averageUserRating', 0), 1)
        ios_downloads = 0  # Not publicly available

        # 3. Update Database
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            
            # Helper function to insert or replace based on app name & platform
            def upsert_metric(app_name, platform, downloads, rating):
                cursor.execute('SELECT id FROM app_metrics WHERE app_name = ? AND platform = ?', (app_name, platform))
                row = cursor.fetchone()
                if row:
                    cursor.execute('''
                        UPDATE app_metrics SET downloads = ?, rating = ? WHERE id = ?
                    ''', (downloads, rating, row[0]))
                else:
                    cursor.execute('''
                        INSERT INTO app_metrics (app_name, platform, downloads, rating)
                        VALUES (?, ?, ?, ?)
                    ''', (app_name, platform, downloads, rating))
                    
            upsert_metric("Germania Insurance", "Android", android_downloads, android_rating)
            upsert_metric("Germania Insurance", "iOS", ios_downloads, ios_rating)
            
            conn.commit()
            
        return jsonify({"message": "Successfully synced Germania Insurance data"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/metrics/yearly', methods=['GET'])
def get_yearly_metrics():
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM yearly_metrics ORDER BY year DESC')
        rows = cursor.fetchall()
        
        metrics = []
        for row in rows:
            metrics.append({
                'id': row['id'],
                'app_name': row['app_name'],
                'platform': row['platform'],
                'year': row['year'],
                'downloads': row['downloads'],
                'uninstalls': row['uninstalls'],
                'subscriptions': row['subscriptions'] if 'subscriptions' in row.keys() else 0
            })
        return jsonify(metrics)

@app.route('/api/ios-downloads/set', methods=['POST'])
def set_ios_downloads():
    """Manually set the iOS download count for a year (used when Analytics API is blocked)."""
    data  = request.get_json() or {}
    year  = str(data.get('year',  datetime.now().year))
    count = int(data.get('count', 0))
    if count < 0:
        return jsonify({'error': 'count must be >= 0'}), 400

    now_str = datetime.now().strftime('%B %d, %Y %I:%M %p')
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            'INSERT INTO ios_download_override (year, count, updated) VALUES (?, ?, ?)'
            ' ON CONFLICT(year) DO UPDATE SET count=excluded.count, updated=excluded.updated',
            (year, count, now_str)
        )
        # Also update the yearly_metrics table so charts reflect this
        conn.execute(
            'UPDATE yearly_metrics SET downloads = ? WHERE platform = "iOS" AND year = ?',
            (count, year)
        )
        conn.commit()

    print(f"iOS downloads override set: {count:,} for {year} (updated {now_str})")
    return jsonify({'year': year, 'count': count, 'updated': now_str})

@app.route('/api/ios-downloads/get', methods=['GET'])
def get_ios_downloads_override():
    """Returns manually-set iOS download overrides."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM ios_download_override ORDER BY year DESC').fetchall()
        return jsonify([dict(r) for r in rows])

@app.route('/api/app-versions', methods=['GET'])
def get_app_versions():
    """Returns current version, release date, and release notes for iOS and Android."""
    ANDROID_PKG  = 'com.germania.mobile.app'
    APPLE_APP_ID = '1535269629'
    result = {'ios': {}, 'android': {}}

    # ── iOS: public iTunes lookup API (no auth) ──────────────────────────────
    try:
        r = requests.get(
            f'https://itunes.apple.com/lookup?id={APPLE_APP_ID}&country=us',
            timeout=10
        )
        if r.status_code == 200:
            items = r.json().get('results', [])
            if items:
                d = items[0]
                raw_date = d.get('currentVersionReleaseDate', '')
                friendly = ''
                if raw_date:
                    try:
                        dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
                        friendly = dt.strftime('%B %d, %Y')
                    except Exception:
                        friendly = raw_date[:10]
                notes = d.get('releaseNotes', '').strip()
                result['ios'] = {
                    'version':      d.get('version', 'Unknown'),
                    'release_date': friendly,
                    'release_notes': notes[:600] + ('...' if len(notes) > 600 else ''),
                    'min_os':       d.get('minimumOsVersion', ''),
                    'size_mb':      round(int(d.get('fileSizeBytes', 0)) / 1_048_576, 1),
                    'rating':       round(d.get('averageUserRatingForCurrentVersion', 0), 1),
                    'store_url':    d.get('trackViewUrl', ''),
                }
    except Exception as e:
        result['ios']['error'] = str(e)

    # ── Android: google-play-scraper ─────────────────────────────────────────
    try:
        d = play_scraper_app(ANDROID_PKG, lang='en', country='us')
        raw_date = d.get('updated') or d.get('released') or 0
        friendly = ''
        if raw_date:
            try:
                friendly = datetime.fromtimestamp(raw_date).strftime('%B %d, %Y')
            except Exception:
                friendly = str(raw_date)
        notes = (d.get('recentChanges') or d.get('whatsNew') or '').strip()
        result['android'] = {
            'version':       d.get('version', 'Unknown'),
            'release_date':  friendly,
            'release_notes': notes[:600] + ('...' if len(notes) > 600 else ''),
            'min_android':   d.get('androidVersion', ''),
            'size':          d.get('size', ''),
            'rating':        round(d.get('score', 0), 1),
            'store_url':     f'https://play.google.com/store/apps/details?id={ANDROID_PKG}',
        }
    except Exception as e:
        result['android']['error'] = str(e)

    return jsonify(result)

@app.route('/api/debug/apple-analytics', methods=['GET'])
def debug_apple_analytics():
    """Shows exactly what Apple's Analytics API is returning — for troubleshooting."""
    import gzip, json as pyjson
    issuer_id    = os.environ.get('APPLE_ISSUER_ID')
    key_id       = os.environ.get('APPLE_KEY_ID')
    key_path     = os.environ.get('APPLE_PRIVATE_KEY_PATH')
    APPLE_APP_ID = '1535269629'

    if not all([issuer_id, key_id, key_path]) or not os.path.exists(key_path):
        return jsonify({"error": "Apple credentials not configured"}), 400

    with open(key_path, 'r') as f:
        private_key = f.read()

    def make_token():
        return jwt.encode(
            {"iss": issuer_id, "exp": int(time.time()) + 1200, "aud": "appstoreconnect-v1"},
            private_key, algorithm="ES256", headers={"kid": key_id}
        )

    result = {
        'key_id': key_id,
        'issuer_id': issuer_id,
        'tests': {}
    }
    headers_auth = {'Authorization': f'Bearer {make_token()}'}

    # Test 1: Can we reach the App Store Connect API at all?
    test1 = requests.get('https://api.appstoreconnect.apple.com/v1/apps',
                         headers=headers_auth)
    apps_found = []
    if test1.status_code == 200:
        for a in test1.json().get('data', []):
            apps_found.append({
                'id':     a['id'],
                'name':   a.get('attributes', {}).get('name'),
                'bundle': a.get('attributes', {}).get('bundleId'),
            })
    result['tests']['list_apps'] = {
        'status': test1.status_code,
        'apps':   apps_found,
        'error':  test1.text[:300] if test1.status_code != 200 else None
    }

    # Test 2: Analytics endpoint WITHOUT filter
    test2 = requests.get('https://api.appstoreconnect.apple.com/v1/analyticsReportRequests',
                         headers={'Authorization': f'Bearer {make_token()}'})
    result['tests']['analytics_no_filter'] = {
        'status': test2.status_code,
        'body':   test2.text[:500]
    }

    # Test 3: Analytics endpoint WITH app filter
    test3 = requests.get(
        f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests?filter[app]={APPLE_APP_ID}',
        headers={'Authorization': f'Bearer {make_token()}'}
    )
    result['tests']['analytics_with_filter'] = {
        'status': test3.status_code,
        'body':   test3.text[:500]
    }

    # Test 4: Try fetching the specific app by ID
    test4 = requests.get(
        f'https://api.appstoreconnect.apple.com/v1/apps/{APPLE_APP_ID}',
        headers={'Authorization': f'Bearer {make_token()}'}
    )
    result['tests']['get_app_by_id'] = {
        'status': test4.status_code,
        'body':   test4.text[:300]
    }

    return jsonify(result)

@app.route('/api/sync/private', methods=['POST'])
def sync_private_data():
    try:
        current_year = datetime.now().strftime('%Y')

        android_downloads = 0
        android_uninstalls = 0
        ios_downloads = 0
        ios_uninstalls = 0

        # --- 1. GOOGLE PLAY CONSOLE ---
        # Google Play Console exports MONTHLY CSVs (not yearly).
        # We loop through each month of the current year and sum them up.
        bucket_name = os.environ.get('GOOGLE_PLAY_BUCKET_ID')
        if bucket_name and os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            try:
                import io, csv
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                current_month_num = datetime.now().month

                for month_num in range(1, current_month_num + 1):
                    month_str = f"{current_year}{month_num:02d}"
                    blob_name = f"stats/installs/installs_com.germania.mobile.app_{month_str}_overview.csv"
                    blob = bucket.blob(blob_name)
                    try:
                        # Google Play Console exports CSVs in UTF-16 LE (not UTF-8)
                        content = blob.download_as_bytes()
                        content = content.decode('utf-16')
                        reader = csv.DictReader(io.StringIO(content))

                        month_dl = 0
                        month_ul = 0
                        rows_read = 0

                        for row in reader:
                            if rows_read == 0 and month_num == 1:
                                print(f"[DEBUG] GCS row1 — "
                                      f"Daily User Installs={row.get('Daily User Installs','?')} | "
                                      f"Daily Device Installs={row.get('Daily Device Installs','?')} | "
                                      f"Install events={row.get('Install events','?')}")

                            # 'Install events' = all install events including re-installs
                            # This most closely matches Play Console "User acquisition (All events)"
                            dl_val = row.get('Install events') or row.get('Daily User Installs') or '0'
                            ul_val = row.get('Daily User Uninstalls') or row.get('Daily Device Uninstalls') or '0'

                            try:
                                month_dl += int(str(dl_val).replace(',', '').strip() or 0)
                                month_ul += int(str(ul_val).replace(',', '').strip() or 0)
                            except ValueError:
                                pass
                            rows_read += 1

                        android_downloads  += month_dl
                        android_uninstalls += month_ul
                        print(f"Loaded GCS {month_str}: +{month_dl:,} downloads, +{month_ul:,} uninstalls ({rows_read} rows)")

                    except Exception as month_err:
                        print(f"No report for {month_str}: {month_err}")
                        continue

            except Exception as e:
                print(f"Error connecting to GCS: {e}")

        # --- 2. APPLE APP STORE CONNECT ---
        issuer_id    = os.environ.get('APPLE_ISSUER_ID')
        key_id       = os.environ.get('APPLE_KEY_ID')
        key_path     = os.environ.get('APPLE_PRIVATE_KEY_PATH')
        vendor_number = os.environ.get('APPLE_VENDOR_NUMBER')
        APPLE_APP_ID = '1535269629'

        if not all([issuer_id, key_id, key_path]):
            print("Apple credentials not fully configured — skipping Apple sync.")
        elif not os.path.exists(key_path):
            print(f"Apple .p8 key file not found at: {key_path}")
        else:
            try:
                import gzip, io as sysio, csv, json as pyjson

                with open(key_path, 'r') as f:
                    private_key = f.read()

                def make_token():
                    return jwt.encode(
                        {"iss": issuer_id, "exp": int(time.time()) + 1200, "aud": "appstoreconnect-v1"},
                        private_key, algorithm="ES256", headers={"kid": key_id}
                    )

                # ── Sales API: subscriptions (7F) AND downloads (type 1) ────────────────
                if vendor_number:
                    current_month_num = datetime.now().month
                    ios_subscriptions = 0   # 7F — insurance policy renewals
                    ios_dl_from_sales = 0   # type 1 — free app downloads

                    for month_num in range(1, current_month_num + 1):
                        month_str = f"{current_year}-{month_num:02d}"
                        url = (
                            f"https://api.appstoreconnect.apple.com/v1/salesReports"
                            f"?filter[frequency]=MONTHLY&filter[reportSubType]=SUMMARY"
                            f"&filter[reportType]=SALES&filter[vendorNumber]={vendor_number}"
                            f"&filter[reportDate]={month_str}"
                        )
                        resp = requests.get(url, headers={'Authorization': f'Bearer {make_token()}'})
                        if resp.status_code == 200:
                            try:
                                data = gzip.decompress(resp.content).decode('utf-8')
                            except Exception:
                                data = resp.content.decode('utf-8')
                            reader = csv.DictReader(sysio.StringIO(data), delimiter='\t')
                            month_subs = 0
                            month_dl   = 0
                            all_rows   = list(reader)

                            # DEBUG: dump all columns from first 3F row so we can find platform filter
                            if month_num == 1:
                                print(f"[DEBUG] Sales {month_str}: {len(all_rows)} rows")
                                for r in all_rows:
                                    if str(r.get('Apple Identifier','')).strip() == APPLE_APP_ID \
                                       and r.get('Product Type Identifier','').strip() == '3F':
                                        print("[DEBUG] All columns in first 3F row:")
                                        for col, val in r.items():
                                            print(f"  {col!r}: {val!r}")
                                        break  # only need one row to see column names

                            for row in all_rows:
                                units        = int(row.get('Units', 0) or 0)
                                product_type = row.get('Product Type Identifier', '').strip()
                                apple_id     = str(row.get('Apple Identifier', '')).strip()
                                if apple_id == APPLE_APP_ID:
                                    if product_type == '7F':
                                        ios_subscriptions += units
                                        month_subs        += units
                                    # NOTE: 3F from Sales Reports includes app UPDATES (not just downloads)
                                    # so it does NOT match App Store Connect Analytics "Total Downloads".
                                    # We leave ios_downloads=0 until Analytics API permission is granted.
                            print(f"Apple Sales {month_str}: {month_subs:,} renewals")
                        else:
                            print(f"Apple Sales {month_str}: HTTP {resp.status_code} — {resp.text[:150]}")

                    print(f"Apple Sales YTD: {ios_subscriptions:,} renewals")
                    ios_downloads  = 0               # will be set by Analytics API below
                    ios_uninstalls = ios_subscriptions   # passed to upsert as subscriptions field

                # ── Analytics API: correct usage (POST + GET_INSTANCE) ───────────
                # GET_COLLECTION is blocked; we must POST to create, then GET by ID
                import json as pyjson

                # Step 1: get the internal app ID from the apps list
                internal_app_id = APPLE_APP_ID  # default to iTunes ID
                apps_resp = requests.get(
                    'https://api.appstoreconnect.apple.com/v1/apps'
                    '?filter[bundleId]=com.germania.mobile.app',
                    headers={'Authorization': f'Bearer {make_token()}'}
                )
                if apps_resp.status_code == 200:
                    app_data = apps_resp.json().get('data', [])
                    if app_data:
                        internal_app_id = app_data[0]['id']
                        print(f"Internal App ID confirmed: {internal_app_id}")

                # Step 2: load any previously stored request IDs from the DB
                stored_ids = []
                with sqlite3.connect(DB_NAME) as _c:
                    rows = _c.execute('SELECT request_id, access_type FROM analytics_request_ids').fetchall()
                    stored_ids = [(r[0], r[1]) for r in rows]
                print(f"Stored analytics request IDs: {stored_ids}")

                # Step 3: try each stored ID using GET_INSTANCE (allowed!)
                found_downloads = False
                for req_id, access_type in stored_ids:
                    rep_resp = requests.get(
                        f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{req_id}/reports',
                        headers={'Authorization': f'Bearer {make_token()}'}
                    )
                    print(f"  Request {req_id} ({access_type}): reports status={rep_resp.status_code}")
                    if rep_resp.status_code != 200:
                        continue
                    for report in rep_resp.json().get('data', []):
                        category = report.get('attributes', {}).get('category', '')
                        state    = report.get('attributes', {}).get('processingState', '')
                        if state != 'READY':
                            print(f"    Report {report['id']} ({category}): {state} — not ready yet")
                            continue
                        seg_resp = requests.get(
                            f'https://api.appstoreconnect.apple.com/v1/analyticsReports/{report["id"]}/segments',
                            headers={'Authorization': f'Bearer {make_token()}'}
                        )
                        if seg_resp.status_code != 200:
                            continue
                        for seg in seg_resp.json().get('data', []):
                            dl_url = seg.get('attributes', {}).get('url', '')
                            if not dl_url:
                                continue
                            sr = requests.get(dl_url, headers={'Authorization': f'Bearer {make_token()}'})
                            if sr.status_code != 200:
                                continue
                            try:
                                raw = gzip.decompress(sr.content).decode('utf-8')
                            except Exception:
                                raw = sr.content.decode('utf-8')
                            reader = csv.DictReader(sysio.StringIO(raw), delimiter='\t')
                            for row in reader:
                                row_date = row.get('Date', '') or row.get('date', '')
                                if not row_date.startswith(current_year):
                                    continue
                                dl = (row.get('Downloads') or row.get('Total Downloads')
                                      or row.get('First Time Downloads') or '0')
                                try:
                                    ios_downloads += int(str(dl).replace(',', '') or 0)
                                    found_downloads = True
                                except ValueError:
                                    pass

                if found_downloads:
                    print(f"Analytics API: {ios_downloads:,} iOS downloads for {current_year}")

                # Step 4: if no stored IDs, try app relationship endpoint to discover existing ones
                if not stored_ids:
                    print("Trying app relationship endpoint to discover existing request IDs...")
                    rel_resp = requests.get(
                        f'https://api.appstoreconnect.apple.com/v1/apps/{internal_app_id}/analyticsReportRequests',
                        headers={'Authorization': f'Bearer {make_token()}'}
                    )
                    print(f"  App relationship status: {rel_resp.status_code}")
                    if rel_resp.status_code == 200:
                        for req in rel_resp.json().get('data', []):
                            rid = req['id']
                            atype = req.get('attributes', {}).get('accessType', 'UNKNOWN')
                            print(f"  Discovered request ID: {rid} ({atype})")
                            with sqlite3.connect(DB_NAME) as _c:
                                _c.execute(
                                    'INSERT OR IGNORE INTO analytics_request_ids '
                                    '(request_id, app_id, access_type, created) VALUES (?,?,?,?)',
                                    (rid, internal_app_id, atype,
                                     datetime.now().strftime('%Y-%m-%d %H:%M'))
                                )
                            stored_ids.append((rid, atype))
                    else:
                        print(f"  Body: {rel_resp.text[:300]}")

                # Step 5: create a new ONGOING request if we still have no IDs
                if not stored_ids:
                    print("No existing requests found — creating new ONGOING request via POST...")
                    cr = requests.post(
                        'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests',
                        headers={'Authorization': f'Bearer {make_token()}',
                                 'Content-Type': 'application/json'},
                        data=pyjson.dumps({
                            "data": {
                                "type": "analyticsReportRequests",
                                "attributes": {"accessType": "ONGOING"},
                                "relationships": {
                                    "app": {"data": {"type": "apps", "id": internal_app_id}}
                                }
                            }
                        })
                    )
                    print(f"  POST status: {cr.status_code} — {cr.text[:400]}")
                    if cr.status_code == 201:
                        new_id = cr.json()['data']['id']
                        print(f"  ONGOING request created: {new_id} — reports ready in ~24 hours")
                        with sqlite3.connect(DB_NAME) as _c:
                            _c.execute(
                                'INSERT OR IGNORE INTO analytics_request_ids '
                                '(request_id, app_id, access_type, created) VALUES (?,?,?,?)',
                                (new_id, internal_app_id, 'ONGOING',
                                 datetime.now().strftime('%Y-%m-%d %H:%M'))
                            )
                    elif cr.status_code == 409:
                        # 409 = already exists; try to extract the ID from the error body
                        body = cr.json()
                        existing_id = None
                        for err in body.get('errors', []):
                            src = err.get('source', {})
                            existing_id = (src.get('parameter') or
                                           err.get('meta', {}).get('existingId'))
                        print(f"  409 body: {pyjson.dumps(body)[:500]}")
                        if existing_id:
                            print(f"  Recovered existing ID from 409 body: {existing_id}")
                            with sqlite3.connect(DB_NAME) as _c:
                                _c.execute(
                                    'INSERT OR IGNORE INTO analytics_request_ids '
                                    '(request_id, app_id, access_type, created) VALUES (?,?,?,?)',
                                    (existing_id, internal_app_id, 'ONGOING',
                                     datetime.now().strftime('%Y-%m-%d %H:%M'))
                                )
                        else:
                            print("  Could not extract existing ID from 409 — sync again tomorrow")
                    else:
                        print(f"POST analytics request failed: {cr.status_code} — {cr.text[:300]}")


            except Exception as e:
                print(f"Error fetching Apple data: {e}")

        # --- 3. SAVE TO yearly_metrics TABLE ---
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()

            def upsert_yearly(app_name, platform, year, downloads, uninstalls, subscriptions=0):
                cursor.execute(
                    'SELECT id FROM yearly_metrics WHERE app_name = ? AND platform = ? AND year = ?',
                    (app_name, platform, year)
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        'UPDATE yearly_metrics SET downloads = ?, uninstalls = ?, subscriptions = ? WHERE id = ?',
                        (downloads, uninstalls, subscriptions, row[0])
                    )
                else:
                    cursor.execute(
                        'INSERT INTO yearly_metrics (app_name, platform, year, downloads, uninstalls, subscriptions) VALUES (?, ?, ?, ?, ?, ?)',
                        (app_name, platform, year, downloads, uninstalls, subscriptions)
                    )

            upsert_yearly("Germania Insurance", "Android", current_year, android_downloads, android_uninstalls)
            # ios_uninstalls was repurposed to hold subscription count from Sales API
            upsert_yearly("Germania Insurance", "iOS", current_year, ios_downloads, 0, ios_uninstalls)

            # Aggregate total iOS downloads across all years and update the Current Metrics card
            cursor.execute(
                'SELECT SUM(downloads) FROM yearly_metrics WHERE platform = "iOS" AND app_name = "Germania Insurance"'
            )
            row = cursor.fetchone()
            total_ios = row[0] if row and row[0] is not None else 0
            cursor.execute(
                'UPDATE app_metrics SET downloads = ? WHERE platform = "iOS" AND app_name = "Germania Insurance"',
                (total_ios,)
            )

            conn.commit()

        return jsonify({"message": "Private yearly data synced successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/sync/historical', methods=['POST'])
def sync_historical_data():
    """Pull data for 2020–2025 from Google Play GCS and Apple Sales API."""
    try:
        import io, csv, gzip, json as pyjson
        import io as sysio

        bucket_name   = os.environ.get('GOOGLE_PLAY_BUCKET_ID')
        issuer_id     = os.environ.get('APPLE_ISSUER_ID')
        key_id        = os.environ.get('APPLE_KEY_ID')
        key_path      = os.environ.get('APPLE_PRIVATE_KEY_PATH')
        vendor_number = os.environ.get('APPLE_VENDOR_NUMBER')
        APPLE_APP_ID  = '1535269629'
        APP_START_YEAR = 2020
        current_year   = datetime.now().year

        results = {}

        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()

            def upsert_hist(platform, year, downloads, uninstalls, subscriptions=0):
                cursor.execute(
                    'SELECT id FROM yearly_metrics WHERE app_name = ? AND platform = ? AND year = ?',
                    ("Germania Insurance", platform, str(year))
                )
                row = cursor.fetchone()
                if row:
                    # Only update fields that have actual data (don't overwrite with zeros)
                    if downloads > 0 or uninstalls > 0 or subscriptions > 0:
                        cursor.execute(
                            'UPDATE yearly_metrics SET downloads = ?, uninstalls = ?, subscriptions = ? WHERE id = ?',
                            (downloads, uninstalls, subscriptions, row[0])
                        )
                else:
                    cursor.execute(
                        'INSERT INTO yearly_metrics (app_name, platform, year, downloads, uninstalls, subscriptions) VALUES (?, ?, ?, ?, ?, ?)',
                        ("Germania Insurance", platform, str(year), downloads, uninstalls, subscriptions)
                    )

            # ── Android: loop past years via GCS ──────────────────────────
            if bucket_name and os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                try:
                    storage_client = storage.Client()
                    bucket = storage_client.bucket(bucket_name)

                    for year in range(APP_START_YEAR, current_year):
                        year_dl = 0
                        year_ul = 0
                        months_found = 0

                        for month_num in range(1, 13):
                            month_str = f"{year}{month_num:02d}"
                            blob = bucket.blob(f"stats/installs/installs_com.germania.mobile.app_{month_str}_overview.csv")
                            try:
                                content = blob.download_as_bytes().decode('utf-16')
                                reader = csv.DictReader(io.StringIO(content))
                                for row in reader:
                                    dl_val = row.get('Install events') or row.get('Daily User Installs') or '0'
                                    ul_val = row.get('Daily User Uninstalls') or row.get('Daily Device Uninstalls') or '0'
                                    try:
                                        year_dl += int(str(dl_val).replace(',', '').strip() or 0)
                                        year_ul += int(str(ul_val).replace(',', '').strip() or 0)
                                    except ValueError:
                                        pass
                                months_found += 1
                            except Exception:
                                pass  # Month file not available

                        if months_found > 0:
                            upsert_hist("Android", year, year_dl, year_ul)
                            print(f"Android {year}: {year_dl:,} downloads, {year_ul:,} uninstalls ({months_found} months)")
                            results[f"android_{year}"] = year_dl
                        else:
                            print(f"Android {year}: no GCS data found")

                except Exception as e:
                    print(f"GCS historical error: {e}")

            # ── iOS: loop past years via Apple Sales API ───────────────────
            if all([issuer_id, key_id, key_path, vendor_number]) and os.path.exists(key_path):
                try:
                    with open(key_path, 'r') as f:
                        private_key = f.read()

                    def make_token():
                        return jwt.encode(
                            {"iss": issuer_id, "exp": int(time.time()) + 1200, "aud": "appstoreconnect-v1"},
                            private_key, algorithm="ES256", headers={"kid": key_id}
                        )

                    for year in range(APP_START_YEAR, current_year):
                        year_subs = 0
                        months_found = 0

                        for month_num in range(1, 13):
                            month_str = f"{year}-{month_num:02d}"
                            url = (
                                f"https://api.appstoreconnect.apple.com/v1/salesReports"
                                f"?filter[frequency]=MONTHLY&filter[reportSubType]=SUMMARY"
                                f"&filter[reportType]=SALES&filter[vendorNumber]={vendor_number}"
                                f"&filter[reportDate]={month_str}"
                            )
                            resp = requests.get(url, headers={'Authorization': f'Bearer {make_token()}'})
                            if resp.status_code == 200:
                                try:
                                    data = gzip.decompress(resp.content).decode('utf-8')
                                except Exception:
                                    data = resp.content.decode('utf-8')
                                reader = csv.DictReader(sysio.StringIO(data), delimiter='\t')
                                for row in reader:
                                    units        = int(row.get('Units', 0) or 0)
                                    product_type = row.get('Product Type Identifier', '').strip()
                                    apple_id     = str(row.get('Apple Identifier', '')).strip()
                                    if apple_id == APPLE_APP_ID and product_type == '7F':
                                        year_subs += units
                                months_found += 1

                        if months_found > 0:
                            upsert_hist("iOS", year, 0, 0, year_subs)
                            print(f"iOS {year}: {year_subs:,} subscription renewals ({months_found} months)")
                            results[f"ios_{year}_subs"] = year_subs
                        else:
                            print(f"iOS {year}: no Apple Sales data found")

                except Exception as e:
                    print(f"Apple historical error: {e}")

            conn.commit()

        return jsonify({"message": "Historical data synced (2020–2025)", "results": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print(f"\n{'='*55}")
    print(f"  Germania Mobile App Dashboard")
    print(f"  Local:   http://127.0.0.1:5000")
    print(f"  Network: http://{local_ip}:5000  ← share this with your team")
    print(f"{'='*55}\n")
    app.run(debug=False, host='0.0.0.0', port=5000)
