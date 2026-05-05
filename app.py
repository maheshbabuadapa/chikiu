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
        # Migrate existing DBs that don't have the subscriptions column yet
        try:
            cursor.execute('ALTER TABLE yearly_metrics ADD COLUMN subscriptions INTEGER NOT NULL DEFAULT 0')
        except Exception:
            pass  # Column already exists — that's fine
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

    result = {}
    headers_auth = {'Authorization': f'Bearer {make_token()}'}

    # List all existing report requests for this app
    list_resp = requests.get(
        f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests?filter[app]={APPLE_APP_ID}',
        headers=headers_auth
    )
    result['list_status'] = list_resp.status_code
    result['requests'] = []

    if list_resp.status_code == 200:
        req_data = list_resp.json().get('data', [])
        for req in req_data:
            req_info = {
                'id': req['id'],
                'accessType': req.get('attributes', {}).get('accessType'),
                'reports': []
            }
            # Get reports for this request
            rep_resp = requests.get(
                f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{req["id"]}/reports',
                headers={'Authorization': f'Bearer {make_token()}'}
            )
            req_info['reports_status'] = rep_resp.status_code
            if rep_resp.status_code == 200:
                for report in rep_resp.json().get('data', []):
                    attrs = report.get('attributes', {})
                    req_info['reports'].append({
                        'id': report['id'],
                        'category': attrs.get('category'),
                        'name': attrs.get('name'),
                        'state': attrs.get('processingState'),
                    })
            result['requests'].append(req_info)

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

                            # DEBUG: on first month only, dump all rows so we can see what Apple sends
                            if month_num == 1:
                                print(f"[DEBUG] Sales report {month_str}: {len(all_rows)} rows total")
                                seen_types = {}
                                for r in all_rows:
                                    pt  = r.get('Product Type Identifier', '?').strip()
                                    aid = str(r.get('Apple Identifier', '?')).strip()
                                    key = (aid, pt)
                                    if key not in seen_types:
                                        seen_types[key] = int(r.get('Units', 0) or 0)
                                    else:
                                        seen_types[key] += int(r.get('Units', 0) or 0)
                                for (aid, pt), units in sorted(seen_types.items()):
                                    marker = " ← OUR APP" if aid == APPLE_APP_ID else ""
                                    print(f"  Apple ID={aid}  Type={pt}  Units={units}{marker}")

                            for row in all_rows:
                                units        = int(row.get('Units', 0) or 0)
                                product_type = row.get('Product Type Identifier', '').strip()
                                apple_id     = str(row.get('Apple Identifier', '')).strip()
                                if apple_id == APPLE_APP_ID:
                                    if product_type == '7F':
                                        ios_subscriptions += units
                                        month_subs        += units
                                    elif product_type in ('3F', '1F', '1', 'F1'):
                                        # 3F = free app download (confirmed from debug output)
                                        ios_dl_from_sales += units
                                        month_dl          += units
                            print(f"Apple Sales {month_str}: {month_dl:,} downloads  |  {month_subs:,} renewals")
                        else:
                            print(f"Apple Sales {month_str}: HTTP {resp.status_code} — {resp.text[:150]}")

                    print(f"Apple Sales YTD: {ios_dl_from_sales:,} downloads, {ios_subscriptions:,} renewals")
                    ios_downloads  = ios_dl_from_sales
                    ios_uninstalls = ios_subscriptions   # passed to upsert as subscriptions field


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
