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
                            # Print column names on first month to help debug
                            if rows_read == 0 and month_num == 1:
                                print(f"[DEBUG] GCS CSV columns: {list(row.keys())}")

                            # Google Play uses 'Daily Device Installs' — handle common variants
                            dl_val = (
                                row.get('Daily Device Installs') or
                                row.get('daily_device_installs') or
                                row.get('Device Installs') or
                                '0'
                            )
                            ul_val = (
                                row.get('Daily Device Uninstalls') or
                                row.get('daily_device_uninstalls') or
                                row.get('Device Uninstalls') or
                                '0'
                            )

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

                # ── Part A: Sales API (available NOW) ──────────────────────────────
                # Gets subscription renewals (7F), app updates (1F), and in-app purchases
                # NOTE: Free app downloads (type '1') do NOT appear in Sales Reports
                if vendor_number:
                    current_month_num = datetime.now().month
                    ios_subscriptions = 0   # 7F — insurance policy renewals

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
                            for row in reader:
                                units        = int(row.get('Units', 0) or 0)
                                product_type = row.get('Product Type Identifier', '').strip()
                                apple_id     = str(row.get('Apple Identifier', '')).strip()
                                if apple_id == APPLE_APP_ID and product_type == '7F':
                                    ios_subscriptions += units
                                    month_subs += units
                            print(f"Apple Sales {month_str}: {month_subs:,} subscription renewals")
                    print(f"Apple Sales total: {ios_subscriptions:,} subscription renewals for {current_year}")
                    # Store subscription count as a proxy until Analytics downloads arrive
                    ios_uninstalls = ios_subscriptions  # repurposed field for subscriptions

                # ── Part B: Analytics API (downloads — ready after 24h) ────────────
                headers_auth = {'Authorization': f'Bearer {make_token()}'}
                request_id = None
                list_resp = requests.get(
                    f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests?filter[app]={APPLE_APP_ID}',
                    headers=headers_auth
                )
                if list_resp.status_code == 200:
                    for req in list_resp.json().get('data', []):
                        if req.get('attributes', {}).get('accessType') == 'ONGOING':
                            request_id = req['id']
                            print(f"Found analytics request: {request_id}")
                            break

                if not request_id:
                    cr = requests.post(
                        'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests',
                        headers={**headers_auth, 'Content-Type': 'application/json'},
                        data=pyjson.dumps({
                            "data": {
                                "type": "analyticsReportRequests",
                                "attributes": {"accessType": "ONGOING"},
                                "relationships": {"app": {"data": {"type": "apps", "id": APPLE_APP_ID}}}
                            }
                        })
                    )
                    if cr.status_code == 201:
                        request_id = cr.json()['data']['id']
                        print(f"Created analytics request {request_id} — downloads available tomorrow.")

                if request_id:
                    headers_auth = {'Authorization': f'Bearer {make_token()}'}
                    rep_resp = requests.get(
                        f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{request_id}/reports',
                        headers=headers_auth
                    )
                    if rep_resp.status_code == 200:
                        for report in rep_resp.json().get('data', []):
                            if report.get('attributes', {}).get('category') != 'APP_USAGE':
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
                                    dl    = row.get('Downloads') or row.get('Total Downloads') or row.get('First Time Downloads') or '0'
                                    re_dl = row.get('Redownloads') or row.get('Re-Downloads') or '0'
                                    try:
                                        ios_downloads += int(str(dl).replace(',', '') or 0)
                                        ios_downloads += int(str(re_dl).replace(',', '') or 0)
                                    except ValueError:
                                        pass
                        if ios_downloads > 0:
                            print(f"Apple Analytics: {ios_downloads:,} total downloads for {current_year}")
                        else:
                            print("Apple Analytics: download report not ready yet — will appear after next sync.")

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
