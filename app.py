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

load_dotenv()

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
                downloads INTEGER NOT NULL,
                uninstalls INTEGER NOT NULL,
                UNIQUE(app_name, platform, year)
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
                'uninstalls': row['uninstalls']
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
        # Google Play Console exports yearly install/uninstall reports to a GCS bucket.
        # The report filename format is: installs_com.germania.mobile.app_YYYY_overview.csv
        bucket_name = os.environ.get('GOOGLE_PLAY_BUCKET_ID')
        if bucket_name and os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            try:
                import io, csv
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                # Download the yearly overview CSV for the current year
                blob_name = f"stats/installs/installs_com.germania.mobile.app_{current_year}_overview.csv"
                blob = bucket.blob(blob_name)
                content = blob.download_as_text()
                reader = csv.DictReader(io.StringIO(content))
                for row in reader:
                    android_downloads += int(row.get('Daily Device Installs', 0) or 0)
                    android_uninstalls += int(row.get('Daily Device Uninstalls', 0) or 0)
            except Exception as e:
                print(f"Error fetching GCS yearly report: {e}")

        # --- 2. APPLE APP STORE CONNECT ---
        # Apple Sales API provides YEARLY reports via filter[frequency]=YEARLY
        issuer_id = os.environ.get('APPLE_ISSUER_ID')
        key_id = os.environ.get('APPLE_KEY_ID')
        key_path = os.environ.get('APPLE_PRIVATE_KEY_PATH')
        vendor_number = os.environ.get('APPLE_VENDOR_NUMBER')

        if issuer_id and key_id and key_path and vendor_number and os.path.exists(key_path):
            try:
                import gzip, io as sysio
                with open(key_path, 'r') as f:
                    private_key = f.read()

                token = jwt.encode(
                    {
                        "iss": issuer_id,
                        "exp": int(time.time()) + 1200,
                        "aud": "appstoreconnect-v1"
                    },
                    private_key,
                    algorithm="ES256",
                    headers={"kid": key_id}
                )
                # Request yearly SALES report from Apple
                url = (
                    f"https://api.appstoreconnect.apple.com/v1/salesReports"
                    f"?filter[frequency]=YEARLY"
                    f"&filter[reportSubType]=SUMMARY"
                    f"&filter[reportType]=SALES"
                    f"&filter[vendorNumber]={vendor_number}"
                    f"&filter[reportDate]={current_year}"
                )
                headers = {'Authorization': f'Bearer {token}'}
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    # Apple returns gzipped TSV
                    data = gzip.decompress(response.content).decode('utf-8')
                    import csv
                    reader = csv.DictReader(sysio.StringIO(data), delimiter='\t')
                    for row in reader:
                        units = int(row.get('Units', 0) or 0)
                        product_type = row.get('Product Type Identifier', '')
                        if product_type in ('1', '1F', '1T'):  # Free/paid app downloads
                            ios_downloads += units
                else:
                    print(f"Apple API error {response.status_code}: {response.text}")
            except Exception as e:
                print(f"Error fetching App Store Connect yearly report: {e}")

        # --- 3. SAVE TO yearly_metrics TABLE ---
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()

            def upsert_yearly(app_name, platform, year, downloads, uninstalls):
                cursor.execute(
                    'SELECT id FROM yearly_metrics WHERE app_name = ? AND platform = ? AND year = ?',
                    (app_name, platform, year)
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        'UPDATE yearly_metrics SET downloads = ?, uninstalls = ? WHERE id = ?',
                        (downloads, uninstalls, row[0])
                    )
                else:
                    cursor.execute(
                        'INSERT INTO yearly_metrics (app_name, platform, year, downloads, uninstalls) VALUES (?, ?, ?, ?, ?)',
                        (app_name, platform, year, downloads, uninstalls)
                    )

            upsert_yearly("Germania Insurance", "Android", current_year, android_downloads, android_uninstalls)
            upsert_yearly("Germania Insurance", "iOS", current_year, ios_downloads, ios_uninstalls)

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
