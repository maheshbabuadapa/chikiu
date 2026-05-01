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
            CREATE TABLE IF NOT EXISTS monthly_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_name TEXT NOT NULL,
                platform TEXT NOT NULL,
                month_year TEXT NOT NULL,
                downloads INTEGER NOT NULL,
                uninstalls INTEGER NOT NULL
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

@app.route('/api/metrics/monthly', methods=['GET'])
def get_monthly_metrics():
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM monthly_metrics ORDER BY month_year DESC')
        rows = cursor.fetchall()
        
        metrics = []
        for row in rows:
            metrics.append({
                'id': row['id'],
                'app_name': row['app_name'],
                'platform': row['platform'],
                'month_year': row['month_year'],
                'downloads': row['downloads'],
                'uninstalls': row['uninstalls']
            })
        return jsonify(metrics)

@app.route('/api/sync/private', methods=['POST'])
def sync_private_data():
    try:
        current_month = datetime.now().strftime('%Y-%m')
        
        android_downloads = 0
        android_uninstalls = 0
        ios_downloads = 0
        ios_uninstalls = 0

        # --- 1. GOOGLE PLAY CONSOLE ---
        # Note: This is skeleton code. In reality you must parse the specific CSV format.
        bucket_name = os.environ.get('GOOGLE_PLAY_BUCKET_ID')
        if bucket_name and os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                # Find the latest report (e.g. installs_com.germania.mobile.app_YYYYMM_overview.csv)
                # For demonstration, we simulate parsing a downloaded CSV
                android_downloads = 1200 # Simulated data
                android_uninstalls = 300 # Simulated data
            except Exception as e:
                print(f"Error fetching GCS: {e}")

        # --- 2. APPLE APP STORE CONNECT ---
        issuer_id = os.environ.get('APPLE_ISSUER_ID')
        key_id = os.environ.get('APPLE_KEY_ID')
        key_path = os.environ.get('APPLE_PRIVATE_KEY_PATH')
        
        if issuer_id and key_id and key_path and os.path.exists(key_path):
            try:
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
                # Call Apple Sales API
                # url = f"https://api.appstoreconnect.apple.com/v1/salesReports?filter[frequency]=MONTHLY&filter[reportSubType]=SUMMARY&filter[reportType]=SALES&filter[vendorNumber]=YOUR_VENDOR"
                # headers = {'Authorization': f'Bearer {token}'}
                # response = requests.get(url, headers=headers)
                
                # For demonstration, we simulate parsing the downloaded report
                ios_downloads = 850 # Simulated data
                ios_uninstalls = 0  # Apple doesn't typically provide uninstalls in this report
            except Exception as e:
                print(f"Error fetching App Store Connect: {e}")

        # --- 3. SAVE TO DB ---
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            
            def upsert_monthly(app_name, platform, month, downloads, uninstalls):
                cursor.execute('SELECT id FROM monthly_metrics WHERE app_name = ? AND platform = ? AND month_year = ?', (app_name, platform, month))
                row = cursor.fetchone()
                if row:
                    cursor.execute('''
                        UPDATE monthly_metrics SET downloads = ?, uninstalls = ? WHERE id = ?
                    ''', (downloads, uninstalls, row[0]))
                else:
                    cursor.execute('''
                        INSERT INTO monthly_metrics (app_name, platform, month_year, downloads, uninstalls)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (app_name, platform, month, downloads, uninstalls))

            upsert_monthly("Germania Insurance", "Android", current_month, android_downloads, android_uninstalls)
            upsert_monthly("Germania Insurance", "iOS", current_month, ios_downloads, ios_uninstalls)
            conn.commit()

        return jsonify({"message": "Private data synced successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
