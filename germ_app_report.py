"""
Germania Insurance - Monthly App Download Stats Report
Pulls iOS and Android stats, saves HTML report for intranet.

Requirements:
    pip install requests google-cloud-storage PyJWT cryptography

Credentials needed:
    - iOS: AuthKey_2675Z3U832.p8 (Admin key with Analytics access)
    - Android: google-service-account.json
"""

import csv
import datetime
import gzip
import io
import os
import time

import jwt
import requests
from google.cloud import storage
from google.oauth2 import service_account


# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────

IOS_KEY_ID    = "2675Z3U832"
IOS_ISSUER_ID = "69a6de90-ea68-47e3-e053-5b8c7c11a4d1"
IOS_KEY_PATH  = os.environ.get("IOS_KEY_PATH", "/secrets/AuthKey_2675Z3U832.p8")
IOS_APP_ID    = "1535269629"

GOOGLE_SA_PATH = os.environ.get("GOOGLE_SA_PATH", "/secrets/google-service-account.json")
ANDROID_BUCKET = "pubsite_prod_5145116318652946598"
ANDROID_PKG    = "com.germania.mobile.app"

OUTPUT_DIR = os.environ.get("REPORT_OUTPUT_DIR", "/reports")


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def make_ios_token():
    key = open(IOS_KEY_PATH).read()
    now = int(time.time())
    return jwt.encode(
        {"iss": IOS_ISSUER_ID, "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"},
        key, algorithm="ES256", headers={"kid": IOS_KEY_ID}
    )


def decode_content(content):
    """Decompress if gzipped, decode with correct encoding, strip nulls."""
    try:
        content = gzip.decompress(content)
    except Exception:
        pass
    for enc in ["utf-16", "utf-16-le", "utf-8-sig", "utf-8", "latin-1"]:
        try:
            text = content.decode(enc)
            if text and len(text) > 5:
                return text.replace("\x00", "")
        except Exception:
            continue
    return content.decode("latin-1", errors="replace").replace("\x00", "")


# ─────────────────────────────────────────────
#  iOS STATS
# ─────────────────────────────────────────────

def get_ios_stats():
    token = make_ios_token()
    headers = {"Authorization": f"Bearer {token}"}
    base = "https://api.appstoreconnect.apple.com/v1"
    today = datetime.date.today()
    prev_month = today.replace(day=1) - datetime.timedelta(days=1)

    last_30 = 0
    total = 0

    # ── Last 30 days via Analytics metrics API ──
    r = requests.get(
        f"{base}/apps/{IOS_APP_ID}/metrics/installs",
        headers=headers,
        params={"granularity": "P1D", "period": "P30D"}
    )
    print(f"  iOS last30 API status: {r.status_code}")
    if r.status_code == 200:
        for item in r.json().get("data", []):
            for dp in item.get("dataPoints", []):
                last_30 += dp.get("values", {}).get("installs", 0)
        print(f"  iOS last30: {last_30}")
    else:
        print(f"  iOS last30 error: {r.text[:200]}")

    # ── All-time total via Analytics metrics API ──
    r2 = requests.get(
        f"{base}/apps/{IOS_APP_ID}/metrics/installs",
        headers=headers,
        params={"granularity": "P1M", "period": "P5Y"}
    )
    print(f"  iOS total API status: {r2.status_code}")
    if r2.status_code == 200:
        for item in r2.json().get("data", []):
            for dp in item.get("dataPoints", []):
                total += dp.get("values", {}).get("installs", 0)
        print(f"  iOS total: {total}")
    else:
        print(f"  iOS total error: {r2.text[:200]}")

    # ── Rating via app endpoint ──
    avg_rating = "N/A"
    r3 = requests.get(f"{base}/apps/{IOS_APP_ID}", headers=headers)
    if r3.status_code == 200:
        raw = r3.json().get("data", {}).get("attributes", {}).get("averageUserRating")
        if raw:
            avg_rating = round(float(raw), 1)
    print(f"  iOS rating: {avg_rating}")

    # ── Reviews for yearly breakdown ──
    last_review_date = "N/A"
    reviews_by_year = {}
    r4 = requests.get(
        f"{base}/apps/{IOS_APP_ID}/customerReviews?sort=-createdDate&limit=200",
        headers=headers
    )
    if r4.status_code == 200:
        all_revs = r4.json().get("data", [])
        if all_revs:
            created = all_revs[0]["attributes"].get("createdDate", "")
            if created:
                last_review_date = datetime.datetime.fromisoformat(
                    created.replace("Z", "+00:00")
                ).strftime("%b %Y")
        for rev in all_revs:
            yr = rev["attributes"].get("createdDate", "")[:4]
            stars = rev["attributes"].get("rating", 0)
            if yr:
                reviews_by_year.setdefault(yr, []).append(stars)

    return {
        "last_30_days": last_30,
        "total": total,
        "last_review_date": last_review_date,
        "rating": avg_rating,
        "reviews_by_year": reviews_by_year,
    }


# ─────────────────────────────────────────────
#  ANDROID STATS
# ─────────────────────────────────────────────

def get_android_stats():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_SA_PATH,
        scopes=["https://www.googleapis.com/auth/devstorage.read_only"],
    )
    client = storage.Client(credentials=creds, project=creds.project_id)
    bucket = client.bucket(ANDROID_BUCKET)
    today = datetime.date.today()

    last_30 = 0
    total = 0

    # Sort blobs by name (chronological)
    blobs = sorted(
        [b for b in bucket.list_blobs(prefix=f"stats/installs/installs_{ANDROID_PKG}")
         if "overview" in b.name],
        key=lambda b: b.name
    )

    print(f"  Android: processing {len(blobs)} monthly files")

    for blob in blobs:
        try:
            yyyymm = blob.name.split("_")[-2]
            report_date = datetime.date(int(yyyymm[:4]), int(yyyymm[4:]), 1)
        except Exception:
            continue

        text = decode_content(blob.download_as_bytes())
        monthly_sum = 0

        try:
            for row in csv.DictReader(io.StringIO(text)):
                pkg = row.get("Package name", row.get("Package Name", "")).strip()
                if pkg != ANDROID_PKG:
                    continue
                daily = int(row.get("Daily User Installs", 0) or 0)
                monthly_sum += daily
                total += daily
        except Exception as e:
            continue

        days_ago = (today - report_date).days
        if days_ago <= 62:
            last_30 = monthly_sum

    print(f"  Android last30: {last_30}, total: {total}")

    # ── Reviews from GCS reviews bucket ──
    reviews_by_year, last_review_month, avg_rating = get_android_reviews(bucket)

    return {
        "last_30_days": last_30,
        "total": total,
        "last_review_date": last_review_month,
        "rating": avg_rating,
        "reviews_by_year": reviews_by_year,
    }


def get_android_reviews(bucket):
    reviews_by_year = {}
    last_review_month = "N/A"
    ratings = []

    try:
        blobs = sorted(
            [b for b in bucket.list_blobs(prefix=f"reviews/reviews_{ANDROID_PKG}")],
            key=lambda b: b.name,
            reverse=True
        )
        for blob in blobs:
            try:
                yyyymm = blob.name.replace(".csv", "").split("_")[-1]
                yr = yyyymm[:4]
                mo = int(yyyymm[4:])
                review_month = datetime.date(int(yr), mo, 1).strftime("%b %Y")
            except Exception:
                continue

            text = decode_content(blob.download_as_bytes())
            try:
                for row in csv.DictReader(io.StringIO(text)):
                    stars_raw = row.get("Star Rating", row.get("Rating", ""))
                    try:
                        stars = int(float(stars_raw))
                        reviews_by_year.setdefault(yr, []).append(stars)
                        ratings.append(stars)
                        if last_review_month == "N/A":
                            last_review_month = review_month
                    except Exception:
                        continue
            except Exception:
                continue

    except Exception as e:
        print(f"  Android reviews error: {e}")

    avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else "N/A"
    print(f"  Android reviews: {len(ratings)} total, avg rating: {avg_rating}")
    return reviews_by_year, last_review_month, avg_rating


# ─────────────────────────────────────────────
#  HTML REPORT BUILDER
# ─────────────────────────────────────────────

def summarize_reviews(reviews_by_year):
    summary = {}
    for yr, star_list in reviews_by_year.items():
        pos = sum(1 for s in star_list if s >= 4)
        neg = sum(1 for s in star_list if s <= 2)
        parts = []
        if pos:
            parts.append(f"{pos} positive")
        if neg:
            parts.append(f"{neg} negative")
        summary[yr] = ", ".join(parts) if parts else "none"
    return summary


def fmt(n):
    if isinstance(n, int) and n >= 1000:
        return f"{n:,} ({round(n/1000)}K)"
    return str(n) if n else "N/A"


def build_html(ios, android):
    today = datetime.date.today()
    prev = today.replace(day=1) - datetime.timedelta(days=1)
    report_month = prev.strftime("%B %Y")
    generated = today.strftime("%B %d, %Y")

    ios_rev = summarize_reviews(ios.get("reviews_by_year", {}))
    and_rev = summarize_reviews(android.get("reviews_by_year", {}))
    all_years = sorted(set(list(ios_rev.keys()) + list(and_rev.keys())), reverse=True)

    rows = "".join(
        f"<tr><td>{yr}</td><td>{ios_rev.get(yr,'none')}</td>"
        f"<td>{and_rev.get(yr,'none')}</td></tr>"
        for yr in all_years
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Germania App Download Stats - {report_month}</title>
  <style>
    body{{font-family:Arial,sans-serif;font-size:14px;color:#333;max-width:960px;margin:40px auto;padding:0 24px}}
    .hdr{{border-bottom:3px solid #003DA5;padding-bottom:14px;margin-bottom:8px}}
    .hdr h1{{font-size:22px;color:#003DA5;margin:0 0 4px 0}}
    .meta{{color:#888;font-size:12px;margin-bottom:28px}}
    h2{{font-size:15px;color:#003DA5;margin-top:32px;border-left:4px solid #FFD700;padding-left:10px}}
    table{{border-collapse:collapse;width:100%;margin-top:8px}}
    th{{background:#FFD700;color:#333;padding:9px 14px;border:1px solid #ccc;text-align:left}}
    td{{padding:8px 14px;border:1px solid #ddd}}
    tr:nth-child(even){{background:#f9f9f9}}
    .badge{{display:inline-block;background:#003DA5;color:#fff;border-radius:4px;padding:2px 8px;font-size:11px}}
    .footer{{margin-top:40px;font-size:12px;color:#aaa;border-top:1px solid #eee;padding-top:12px}}
  </style>
</head>
<body>
  <div class="hdr">
    <h1>Germania Insurance — App Download Stats</h1>
    <div>App Store &amp; Google Play &nbsp;<span class="badge">{report_month}</span></div>
  </div>
  <div class="meta">Generated automatically on {generated}</div>

  <h2>Downloads</h2>
  <table>
    <tr><th>Platform</th><th>Metric Type</th><th>What It Includes</th>
        <th>Last 30 Days</th><th>Total Downloads</th><th>Last Review</th><th>Rating</th></tr>
    <tr>
      <td>iOS</td><td>Total Downloads</td><td>First-time installs + re-installs</td>
      <td>{ios.get('last_30_days','N/A')}</td><td>{fmt(ios.get('total','N/A'))}</td>
      <td>{ios.get('last_review_date','N/A')}</td><td>{ios.get('rating','N/A')}</td>
    </tr>
    <tr>
      <td>Android</td><td>All Devices</td><td>First-time installs + re-installs</td>
      <td>{android.get('last_30_days','N/A')}</td><td>{fmt(android.get('total','N/A'))}</td>
      <td>{android.get('last_review_date','N/A')}</td><td>{android.get('rating','N/A')}</td>
    </tr>
  </table>

  <h2>Reviews &amp; Rating by Year</h2>
  <table>
    <tr><th>Year</th><th>iOS Reviews</th><th>Google Play Reviews</th></tr>
    {rows}
  </table>

  <div class="footer">
    Auto-generated by Germania App Report — runs on the 5th of each month via Jenkins.<br>
    Questions? Contact: udokku@germaniainsurance.com
  </div>
</body>
</html>"""


# ─────────────────────────────────────────────
#  SAVE & MAIN
# ─────────────────────────────────────────────

def save_report(html):
    today = datetime.date.today()
    prev = today.replace(day=1) - datetime.timedelta(days=1)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for filename in [f"app_report_{prev.strftime('%Y_%m')}.html", "app_report_latest.html"]:
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  Saved: {path}")


def main():
    print("=" * 50)
    print("Fetching iOS stats...")
    ios = get_ios_stats()

    print("Fetching Android stats...")
    android = get_android_stats()

    print("Building HTML report...")
    html = build_html(ios, android)

    print(f"Saving to: {OUTPUT_DIR}")
    save_report(html)

    print("\n✅ Done!")
    print(f"   View at: http://gddapapp2.germania-ins.com:8085/app_report_latest.html")


if __name__ == "__main__":
    main()
