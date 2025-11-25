from flask import Flask, request, jsonify, render_template
import pandas as pd
import psycopg2
import os

app = Flask(__name__)

def get_connection():
    """
    Use DATABASE_URL environment variable for production (Render/Railway),
    fallback to localhost for local development.
    """
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Production: use the DATABASE_URL
        # Fix for Render's postgres:// vs postgresql:// issue
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(database_url)
    else:
        # Local development
        return psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="postgres",
            user="fisayo_ojo",
            password="",
        )


def load_summary_from_db():
    """
    Load data from view result and table taxi_zones.
    """
    conn = get_connection()

    query = """
        SELECT
            r.date,
            lower(r.bucket) AS period,
            tz.locationid::int AS "LocationID",
            r.zone AS "Zone",
            r.people AS ridership,
            r.category
        FROM public.result r
        JOIN public.taxi_zones tz
          ON r.zone = tz.zone
        WHERE r.zone IS NOT NULL
          AND tz.borough = 'Manhattan'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    return df


# Load once at startup
SUMMARY = load_summary_from_db()


@app.route("/")
def index():
    dates = sorted(SUMMARY["date"].unique())
    return render_template("index.html", dates=dates)


@app.route("/api/busyness")
def api_busyness():
    date_str = request.args.get("date")
    period = request.args.get("period")

    if not date_str or not period:
        return jsonify({"error": "Missing date or period"}), 400

    key_date = date_str
    period = period.lower()

    df = SUMMARY[(SUMMARY["date"] == key_date) & (SUMMARY["period"] == period)].copy()

    if df.empty:
        return jsonify([])

    records = []
    for _, row in df.iterrows():
        try:
            loc_id = int(row["LocationID"])
        except Exception:
            continue

        records.append(
            {
                "location_id": loc_id,
                "zone": row["Zone"],
                "ridership": float(row["ridership"]),
                "category": row["category"],
            }
        )

    return jsonify(records)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))