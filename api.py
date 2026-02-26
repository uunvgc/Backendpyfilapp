"""
FIILTHY Backend API Server
Handles lead retrieval, outreach, and system status
"""

import os
from flask import Flask, jsonify
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Create Supabase client
supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create Flask app
app = Flask(__name__)

# Import and register urgency endpoints
from api_endpoints import urgency_bp
app.register_blueprint(urgency_bp)


@app.route("/")
def home():
    return jsonify({
        "name": "FIILTHY Intelligence API",
        "status": "running",
        "version": "1.0"
    })


@app.route("/api/status")
def status():
    return jsonify({
        "status": "online",
        "database": "connected" if supabase else "not_connected"
    })


@app.route("/api/leads", methods=["GET"])
def get_all_leads():

    if not supabase:
        return jsonify({"error": "Database not configured"}), 503

    try:

        result = supabase.table("leads")\
            .select("*")\
            .order("timestamp", desc=True)\
            .limit(50)\
            .execute()

        return jsonify({
            "leads": result.data,
            "count": len(result.data)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/hot", methods=["GET"])
def get_hot_leads():

    if not supabase:
        return jsonify({"error": "Database not configured"}), 503

    try:

        result = supabase.table("leads")\
            .select("*")\
            .gte("urgency_score", 80)\
            .eq("status", "NEW")\
            .order("urgency_score", desc=True)\
            .execute()

        return jsonify({
            "hot_leads": result.data,
            "count": len(result.data)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)