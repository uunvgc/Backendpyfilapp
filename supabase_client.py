import os
from dotenv import load_dotenv
from supabase import create_client

# Load env reliably on PythonAnywhere
load_dotenv("/home/Fiilthy/.env")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in /home/Fiilthy/.env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def supabase_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)