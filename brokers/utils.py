import os
import pyotp
import requests
import json
from pathlib import Path


def _cache_file_path() -> Path:
    configured = os.environ.get("TOKEN_CACHE_FILE")
    if configured:
        return Path(configured)
    return Path("/tmp/.keystore.json")

def generate_totp_from_salt(salt, interval=30):
    salt = salt.encode()
    totp = pyotp.TOTP(salt, interval=interval)
    return totp.now()


def login2fa(username, request_id, otp_salt):
    response = requests.post("https://kite.zerodha.com/api/twofa", data={
        "user_id": username,
        "request_id": request_id,
        "twofa_value": generate_totp_from_salt(otp_salt),
        "twofa_type": "totp",
        "skip_session": "",
    })

    resp = response.cookies.get_dict()
    resp.update(response.json())
    resp.update(dict(response.headers))
    return resp


def is_token_valid(username, enctoken):
    # Attempt to call a known authenticated endpoint
    headers = {"Authorization": f"enctoken {enctoken}"}
    resp = requests.get("https://kite.zerodha.com/oms/user/profile", headers=headers)
    return resp.status_code == 200


def save_token_cache(data):
    cache_file = _cache_file_path()
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(data, f)


def load_token_cache():
    cache_file = _cache_file_path()
    if not cache_file.exists():
        return None
    with open(cache_file) as f:
        try:
            return json.load(f)
        except:
            return None


def login(username=None, password=None, otp_salt=None):
    if username is None:
        username = os.environ.get("USERID")
    if password is None:
        password = os.environ.get("PASSWORD")
    if otp_salt is None:
        otp_salt = os.environ.get("OTP_SALT")
    
    # insulated position  / order data
    
    
    # Try cached token
    
    cached = load_token_cache()
    if cached and is_token_valid( cached.get("user_id")  ,cached.get("enctoken")):
        print("✔ Reusing cached enctoken")
        return cached

    # Otherwise, perform full login
    print("🔐 Performing full login")
    response = requests.post("https://kite.zerodha.com/api/login",
                             data={"user_id": username, "password": password})
    if response.status_code != 200:
        raise Exception(f"Login failed: {response.text}")

    data = response.json()
    login_result = login2fa(username, data["data"]["request_id"], otp_salt)

    # Persist token
    if "enctoken" in login_result:
        save_token_cache(   { "user_id": username ,  "enctoken": login_result["enctoken"]})

    return login_result
