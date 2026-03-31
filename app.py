#!/usr/bin/env python3
import os, json, time, csv, io, threading, requests
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, send_file, jsonify, Response

app = Flask(__name__)

SHOPIFY_STORE = "tattoonumbingcreamco.myshopify.com"
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN", "")
API_VERSION = "2024-01"
HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}
CACHE_FILE = Path("/tmp/tnc_data_cache.json")
CACHE_MAX_AGE = 3600
_fetching = False

LOCATION_MAP = {
    35785179194: "usa", 62605688890: "usa",
    60906668090: "uk",
    11151310884: "aus",
}
COUNTRY_MAP = {
    "US": "usa", "CA": "usa",
    "GB": "uk", "IE": "uk", "FR": "uk", "DE": "uk", "NL": "uk",
    "AU": "aus", "NZ": "aus",
}

def shopify_get_all(endpoint, params):
    url = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/{endpoint}"
    resource = endpoint.split(".json")[0].split("/")[-1]
    all_items, page = [], 0
    while url:
        r = requests.get(url, headers=HEADERS, params=params if page==0 else None, timeout=30)
        r.raise_for_status()
        items = r.json().get(resource, [])
        all_items.extend(items)
        page += 1
        link = r.headers.get("Link", "")
        url = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.strip().split(";")[0].strip().strip("<>")
                    break
        time.sleep(0.35)
    return all_items

def fetch_live_data():
    global _fetching
    if _fetching:
        return
    _fetching = True
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00Z")
        orders = shopify_get_all("orders.json", {
            "limit": 250, "status": "any", "financial_status": "paid",
            "created_at_min": cutoff,
            "fields": "id,created_at,line_items,shipping_address"
        })

        # Count sales per SKU per location
        sales = {"usa": {}, "uk": {}, "aus": {}}
        for order in orders:
            addr = order.get("shipping_address") or {}
            loc = COUNTRY_MAP.get(addr.get("country_code",""), "usa")
            age_days = (datetime.utcnow() - datetime.fromisoformat(order["created_at"].replace("Z","").split("+")[0])).days
            for item in order.get("line_items", []):
                sku = (item.get("sku") or "").strip()
                if not sku: continue
                qty = item.get("quantity", 0)
                if sku not in sales[loc]:
                    sales[loc][sku] = {"qty": 0, "name": item.get("name",""), "days": max(age_days, 1)}
                sales[loc][sku]["qty"] += qty
                sales[loc][sku]["days"] = max(sales[loc][sku]["days"], age_days)

        # Fetch products
        products = shopify_get_all("products.json", {"limit": 250, "fields": "id,title,variants"})

        # Fetch inventory by location
        iids = [str(v["inventory_item_id"]) for p in products for v in p.get("variants",[]) if v.get("inventory_item_id")]
        inv_by_loc = {"usa": {}, "uk": {}, "aus": {}}
        iid_to_sku = {}
        for p in products:
            for v in p.get("variants",[]):
                if v.get("inventory_item_id") and v.get("sku"):
                    iid_to_sku[v["inventory_item_id"]] = v["sku"].strip()

        for i in range(0, len(iids), 50):
            batch = iids[i:i+50]
            r = requests.get(f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/inventory_levels.json",
                headers=HEADERS, params={"inventory_item_ids": ",".join(batch), "limit": 250}, timeout=30)
            for level in r.json().get("inventory_levels", []):
                iid = level["inventory_item_id"]
                loc_id = level["location_id"]
                loc = LOCATION_MAP.get(loc_id)
                if loc and iid in iid_to_sku:
                    sku = iid_to_sku[iid]
                    inv_by_loc[loc][sku] = inv_by_loc[loc].get(sku, 0) + max(0, level.get("available") or 0)
            time.sleep(0.35)

        # Build SKU data per location
        result = {}
        for loc in ["usa", "uk", "aus"]:
            all_skus = set(list(sales[loc].keys()) + list(inv_by_loc[loc].keys()))
            skus = []
            for sku in all_skus:
                stock = inv_by_loc[loc].get(sku, 0)
                s = sales[loc].get(sku, {})
                daily_rate = s["qty"] / max(s.get("days", 180), 1) if s.get("qty") else 0
                days = int(stock / daily_rate) if daily_rate > 0 else 999
                # Reorder: suggest enough for 90 days
                reorder = max(0, int(daily_rate * 90) - stock) if daily_rate > 0 else 0
                status = "critical" if days < 30 else "warning" if days < 60 else "ok"
                # Get product name
                name = s.get("name", sku)
                skus.append({
                    "sku": sku, "name": name, "stock": stock,
                    "daily_rate": round(daily_rate, 2),
                    "days_remaining": min(days, 999),
                    "status": status,
                    "reorder_qty": reorder
                })
            result[loc] = {"skus": skus}

        cache = {
            "fetched_at": datetime.now().strftime("%d %b %Y %H:%M"),
            "data": result
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        print(f"Fetch complete: {sum(len(v['skus']) for v in result.values())} SKU-location entries")
    except Exception as e:
        print(f"Fetch error: {e}")
    finally:
        _fetching = False

def get_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        return cache
    return None

def trigger_refresh():
    threading.Thread(target=fetch_live_data, daemon=True).start()

@app.route("/")
@app.route("/inventory-dashboard.html")
def dashboard():
    return send_file("inventory-dashboard.html")

@app.route("/api/dashboard")
def api_dashboard():
    cache = get_cache()
    if not cache:
        trigger_refresh()
        return jsonify({"status": "fetching", "message": "Data is being fetched, please wait 2 minutes and refresh"})
    result = cache.get("data", {})
    result["fetched_at"] = cache.get("fetched_at", "")
    return jsonify(result)

@app.route("/api/status")
def status():
    cache = get_cache()
    if cache:
        return jsonify({"status": "ready", "fetched_at": cache.get("fetched_at","")})
    trigger_refresh()
    return jsonify({"status": "fetching"})

@app.route("/api/refresh")
def refresh():
    trigger_refresh()
    return jsonify({"ok": True})

@app.route("/papaparse.min.js")
def papaparse():
    return send_file("papaparse.min.js", mimetype="application/javascript")

if __name__ == "__main__":
    trigger_refresh()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
