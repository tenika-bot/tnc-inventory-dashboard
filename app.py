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
_fetch_lock = threading.Lock()
_fetching = False

# Country → location mapping
COUNTRY_MAP = {
    "US": "usa", "CA": "usa",  # North America → USA warehouse
    "GB": "uk", "IE": "uk", "FR": "uk", "DE": "uk", "NL": "uk", "SE": "uk", "NO": "uk", "DK": "uk", "BE": "uk", "CH": "uk", "AT": "uk", "IT": "uk", "ES": "uk", "PT": "uk",  # Europe → UK warehouse
    "AU": "aus", "NZ": "aus",  # Oceania → AUS warehouse
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
    with _fetch_lock:
        if _fetching:
            return
        _fetching = True
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00Z")
        orders = shopify_get_all("orders.json", {
            "limit": 250, "status": "any", "financial_status": "paid",
            "created_at_min": cutoff,
            "fields": "id,created_at,line_items,fulfillment_status,financial_status,shipping_address"
        })

        # Split orders by location
        orders_by_loc = {"usa": [], "uk": [], "aus": []}
        for order in orders:
            addr = order.get("shipping_address") or {}
            country = addr.get("country_code", "")
            loc = COUNTRY_MAP.get(country, "usa")  # default to USA if unknown
            for item in order.get("line_items", []):
                sku = (item.get("sku") or "").strip() or f'variant_{item.get("variant_id","?")}'
                row = {
                    "Created at": order["created_at"],
                    "Lineitem name": item.get("name", ""),
                    "Lineitem sku": sku,
                    "Lineitem quantity": item.get("quantity", 0),
                    "Financial Status": order.get("financial_status", ""),
                    "Fulfillment Status": order.get("fulfillment_status") or ""
                }
                orders_by_loc[loc].append(row)

        # Fetch products & inventory
        products = shopify_get_all("products.json", {"limit": 250, "fields": "id,title,variants"})
        iids = [str(v["inventory_item_id"]) for p in products for v in p.get("variants", []) if v.get("inventory_item_id")]
        inv_levels = {}
        for i in range(0, len(iids), 50):
            batch = iids[i:i+50]
            r = requests.get(
                f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/inventory_levels.json",
                headers=HEADERS,
                params={"inventory_item_ids": ",".join(batch), "limit": 250},
                timeout=30
            )
            for level in r.json().get("inventory_levels", []):
                iid = level["inventory_item_id"]
                inv_levels[iid] = inv_levels.get(iid, 0) + max(0, level.get("available") or 0)
            time.sleep(0.35)

        inv_rows = []
        for product in products:
            for variant in product.get("variants", []):
                sku = (variant.get("sku") or "").strip()
                if not sku: continue
                title = product["title"]
                vtitle = variant.get("title", "")
                if vtitle and vtitle.lower() not in ("default title", "default"):
                    title = f"{title} – {vtitle}"
                inv_rows.append({"SKU": sku, "Title": title, "Available": inv_levels.get(variant.get("inventory_item_id"), 0)})

        def to_csv(rows, fields):
            out = io.StringIO()
            w = csv.DictWriter(out, fieldnames=fields)
            w.writeheader(); w.writerows(rows)
            return out.getvalue()

        order_fields = ["Created at","Lineitem name","Lineitem sku","Lineitem quantity","Financial Status","Fulfillment Status"]
        inv_fields = ["SKU","Title","Available"]

        cache = {
            "fetched_at": datetime.now().isoformat(),
            "last_fetch": datetime.now().isoformat(),
            "orders_csv_usa": to_csv(orders_by_loc["usa"], order_fields),
            "orders_csv_uk":  to_csv(orders_by_loc["uk"],  order_fields),
            "orders_csv_aus": to_csv(orders_by_loc["aus"], order_fields),
            "inventory_csv":  to_csv(inv_rows, inv_fields),
            "order_count": sum(len(v) for v in orders_by_loc.values()),
            "sku_count": len(inv_rows),
            "counts": {k: len(v) for k, v in orders_by_loc.items()}
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        print(f"Fetched: USA={len(orders_by_loc['usa'])} UK={len(orders_by_loc['uk'])} AUS={len(orders_by_loc['aus'])} SKUs={len(inv_rows)}")
    finally:
        _fetching = False

def get_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        if (datetime.now() - datetime.fromisoformat(cache["fetched_at"])).total_seconds() < CACHE_MAX_AGE:
            return cache
    return None

def trigger_refresh():
    threading.Thread(target=fetch_live_data, daemon=True).start()

@app.route("/")
@app.route("/inventory-dashboard.html")
def dashboard():
    return send_file("inventory-dashboard.html")

@app.route("/shopify_orders_usa.csv")
def orders_usa():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("orders_csv_usa", ""), mimetype="text/csv")

@app.route("/shopify_orders_uk.csv")
def orders_uk():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("orders_csv_uk", ""), mimetype="text/csv")

@app.route("/shopify_orders_aus.csv")
def orders_aus():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("orders_csv_aus", ""), mimetype="text/csv")

@app.route("/shopify_inventory_aus.csv")
@app.route("/shopify_inventory_uk.csv")
@app.route("/shopify_inventory_usa.csv")
@app.route("/api/inventory")
def inventory_csv():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("inventory_csv", ""), mimetype="text/csv")

@app.route("/last_fetch.json")
@app.route("/api/status")
def status():
    cache = get_cache()
    if cache:
        return jsonify({"last_fetch": cache["fetched_at"], "order_count": cache.get("order_count",0), "sku_count": cache.get("sku_count",0), "counts": cache.get("counts",{})})
    trigger_refresh()
    return jsonify({"last_fetch": None, "status": "fetching"})

@app.route("/api/refresh")
def refresh():
    trigger_refresh()
    return jsonify({"ok": True})

if __name__ == "__main__":
    trigger_refresh()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
