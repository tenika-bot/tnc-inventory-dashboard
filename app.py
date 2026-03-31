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

# Shopify location ID → dashboard location
LOCATION_MAP = {
    35785179194: "usa",  # Fulfillmate USA
    62605688890: "usa",  # USA ONLY
    60906668090: "uk",   # FodaFiment by Fodabox
    11151310884: "aus",  # NSW Warehouse
}

# Country code fallback for orders without location data
COUNTRY_MAP = {
    "US": "usa", "CA": "usa",
    "GB": "uk", "IE": "uk", "FR": "uk", "DE": "uk", "NL": "uk", "SE": "uk",
    "NO": "uk", "DK": "uk", "BE": "uk", "CH": "uk", "AT": "uk", "IT": "uk", "ES": "uk", "PT": "uk",
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
    with _fetch_lock:
        if _fetching:
            return
        _fetching = True
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00Z")

        # Fetch orders - split by shipping country
        orders = shopify_get_all("orders.json", {
            "limit": 250, "status": "any", "financial_status": "paid",
            "created_at_min": cutoff,
            "fields": "id,created_at,line_items,fulfillment_status,financial_status,shipping_address"
        })

        orders_by_loc = {"usa": [], "uk": [], "aus": []}
        for order in orders:
            addr = order.get("shipping_address") or {}
            country = addr.get("country_code", "")
            loc = COUNTRY_MAP.get(country, "usa")
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

        # Fetch products
        products = shopify_get_all("products.json", {"limit": 250, "fields": "id,title,variants"})

        # Fetch inventory levels per location
        iids = [str(v["inventory_item_id"]) for p in products for v in p.get("variants", []) if v.get("inventory_item_id")]
        inv_by_loc = {"usa": {}, "uk": {}, "aus": {}}

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
                loc_id = level["location_id"]
                loc = LOCATION_MAP.get(loc_id)
                if loc:
                    avail = max(0, level.get("available") or 0)
                    inv_by_loc[loc][iid] = inv_by_loc[loc].get(iid, 0) + avail
            time.sleep(0.35)

        # Build per-location inventory CSVs
        def build_inv_csv(loc):
            rows = []
            for product in products:
                for variant in product.get("variants", []):
                    sku = (variant.get("sku") or "").strip()
                    if not sku: continue
                    iid = variant.get("inventory_item_id")
                    available = inv_by_loc[loc].get(iid, 0)
                    title = product["title"]
                    vtitle = variant.get("title", "")
                    if vtitle and vtitle.lower() not in ("default title", "default"):
                        title = f"{title} – {vtitle}"
                    rows.append({"SKU": sku, "Title": title, "Available": available})
            return to_csv(rows, ["SKU","Title","Available"])

        def to_csv(rows, fields):
            out = io.StringIO()
            w = csv.DictWriter(out, fieldnames=fields)
            w.writeheader(); w.writerows(rows)
            return out.getvalue()

        order_fields = ["Created at","Lineitem name","Lineitem sku","Lineitem quantity","Financial Status","Fulfillment Status"]

        cache = {
            "fetched_at": datetime.now().isoformat(),
            "last_fetch": datetime.now().isoformat(),
            "orders_csv_usa": to_csv(orders_by_loc["usa"], order_fields),
            "orders_csv_uk":  to_csv(orders_by_loc["uk"],  order_fields),
            "orders_csv_aus": to_csv(orders_by_loc["aus"], order_fields),
            "inventory_csv_usa": build_inv_csv("usa"),
            "inventory_csv_uk":  build_inv_csv("uk"),
            "inventory_csv_aus": build_inv_csv("aus"),
            "order_count": sum(len(v) for v in orders_by_loc.values()),
            "sku_count": len(products),
            "counts": {k: len(v) for k, v in orders_by_loc.items()}
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        print(f"Fetched: USA={len(orders_by_loc['usa'])} UK={len(orders_by_loc['uk'])} AUS={len(orders_by_loc['aus'])}")
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
    from pathlib import Path
    html = Path("inventory-dashboard.html").read_text()
    cache = get_cache()
    if cache:
        orders_csv = cache.get("orders_csv_aus", "") or cache.get("orders_csv", "")
        inv_csv = cache.get("inventory_csv_aus", "") or cache.get("inventory_csv", "")
        # Escape for JS injection
        import json as _json
        inject = f"""<script>
// Pre-loaded data from server
window._serverData = {{
  orders_aus: {_json.dumps(cache.get("orders_csv_aus",""))},
  orders_uk:  {_json.dumps(cache.get("orders_csv_uk",""))},
  orders_usa: {_json.dumps(cache.get("orders_csv_usa",""))},
  inv_aus:    {_json.dumps(cache.get("inventory_csv_aus","") or cache.get("inventory_csv",""))},
  inv_uk:     {_json.dumps(cache.get("inventory_csv_uk","") or cache.get("inventory_csv",""))},
  inv_usa:    {_json.dumps(cache.get("inventory_csv_usa","") or cache.get("inventory_csv",""))},
  fetched_at: {_json.dumps(cache.get("fetched_at",""))}
}};
</script>"""
        html = html.replace("</head>", inject + "</head>")
    return html

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

@app.route("/shopify_inventory_usa.csv")
def inv_usa():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("inventory_csv_usa", ""), mimetype="text/csv")

@app.route("/shopify_inventory_uk.csv")
def inv_uk():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("inventory_csv_uk", ""), mimetype="text/csv")

@app.route("/shopify_inventory_aus.csv")
def inv_aus():
    cache = get_cache()
    if not cache: trigger_refresh(); return Response("", mimetype="text/csv")
    return Response(cache.get("inventory_csv_aus", ""), mimetype="text/csv")

@app.route("/last_fetch.json")
@app.route("/api/status")
def status():
    cache = get_cache()
    if cache:
        return jsonify({"last_fetch": cache["fetched_at"], "order_count": cache.get("order_count",0), "sku_count": cache.get("sku_count",0), "counts": cache.get("counts",{})})
    trigger_refresh()
    return jsonify({"last_fetch": None, "status": "fetching"})

@app.route("/papaparse.min.js")
def papaparse():
    return send_file("papaparse.min.js", mimetype="application/javascript")

@app.route("/api/refresh")
def refresh():
    trigger_refresh()
    return jsonify({"ok": True})

if __name__ == "__main__":
    trigger_refresh()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
