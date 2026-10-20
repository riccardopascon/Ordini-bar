#!/usr/bin/env python3
"""
bar – Server HTTP + WebSocket
Persistenza: Supabase (se configurato) oppure file JSON locali.
"""

import asyncio
import json
import mimetypes
import os
import urllib.error
import urllib.request
from datetime import datetime, date
from pathlib import Path

import websockets
from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers

DATA_DIR = Path(__file__).parent / "data"
MENU_FILE = DATA_DIR / "menu.json"
ORDERS_FILE = DATA_DIR / "completed.json"

# Supabase (opzionale) – impostare su Render come variabili d'ambiente
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY") or ""
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

clients = set()
orders = []
completed_today = []
next_id = 1

DEFAULT_MENU = [
    {"id": 1, "name": "Cola", "category": "Bevande", "description": "Lattina da 33 cl", "price": 3.00, "stock": 89},
    {"id": 2, "name": "Acqua", "category": "Bevande", "description": "Bottiglia 50 cl", "price": 2.00, "stock": 120},
    {"id": 3, "name": "Hamburger", "category": "Cibo", "description": "Manzo, cheddar, lattuga e salsa della casa", "price": 11.50, "stock": 7},
    {"id": 4, "name": "Cheeseburger", "category": "Cibo", "description": "Manzo, doppio cheddar, cipolla croccante", "price": 12.50, "stock": 5},
    {"id": 5, "name": "Toast", "category": "Cibo", "description": "Pane, prosciutto cotto, formaggio", "price": 6.50, "stock": 15},
    {"id": 6, "name": "Pizza Margherita", "category": "Cibo", "description": "Pomodoro, mozzarella, basilico", "price": 8.50, "stock": 20},
    {"id": 7, "name": "Pizza Diavola", "category": "Cibo", "description": "Pomodoro, mozzarella, salame piccante", "price": 9.50, "stock": 12},
    {"id": 8, "name": "Patatine Fritte", "category": "Cibo", "description": "Porzione abbondante", "price": 4.00, "stock": 30},
    {"id": 9, "name": "Insalata Mista", "category": "Cibo", "description": "Insalata di stagione con condimento", "price": 5.50, "stock": 10},
]


def _supabase_request(method, path, body=None, extra_headers=None):
    """Chiamata REST a Supabase. Ritorna (ok, data_or_error)."""
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    if extra_headers:
        headers.update(extra_headers)
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return True, None
            return True, json.loads(raw)
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        print(f"⚠️ Supabase HTTP {e.code}: {err[:300]}")
        return False, err
    except Exception as e:
        print(f"⚠️ Supabase errore: {e}")
        return False, str(e)


def _sb_get(key):
    ok, data = _supabase_request("GET", f"app_store?key=eq.{key}&select=value")
    if not ok or not data:
        return None
    if isinstance(data, list) and len(data) > 0:
        return data[0].get("value")
    return None


def _sb_set(key, value):
    """Salva su Supabase con upsert (crea o aggiorna)."""
    payload = {"key": key, "value": value, "updated_at": datetime.utcnow().isoformat() + "Z"}
    # Upsert corretto PostgREST: on_conflict sulla primary key
    ok, err = _supabase_request(
        "POST",
        "app_store?on_conflict=key",
        body=payload,
        extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )
    if ok:
        return True
    # Fallback: prova UPDATE se la riga esiste già
    ok2, _ = _supabase_request(
        "PATCH",
        f"app_store?key=eq.{key}",
        body={"value": value, "updated_at": datetime.utcnow().isoformat() + "Z"},
        extra_headers={"Prefer": "return=minimal"},
    )
    if ok2:
        return True
    # Ultimo tentativo: INSERT semplice
    ok3, _ = _supabase_request(
        "POST",
        "app_store",
        body=payload,
        extra_headers={"Prefer": "return=minimal"},
    )
    return ok3


def load_menu():
    if USE_SUPABASE:
        print("📦 Carico menu da Supabase…")
        val = _sb_get("menu")
        if isinstance(val, list) and len(val) > 0:
            return val
        # Prima volta: salva menu di default
        save_menu(DEFAULT_MENU)
        return [dict(m) for m in DEFAULT_MENU]

    DATA_DIR.mkdir(exist_ok=True)
    if MENU_FILE.exists():
        try:
            data = json.loads(MENU_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list) and len(data) > 0:
                return data
        except Exception:
            pass
    save_menu(DEFAULT_MENU)
    return [dict(m) for m in DEFAULT_MENU]


def save_menu(menu_list):
    if USE_SUPABASE:
        if _sb_set("menu", menu_list):
            print("💾 Menu salvato su Supabase")
        else:
            print("⚠️ Salvataggio menu su Supabase fallito")
        return
    DATA_DIR.mkdir(exist_ok=True)
    MENU_FILE.write_text(json.dumps(menu_list, ensure_ascii=False, indent=2), encoding="utf-8")


def load_completed():
    if USE_SUPABASE:
        print("📦 Carico ordini completati da Supabase…")
        val = _sb_get("completed")
        if isinstance(val, list):
            return val
        _sb_set("completed", [])
        return []

    DATA_DIR.mkdir(exist_ok=True)
    if ORDERS_FILE.exists():
        try:
            data = json.loads(ORDERS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def save_completed():
    if USE_SUPABASE:
        if _sb_set("completed", completed_today):
            print("💾 Ordini completati salvati su Supabase")
        else:
            print("⚠️ Salvataggio ordini su Supabase fallito")
        return
    DATA_DIR.mkdir(exist_ok=True)
    ORDERS_FILE.write_text(json.dumps(completed_today, ensure_ascii=False, indent=2), encoding="utf-8")


print(f"🔧 Persistenza: {'Supabase' if USE_SUPABASE else 'file locali (data/)'}")
MENU = load_menu()
completed_today = load_completed()
if completed_today:
    try:
        next_id = max(int(o.get("id") or 0) for o in completed_today) + 1
    except Exception:
        next_id = 1


async def broadcast(message: dict):
    if not clients:
        return
    data = json.dumps(message)
    await asyncio.gather(
        *[ws.send(data) for ws in clients],
        return_exceptions=True,
    )


def get_stats(from_date=None, to_date=None):
    """
    Calcola statistiche.
    from_date / to_date: stringhe 'YYYY-MM-DD' (inclusive).
    Se non passate → solo oggi.
    """
    today_str = date.today().isoformat()

    if not from_date and not to_date:
        from_date = today_str
        to_date = today_str

    if not from_date:
        from_date = "1970-01-01"
    if not to_date:
        to_date = today_str

    # Fine giornata inclusa
    to_end = to_date + "T23:59:59.999999"

    filtered = []
    for o in completed_today:
        ca = o.get("completedAt", "")
        if not ca:
            continue
        day = ca[:10]
        if from_date <= day <= to_date:
            filtered.append(o)

    total_orders = len(filtered)
    total_revenue = 0.0
    # name -> {qty, revenue}
    dish_map = {}

    for order in filtered:
        for item in order.get("items", []):
            qty = item.get("qty", 1)
            price = float(item.get("price", 0))
            name = item.get("name", "?")
            total_revenue += qty * price
            if name not in dish_map:
                dish_map[name] = {"qty": 0, "revenue": 0.0}
            dish_map[name]["qty"] += qty
            dish_map[name]["revenue"] += qty * price

    dishes = sorted(
        [{"name": n, "qty": v["qty"], "revenue": round(v["revenue"], 2)} for n, v in dish_map.items()],
        key=lambda x: -x["qty"],
    )

    # Lista ordini con tempo di preparazione (ingresso cucina → consegna)
    order_list = []
    for o in sorted(filtered, key=lambda x: x.get("completedAt", ""), reverse=True):
        created = o.get("createdAt", "")
        completed = o.get("completedAt", "")
        duration_sec = None
        duration_label = "—"
        try:
            if created and completed:
                t0 = datetime.fromisoformat(created)
                t1 = datetime.fromisoformat(completed)
                duration_sec = max(0, int((t1 - t0).total_seconds()))
                mins, secs = divmod(duration_sec, 60)
                if mins >= 60:
                    hours, mins = divmod(mins, 60)
                    duration_label = f"{hours}h {mins}m"
                else:
                    duration_label = f"{mins} min {secs:02d}s"
        except Exception:
            pass

        items_summary = ", ".join(
            f"{it.get('qty', 1)}× {it.get('name', '?')}" + (f" ({it['note']})" if it.get("note") else "")
            for it in o.get("items", [])
        )

        order_list.append({
            "id": o.get("id"),
            "tableOrName": o.get("tableOrName", "—"),
            "createdAt": created,
            "completedAt": completed,
            "durationSec": duration_sec,
            "durationLabel": duration_label,
            "itemsSummary": items_summary,
            "items": o.get("items", []),
        })

    pending = len([o for o in orders if o.get("status") == "pending"])
    ready = len([o for o in orders if o.get("status") == "ready"])

    completed_today_count = len([o for o in completed_today if o.get("completedAt", "").startswith(today_str)])

    return {
        "totalOrders": total_orders,
        "totalRevenue": round(total_revenue, 2),
        "topDishes": dishes[:20],
        "dishes": dishes,
        "orders": order_list,
        "pending": pending,
        "ready": ready,
        "completedToday": completed_today_count,
        "fromDate": from_date,
        "toDate": to_date,
    }


def process_request(connection, request):
    # Se è un upgrade WebSocket, lascia passare (non servire HTML)
    upgrade = request.headers.get("Upgrade", "")
    if upgrade and upgrade.lower() == "websocket":
        return None

    path = request.path.split("?")[0]

    # Stato persistenza (per verifica)
    if path == "/api/status":
        info = {
            "ok": True,
            "persistence": "supabase" if USE_SUPABASE else "local_files",
            "supabaseConfigured": USE_SUPABASE,
            "menuItems": len(MENU),
            "completedOrders": len(completed_today),
        }
        if USE_SUPABASE:
            test = _sb_get("menu")
            info["supabaseReachable"] = test is not None or True
            info["menuFromDb"] = isinstance(test, list)
        body = json.dumps(info, ensure_ascii=False).encode("utf-8")
        return Response(200, "OK", Headers([
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-cache"),
        ]), body)

    if path == "/":
        path = "/index.html"

    file_path = (Path(__file__).parent / "public" / path.lstrip("/")).resolve()
    public_dir = (Path(__file__).parent / "public").resolve()

    try:
        if not str(file_path).startswith(str(public_dir)):
            return Response(404, "Not Found", Headers(), b"Not Found")
    except Exception:
        return Response(404, "Not Found", Headers(), b"Not Found")

    if file_path.is_file():
        content = file_path.read_bytes()
        content_type, _ = mimetypes.guess_type(str(file_path))
        if content_type is None:
            content_type = "application/octet-stream"
        headers = Headers([
            ("Content-Type", content_type),
            ("Content-Length", str(len(content))),
            ("Cache-Control", "no-cache"),
        ])
        return Response(200, "OK", headers, content)

    return Response(404, "Not Found", Headers(), b"Not Found")


async def handler(websocket):
    global next_id
    clients.add(websocket)
    print(f"✅ Client connesso ({len(clients)})")

    await websocket.send(json.dumps({
        "type": "init",
        "orders": orders,
        "menu": MENU,
        "stats": get_stats(),
    }))

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")

            if t == "new-order":
                items = msg.get("items", [])
                for it in items:
                    for m in MENU:
                        if m["id"] == it.get("id"):
                            m["stock"] = max(0, m["stock"] - it.get("qty", 1))
                            it["price"] = m["price"]
                            break
                save_menu(MENU)

                order = {
                    "id": next_id,
                    "tableOrName": msg.get("tableOrName") or "Senza nome",
                    "items": items,
                    "createdAt": datetime.now().isoformat(),
                    "status": "pending",
                }
                next_id += 1
                orders.append(order)
                orders.sort(key=lambda o: o["createdAt"])
                print(f"📦 Ordine #{order['id']} – {order['tableOrName']}")
                await broadcast({"type": "order-added", "order": order, "menu": MENU, "stats": get_stats()})

            elif t == "mark-ready":
                order_id = msg.get("orderId")
                for o in orders:
                    if o["id"] == order_id and o["status"] == "pending":
                        o["status"] = "ready"
                        print(f"🟡 Ordine #{order_id} pronto")
                        await broadcast({"type": "order-updated", "order": o, "stats": get_stats()})
                        break

            elif t == "complete-order":
                order_id = msg.get("orderId")
                for i, o in enumerate(orders):
                    if o["id"] == order_id:
                        o["status"] = "completed"
                        o["completedAt"] = datetime.now().isoformat()
                        completed_today.append(o)
                        save_completed()
                        orders.pop(i)
                        print(f"✅ Ordine #{order_id} completato")
                        await broadcast({"type": "order-removed", "orderId": order_id, "stats": get_stats()})
                        break

            elif t == "update-menu-item":
                item = msg.get("item")
                if item and "id" in item:
                    for m in MENU:
                        if m["id"] == item["id"]:
                            m.update({
                                "name": item.get("name", m["name"]),
                                "category": item.get("category", m["category"]),
                                "description": item.get("description", m["description"]),
                                "price": float(item.get("price", m["price"])),
                                "stock": int(item.get("stock", m["stock"])),
                            })
                            break
                    save_menu(MENU)
                    await broadcast({"type": "menu-updated", "menu": MENU})

            elif t == "add-menu-item":
                item = msg.get("item")
                if item:
                    new_id = max((m["id"] for m in MENU), default=0) + 1
                    new_item = {
                        "id": new_id,
                        "name": item.get("name", "Nuovo piatto"),
                        "category": item.get("category", "Cibo"),
                        "description": item.get("description", ""),
                        "price": float(item.get("price", 0)),
                        "stock": int(item.get("stock", 0)),
                    }
                    MENU.append(new_item)
                    save_menu(MENU)
                    await broadcast({"type": "menu-updated", "menu": MENU})

            elif t == "delete-menu-item":
                item_id = msg.get("id")
                if item_id is not None:
                    MENU[:] = [m for m in MENU if m["id"] != item_id]
                    save_menu(MENU)
                    await broadcast({"type": "menu-updated", "menu": MENU})

            elif t == "get-stats":
                from_d = msg.get("fromDate") or None
                to_d = msg.get("toDate") or None
                await websocket.send(json.dumps({
                    "type": "stats",
                    "stats": get_stats(from_d, to_d),
                }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        print(f"❌ Client disconnesso ({len(clients)})")


async def main():
    import os
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", "8080"))
    print(f"🚀 bar server su http://{host}:{port}")
    print(f"   Menu salvato in: {MENU_FILE}")
    async with serve(handler, host, port, process_request=process_request):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
