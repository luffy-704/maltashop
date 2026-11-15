import os, json, uuid, sqlite3, logging, asyncio
from contextlib import asynccontextmanager
import aio_pika
from fastapi import FastAPI, Header
from pydantic import BaseModel

RABBIT_URL = os.environ["RABBITMQ_URL"]
DB_PATH = os.getenv("DB_PATH", "/data/orders.db")

class JsonFmt(logging.Formatter):
    def format(self, r):
        return json.dumps({"level": r.levelname, "service": "order",
            "correlation_id": getattr(r, "cid", None), "msg": r.getMessage()})
h = logging.StreamHandler(); h.setFormatter(JsonFmt())
log = logging.getLogger("order"); log.addHandler(h); log.setLevel(logging.INFO)

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("""CREATE TABLE IF NOT EXISTS orders(
    id TEXT PRIMARY KEY, idem_key TEXT UNIQUE, item TEXT, qty INT, status TEXT)""")

@asynccontextmanager
async def lifespan(app):
    for _ in range(10):                      # broker may still be starting
        try:
            app.state.conn = await aio_pika.connect_robust(RABBIT_URL); break
        except Exception:
            await asyncio.sleep(2)
    app.state.ch = await app.state.conn.channel()
    app.state.ex = await app.state.ch.declare_exchange(
        "maltashop.events", aio_pika.ExchangeType.TOPIC, durable=True)
    yield
    await app.state.conn.close()

app = FastAPI(lifespan=lifespan)

class OrderIn(BaseModel):
    item: str
    qty: int

@app.get("/health")
def health(): return {"status": "ok"}

@app.post("/orders", status_code=201)
async def place_order(o: OrderIn,
        idempotency_key: str = Header(...),            # idempotent create
        x_correlation_id: str = Header(default=None)):
    cid = x_correlation_id or str(uuid.uuid4())
    extra = {"cid": cid}
    row = db.execute("SELECT id,status FROM orders WHERE idem_key=?",
                     (idempotency_key,)).fetchone()
    if row:                                            # retry: return same order
        log.info("duplicate request, returning existing order", extra=extra)
        return {"order_id": row[0], "status": row[1], "correlation_id": cid}
    oid = str(uuid.uuid4())
    db.execute("INSERT INTO orders VALUES(?,?,?,?,?)",
               (oid, idempotency_key, o.item, o.qty, "PLACED")); db.commit()
    log.info(f"order {oid} saved", extra=extra)
    msg = aio_pika.Message(
        body=json.dumps({"order_id": oid, "item": o.item, "qty": o.qty}).encode(),
        message_id=oid, correlation_id=cid,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
    await app.state.ex.publish(msg, routing_key="order.placed")
    log.info("OrderPlaced published", extra=extra)
    return {"order_id": oid, "status": "PLACED", "correlation_id": cid}
