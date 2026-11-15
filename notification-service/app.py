import os, json, sqlite3, logging, asyncio
from contextlib import asynccontextmanager
import aio_pika
from fastapi import FastAPI

RABBIT_URL = os.environ["RABBITMQ_URL"]
DB_PATH = os.getenv("DB_PATH", "/data/notifications.db")

class JsonFmt(logging.Formatter):
    def format(self, r):
        return json.dumps({"level": r.levelname, "service": "notification",
            "correlation_id": getattr(r, "cid", None), "msg": r.getMessage()})
h = logging.StreamHandler(); h.setFormatter(JsonFmt())
log = logging.getLogger("notif"); log.addHandler(h); log.setLevel(logging.INFO)

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("CREATE TABLE IF NOT EXISTS processed(message_id TEXT PRIMARY KEY, order_id TEXT)")

async def handle(msg: aio_pika.IncomingMessage):
    async with msg.process(requeue=False):             # ack on success
        cid = msg.correlation_id
        try:
            db.execute("INSERT INTO processed VALUES(?,?)",
                       (msg.message_id, json.loads(msg.body)["order_id"]))
            db.commit()
        except sqlite3.IntegrityError:                 # idempotent consumer
            log.info("duplicate event ignored", extra={"cid": cid}); return
        body = json.loads(msg.body)
        log.info(f"sending confirmation email for order {body['order_id']}",
                 extra={"cid": cid})

async def consume(app):
    for _ in range(10):
        try:
            conn = await aio_pika.connect_robust(RABBIT_URL); break
        except Exception:
            await asyncio.sleep(2)
    ch = await conn.channel()
    ex = await ch.declare_exchange("maltashop.events",
            aio_pika.ExchangeType.TOPIC, durable=True)
    q = await ch.declare_queue("notification.order_placed", durable=True)
    await q.bind(ex, routing_key="order.placed")
    await q.consume(handle)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(consume(app)); yield; task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health(): return {"status": "ok"}
