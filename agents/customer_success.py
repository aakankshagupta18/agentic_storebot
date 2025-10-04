from agno.agent import Agent
#from agno.models.ollama import Ollama
from agno.models.google import Gemini
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from agents.json_utils import loads_relaxed
import os, json
from tools.safety import guard_write

load_dotenv()

PG = create_engine(os.getenv("POSTGRES_URL"), future=True)
MY = create_engine(os.getenv("MYSQL_URL"), future=True)

SYSTEM = """
You are the Customer Success Agent.
You handle: new orders, updates to undelivered orders, and accepting eligible returns.

STRICT JSON ONLY:
{"operation":"insert|update|delete", "engine":"postgres|mysql", "sql":"...", "params": {...}, "confirmation_hint":"string"}

CRITICAL SCHEMA RULES:
- Returns live in Postgres at ref.returns with EXACT columns: "Returned" (TEXT), "ID" (TEXT).
  • To record a return for Order ID X: INSERT INTO ref.returns ("Returned","ID") VALUES ('Yes', :order_id)
  • Do NOT invent columns like order_id or return_date on this table.
- Orders live in Postgres (sales.orders). Manager lookup tables live in MySQL.
- Only modify undelivered orders: "Ship Date" IS NULL or > CURRENT_DATE.
- Always parameterize via :param style and quote identifiers exactly as shown.
"""

import re

RETURNS_INSERT_RX = re.compile(
    r'insert\s+into\s+ref\.returns\s*\((.*?)\)\s*values\s*\((.*?)\)',
    re.IGNORECASE | re.DOTALL
)

def normalize_returns_insert(sql: str, params: dict):
    """Rewrite common bad inserts into ref.returns to the canonical schema."""
    low = sql.lower()
    if "ref.returns" not in low:
        return sql, params, False
    m = RETURNS_INSERT_RX.search(sql)
    if not m:
        return sql, params, False

    cols = [c.strip(' "`') for c in m.group(1).split(",")]
    # Already correct?
    if set(map(str.lower, cols)) == {"returned", "id"}:
        return sql, params, False

    # Map likely params to :order_id
    order_id = (
        params.get("order_id") or params.get("Order ID") or
        params.get("ID") or params.get("id")
    )
    if not order_id:
        # last resort: try to extract a literal from VALUES list (not ideal, but better than failing)
        return 'INSERT INTO ref.returns ("Returned","ID") VALUES (\'Yes\', :order_id)', params, False

    new_sql = 'INSERT INTO ref.returns ("Returned","ID") VALUES (\'Yes\', :order_id)'
    new_params = dict(params)
    new_params["order_id"] = order_id
    return new_sql, new_params, True


class CustomerSuccessAgent:
    def __init__(self, model_id: str, host: str):
        #self.agent = Agent(model=Ollama(id=model_id, host=host), system_message=SYSTEM, markdown=False)
        self.agent = Agent(model=Gemini(id=model_id), system_message=SYSTEM, markdown=False)

    # def act(self, user_request: str, confirmed: bool=False):
    #     plan = self.agent.run(user_request + "\nRespond JSON ONLY.").content
    #     data = loads_relaxed(plan)

    #     ok, msg = guard_write(f"{data['operation']} on {data['engine']}", confirmed)
    #     if not ok:
    #         return msg

    #     eng = PG if data["engine"]=="postgres" else MY
    #     with eng.begin() as c:
    #         c.execute(text(data["sql"]), data.get("params", {}))
    #     return f"SUCCESS: {data['operation']} executed.\nHint: {data.get('confirmation_hint','')}"

    def act(self, user_request: str, confirmed: bool=False):
        plan = self.agent.run(user_request + "\nRespond JSON ONLY.").content
        data = loads_relaxed(plan)

        ok, msg = guard_write(f"{data['operation']} on {data['engine']}", confirmed)
        if not ok:
            return msg

        eng = PG if data["engine"]=="postgres" else MY
        sql = data["sql"]
        params = data.get("params", {})

        # Normalize returns inserts and make idempotent
        if data["operation"] == "insert" and "ref.returns" in sql.lower():
            sql, params, changed = normalize_returns_insert(sql, params)
            with eng.begin() as c:
                # idempotent: clear any earlier return for this order
                if "order_id" in params:
                    c.execute(text('DELETE FROM ref.returns WHERE "ID"=:order_id'), {"order_id": params["order_id"]})
                c.execute(text(sql), params)
            return f"SUCCESS: return recorded for order {params.get('order_id','(unknown)')}."

        # Default path
        with eng.begin() as c:
            c.execute(text(sql), params)
        return f"SUCCESS: {data['operation']} executed.\nHint: {data.get('confirmation_hint','')}"


