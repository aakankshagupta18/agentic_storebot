from agno.agent import Agent
from agno.models.ollama import Ollama
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os, json
from tools.safety import guard_write

load_dotenv()

PG = create_engine(os.getenv("POSTGRES_URL"), future=True)
MY = create_engine(os.getenv("MYSQL_URL"), future=True)

SYSTEM = """
You are the Customer Success Agent.
You handle: new orders, updates to undelivered orders, and accepting eligible returns.
Return STRICT JSON: 
{"operation":"insert|update|delete", "engine":"postgres|mysql", "sql":"...", "params": {...}, "confirmation_hint":"string"}
Rules:
- Orders live primarily in Postgres (sales.orders).
- Returns live in Postgres (ref.returns).
- Manager lookup tables live in MySQL.
- Only modify undelivered orders: where "Ship Date" IS NULL or > CURRENT_DATE (if dates present).
- Always parameterize changes via :param style. 
"""

class CustomerSuccessAgent:
    def __init__(self, model_id: str, host: str):
        self.agent = Agent(model=Ollama(id=model_id, host=host), system_prompt=SYSTEM, markdown=False)

    def act(self, user_request: str, confirmed: bool=False):
        plan = self.agent.run(user_request + "\nRespond JSON ONLY.").content
        data = json.loads(plan)

        ok, msg = guard_write(f"{data['operation']} on {data['engine']}", confirmed)
        if not ok:
            return msg

        eng = PG if data["engine"]=="postgres" else MY
        with eng.begin() as c:
            c.execute(text(data["sql"]), data.get("params", {}))
        return f"SUCCESS: {data['operation']} executed.\nHint: {data.get('confirmation_hint','')}"

