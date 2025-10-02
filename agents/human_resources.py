from agno.agent import Agent
from agno.models.ollama import Ollama
from sqlalchemy import create_engine, text
from tools.emailer import send_mail
from dotenv import load_dotenv
import os, json

load_dotenv()
MY = create_engine(os.getenv("MYSQL_URL"), future=True)

SYSTEM = """
You are the Human Resources Agent for hierarchical escalations.
Given a request like 'escalate state X to LOB manager', you must:
1) Identify manager emails or names from manager tables.
2) Draft a professional escalation email (subject + body) from the asker to the appropriate LOB/regional manager.
Return JSON: {"to":"...", "subject":"...", "body":"..."} only.
"""

class HumanResourcesAgent:
    def __init__(self, model_id: str, host: str):
        self.agent = Agent(model=Ollama(id=model_id, host=host), system_prompt=SYSTEM, markdown=False)

    def _lookup_manager(self, region=None, state=None, segment=None, category=None):
        # naive examples; expand as needed
        with MY.connect() as c:
            if region:
                row = c.execute(text('SELECT `Manager` FROM customer_succces_managers WHERE `Regions`=:r LIMIT 1'), {"r":region}).fetchone()
                if row: return row[0]
            if state:
                row = c.execute(text('SELECT `Manager` FROM state_managers WHERE `State/Province`=:s LIMIT 1'), {"s":state}).fetchone()
                if row: return row[0]
        return None

    def draft_and_send(self, request_text: str, sender_email: str, region=None, state=None, segment=None, category=None, send=False, to_override=None):
        mgr = to_override or self._lookup_manager(region=region, state=state, segment=segment, category=category) or "manager@example.com"
        plan = self.agent.run(f"Asker: {sender_email}\nTarget: {mgr}\nRequest: {request_text}\nJSON only.").content
        data = json.loads(plan)
        if send:
            send_mail(data["to"], data["subject"], data["body"])
            return f"Email sent to {data['to']}."
        return f"Draft ready for {data['to']}:\nSubject: {data['subject']}\n\n{data['body']}"

