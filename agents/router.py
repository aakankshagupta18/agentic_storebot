from agno.agent import Agent
#from agno.models.ollama import Ollama
from agno.models.google import Gemini
from agents.data_access import DataAccessAgent
from agents.customer_success import CustomerSuccessAgent
from agents.human_resources import HumanResourcesAgent
import os

INTENT_SYSTEM = """
You are the Router. Classify the user's request into one of:
- "data_access" (questions / analytics),
- "customer_success" (create/modify orders, returns),
- "hr" (org hierarchy / escalations / email drafting/ escalate to managers).
Return JSON: {"intent":"data_access|customer_success|hr"} ONLY.
"""

class Router:
    def __init__(self):
        #model_id = os.getenv("OLLAMA_MODEL", "tinyllama")
        model_id = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        #self.router = Agent(model=Ollama(id=model_id, host=host), system_message=INTENT_SYSTEM, markdown=False)
        self.router = Agent(model=Gemini(id=model_id), system_message=INTENT_SYSTEM, markdown=False)
        self.da = DataAccessAgent(model_id, host)
        self.cs = CustomerSuccessAgent(model_id, host)
        self.hr = HumanResourcesAgent(model_id, host)

    def handle(self, msg: str, **kwargs):
        import json
        print(self.router.run(msg).content)
        intent = json.loads(self.router.run(msg).content)["intent"]
        print('intent:', intent)
        if intent == "data_access":
            return self.da.answer(msg)
        if intent == "customer_success":
            return self.cs.act(msg, confirmed=kwargs.get("confirmed", False))
        if intent == "hr":
            return self.hr.draft_and_send(msg, sender_email=kwargs.get("sender_email","user@example.com"),
                                          region=kwargs.get("region"), state=kwargs.get("state"),
                                          segment=kwargs.get("segment"), category=kwargs.get("category"),
                                          send=kwargs.get("send_email", False), to_override=kwargs.get("to"))
        return "Sorry, I couldn't route that."

