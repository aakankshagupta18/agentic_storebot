from agno.agent import Agent
from agno.models.ollama import Ollama
from graph.graph_store import GraphStore
from query.federation import run_sql, stitch, summarize
from query.sql_templates import SELECT_TEMPLATE

SYSTEM_PROMPT = """
You are the Data Access Agent. Use the provided knowledge graph summary to:
1) Decide which tables/engines to query.
2) Generate SQL (ANSI-ish) with quoted identifiers that match table/column spellings.
3) If multiple sources are needed, describe a merge plan: [ {left, right, on_left, on_right, how} ... ].
4) Return JSON with fields: {"sql": [{"engine": "...", "fq_table": "...", "cols": [...], "where": "...", "order_by": "...", "limit": N}], "stitch": [...], "answer_style": "table|summary"}.
Do NOT invent columns; use only those from the graph summary.
"""

def graph_summary(gs: GraphStore) -> str:
    lines=[]
    for t in gs.tables():
        loc = gs.resolve_table_location(t)
        cols = gs.columns(t)
        lines.append(f"- {t} @ {loc['engine']} schema={loc.get('schema')} table={loc.get('table')}\n  cols={', '.join([c.split('.')[-1] for c in cols])}")
    lines.append("Join hints:")
    for a,b,d in gs.G.edges(data=True):
        if d.get("type")=="join":
            lines.append(f"  {a} -> {b} ON {d.get('on')}")
    return "\n".join(lines)

class DataAccessAgent:
    def __init__(self, model_id: str, host: str):
        self.gs = GraphStore().load()
        self.agent = Agent(
            model=Ollama(id=model_id, host=host),
            system_prompt=SYSTEM_PROMPT,
            markdown=True
        )

    def answer(self, user_question: str):
        # 1) Ask LLM for structured query plan
        plan = self.agent.run(f"Knowledge graph:\n{graph_summary(self.gs)}\n\nQuestion: {user_question}\nRespond ONLY with JSON per schema.").content
        import json
        plan = json.loads(plan)

        # 2) Execute SQL parts
        frames = []
        for part in plan["sql"]:
            fq_table = part["fq_table"]
            engine = part["engine"]
            where = part.get("where")
            sql = SELECT_TEMPLATE.render(
                cols=[f'"{c}"' for c in part["cols"]],
                fq_table = fq_table if engine=="mysql" else f'{fq_table}',
                where=where,
                order_by=part.get("order_by"),
                limit=part.get("limit")
            )
            df = run_sql(engine, sql)
            df.attrs["table_name"]=fq_table
            frames.append(df)

        # 3) Stitch if needed
        merged = None
        if plan.get("stitch"):
            current = frames[0]
            for s in plan["stitch"]:
                # find df by table name
                right = next(df for df in frames if df.attrs["table_name"]==s["right"])
                current = stitch(current, right, s["on_left"], s["on_right"], s.get("how","left"))
            merged = current
        else:
            merged = frames[0] if frames else None

        # 4) Summarize
        if merged is None or merged.empty:
            return "No rows."
        if plan.get("answer_style") == "summary":
            return summarize(merged, limit=10)
        return summarize(merged, limit=25)

