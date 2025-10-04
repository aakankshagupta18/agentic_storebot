# agents/data_access.py
from __future__ import annotations

import os
import re
from typing import Dict, List

from agno.agent import Agent
from agno.models.google import Gemini
from graph.graph_store import GraphStore
from query.federation import run_sql, summarize
from sqlalchemy.exc import ProgrammingError, ResourceClosedError


# =========================
# Small, generic SQL helpers
# =========================

_SQL_FENCE = re.compile(r"```sql\s*(.*?)```", re.IGNORECASE | re.DOTALL)

def extract_sql_block(text: str) -> str | None:
    """Return the SQL inside ```sql fences, else the first WITH/SELECT block."""
    if not text:
        return None
    m = _SQL_FENCE.search(text)
    if m:
        return m.group(1).strip()
    m = re.search(r"(?is)\b(with|select)\b.+", text)
    return m.group(0).strip() if m else None

def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # /* ... */
    lines = []
    for line in sql.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        lines.append(line)
    return "\n".join(lines)

def split_sql_statements(sql: str) -> List[str]:
    """Split by semicolons outside quotes."""
    stmts, buf = [], []
    in_s, in_d = False, False
    for ch in sql:
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        if ch == ";" and not in_s and not in_d:
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(ch)
    s = "".join(buf).strip()
    if s:
        stmts.append(s)
    return stmts

def pick_resultset_statement(sql_block: str) -> str | None:
    """
    From any (possibly commented / multi-statement) SQL, return the LAST
    statement that starts with SELECT or WITH.
    """
    if not sql_block:
        return None
    no_comments = strip_sql_comments(sql_block).strip()
    candidates = [s for s in split_sql_statements(no_comments)
                  if re.match(r"(?is)^\s*(with|select)\b", s)]
    if not candidates:
        return None
    return candidates[-1].rstrip(";")


# ==========================================
# Knowledge-graph–aware normalizers (light)
# ==========================================

def _basename(name: str) -> str:
    return (name or "").replace('"', "").split(".")[-1].lower()

def soft_remap_known_tables_to_graph(sql: str, gs: GraphStore) -> str:
    """
    Remap FROM/JOIN object names to the actual FQNs in the graph by basename,
    e.g., synthetic_store.regional_managers -> ref.regional_managers.
    """
    idx = { _basename(t): t for t in gs.tables() }

    def repl(m):
        kw, obj = m.group("kw"), m.group("obj")
        base = _basename(obj)
        if base in idx:
            return f"{kw} {idx[base]}"
        return m.group(0)

    pattern = r'(?i)(?P<kw>\bFROM\b|\bJOIN\b)\s+(?P<obj>(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)'
    return re.sub(pattern, repl, sql)

_ALIAS_RX = re.compile(r'(?is)\b(from|join)\s+(?P<obj>(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)\s+(?:as\s+)?(?P<alias>\w+)\b')

def _alias_to_table(sql: str) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for mo in _ALIAS_RX.finditer(sql):
        m[mo.group("alias")] = mo.group("obj").replace('"', "")
    return m

def _resolve_fqn(gs: GraphStore, obj: str) -> str | None:
    obj_clean = obj.replace('"', "")
    if obj_clean in gs.tables():
        return obj_clean
    base = _basename(obj_clean)
    for t in gs.tables():
        if _basename(t) == base:
            return t
    return None

def _known_cols_for(gs: GraphStore, fq_table: str) -> List[str]:
    return [c.split(".")[-1] for c in gs.columns(fq_table)]

def _quote_ident(name: str) -> str:
    # Simpler and 100% safe; avoids f-string escape edge cases.
    return '"' + name.strip().strip('"') + '"'

def auto_quote_mixed_case_after_aliases(sql: str, gs: GraphStore) -> str:
    """
    Quote alias.col -> alias."Col" for simple word columns known in the KG.
    Helps when the model writes t.Sales instead of t."Sales".
    """
    alias_map = _alias_to_table(sql)
    out = sql
    for alias, obj in alias_map.items():
        fq = _resolve_fqn(gs, obj)
        if not fq:
            continue
        cols = _known_cols_for(gs, fq)
        simple_cols = [c for c in cols if re.fullmatch(r"\w+", c)]
        for col in simple_cols:
            pattern = re.compile(rf'\b{re.escape(alias)}\.{re.escape(col)}\b', flags=re.IGNORECASE)
            out = pattern.sub(f'{alias}.{_quote_ident(col)}', out)
    return out

_AGG_INNER_RX = re.compile(r'\b(count|sum|avg|min|max)\s*\(\s*(?P<arg>(?:"[^"]+"|\w+\.\w+|\w+|\*))\s*\)', re.IGNORECASE)

def auto_quote_aggregate_inners(sql: str, gs: GraphStore) -> str:
    """
    Quote inners of aggregates: SUM(Sales) -> SUM("Sales"); COUNT(t.Sales) -> COUNT(t."Sales").
    """
    alias_map = _alias_to_table(sql)

    def repl(m):
        func = m.group(1).upper()
        arg = m.group('arg').strip()
        if arg == '*':
            return f"{func}(*)"
        if arg.startswith('"'):
            return f"{func}({arg})"
        if '.' in arg:
            al, col = arg.split('.', 1)
            fq = _resolve_fqn(gs, alias_map.get(al) or al)
            if fq:
                for real in _known_cols_for(gs, fq):
                    if real.lower() == col.lower():
                        return f'{func}({al}.{_quote_ident(real)})'
            return f"{func}({arg})"
        # bare col: quote if unambiguous across involved tables
        matches = []
        for t in _involved_tables(sql, gs):
            for real in _known_cols_for(gs, t):
                if real.lower() == arg.lower():
                    matches.append(real)
        if len(matches) == 1:
            return f'{func}({_quote_ident(matches[0])})'
        return f"{func}({arg})"

    return _AGG_INNER_RX.sub(repl, sql)

def _involved_tables(sql: str, gs: GraphStore) -> List[str]:
    fqn = set()
    for mo in re.finditer(r'(?is)\b(from|join)\s+(?P<obj>(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)', sql):
        rf = _resolve_fqn(gs, mo.group('obj'))
        if rf:
            fqn.add(rf)
    return list(fqn)

def auto_quote_bare_known_cols(sql: str, gs: GraphStore) -> str:
    """
    Quote bare word columns when they map unambiguously to a single casing
    across the involved tables.
    """
    tables = _involved_tables(sql, gs)
    if not tables:
        return sql
    col_to_casings: Dict[str, set] = {}
    for fq in tables:
        for c in _known_cols_for(gs, fq):
            col_to_casings.setdefault(c.lower(), set()).add(c)
    # only those with a single canonical casing
    candidates = { next(iter(v)) for v in col_to_casings.values() if len(v) == 1 }

    parts = re.split(r'("(?:""|[^"])*"|\'(?:\'\'|[^\'])*\')', sql)
    def fix_span(span: str) -> str:
        if not span or span.startswith(("'", '"')):
            return span
        for col in sorted(candidates, key=len, reverse=True):
            span = re.sub(rf'(?<!["\w\.]){re.escape(col)}(?!["\w\.])', _quote_ident(col), span, flags=re.IGNORECASE)
        return span

    return "".join(fix_span(p) for p in parts)

def normalize_sql_with_graph(sql: str, gs: GraphStore) -> str:
    """
    Light normalization only: table-name remap + quoting aids.
    The LLM remains responsible for correctness and full SQL construction.
    """
    sql = soft_remap_known_tables_to_graph(sql, gs)
    sql = auto_quote_mixed_case_after_aliases(sql, gs)
    sql = auto_quote_aggregate_inners(sql, gs)
    sql = auto_quote_bare_known_cols(sql, gs)
    return sql

def repair_from_hint(sql: str, err_msg: str) -> str | None:
    """
    Use Postgres' 'Perhaps you meant to reference the column "tbl.Col"' hint
    to quote that column everywhere (alias.col and bare col).
    """
    m = re.search(r'Perhaps you meant to reference the column "([^"]+)\.([^"]+)"', err_msg)
    if not m:
        return None
    tbl_or_alias, col = m.group(1), m.group(2)
    out = re.sub(rf'\b{re.escape(tbl_or_alias)}\.{re.escape(col)}\b', f'{tbl_or_alias}.{_quote_ident(col)}', sql)
    out = re.sub(rf'(?<!["\w\.]){re.escape(col)}(?!["\w\.])', _quote_ident(col), out)
    return out


# =======================
# Data Access Agent (LLM)
# =======================

SYSTEM_MESSAGE = "You are a PostgreSQL 14+ specialist. Return only a single SQL query in a ```sql fenced block```."

class DataAccessAgent:
    def __init__(self, model_id: str, host: str | None = None):
        self.gs = GraphStore().load()
        self.agent = Agent(
            model=Gemini(id=model_id),
            system_message=SYSTEM_MESSAGE,
            markdown=True,
        )

    def answer(self, user_question: str):
        # 0) Load KG JSON (verbatim) for the prompt
        kg_path = os.getenv("KG_JSON_PATH", "./graph/store_graph.json")
        try:
            with open(kg_path, "r", encoding="utf-8") as f:
                kg_json_text = f.read()
        except Exception:
            kg_json_text = "{}"

        # 1) Ask the LLM for a single executable Postgres query (SQL-only contract)
        prompt = f"""
You are a **PostgreSQL 14+** expert and query planner.

You are given a **Knowledge Graph JSON** describing the ONLY allowed schemas/tables/columns/joins.
Use ONLY objects present in this KG. Do NOT invent columns or tables.

### Knowledge Graph (JSON)
{kg_json_text}

### Hard rules
- Engine: PostgreSQL only.
- Use schema-qualified table names exactly as in the KG (e.g., sales.orders, ref.regional_managers).
- If a column has uppercase or special characters (e.g., "Sales", "Profit", "Order ID", "State/Province"),
  you MUST double-quote it everywhere you reference it.
- If you use aggregates, include ALL non-aggregate selected columns in GROUP BY.
- Produce exactly ONE statement that RETURNS ROWS (SELECT or WITH ... SELECT). No DDL/DML. No multi-statements.
- Output ONLY the SQL inside one ```sql fenced block. No prose.

### User Question
{user_question}
"""
        raw = self.agent.run(prompt).content or ""
        sql_block = extract_sql_block(raw)
        stmt = pick_resultset_statement(sql_block or raw)

        if not stmt:
            # Ask to rewrite as a single result-set query
            repair_prompt = f"""
Rewrite the previous output as ONE PostgreSQL query that RETURNS ROWS (SELECT or WITH ... SELECT),
following the Hard rules above. Output ONLY the SQL in a single ```sql fenced block.

Previous output:
{raw}

User question:
{user_question}

(Use the same Knowledge Graph JSON as above.)
"""
            raw2 = self.agent.run(repair_prompt).content or ""
            sql_block = extract_sql_block(raw2)
            stmt = pick_resultset_statement(sql_block or raw2)

        if not stmt:
            return "Planner did not produce a SELECT/WITH statement."

        # 2) Light normalization (table FQNs + identifier quoting aids)
        stmt = normalize_sql_with_graph(stmt, self.gs)

        # 3) Execute. If it fails or doesn't return rows, self-repair ONCE with exact error + KG.
        try:
            df = run_sql("postgres", stmt)
        except (ProgrammingError, ResourceClosedError) as e:
            # Try a HINT-based fix first (UndefinedColumn with hint)
            fixed = repair_from_hint(stmt, str(e))
            if fixed:
                try:
                    df = run_sql("postgres", fixed)
                    stmt = fixed
                except Exception as e2:
                    # Fall back to LLM self-repair
                    repair2 = f"""
Your previous SQL failed on PostgreSQL. Using ONLY the KG above,
produce a corrected SINGLE SELECT/WITH query that RETURNS ROWS.

--- ERROR (verbatim) ---
{e2}

--- FAILED SQL ---
{fixed}

User question:
{user_question}

Return ONLY the SQL in one ```sql fenced block.
"""
                    raw3 = self.agent.run(repair2).content or ""
                    sql3 = extract_sql_block(raw3) or raw3
                    stmt2 = pick_resultset_statement(sql3)
                    if not stmt2:
                        return f"SQL execution failed:\n{e2}\n\nSQL:\n{fixed}"
                    df = run_sql("postgres", stmt2)
                    stmt = stmt2
            else:
                # Ask the model to self-repair with the exact error + KG
                repair2 = f"""
Your previous SQL failed on PostgreSQL. Using ONLY the KG above,
produce a corrected SINGLE SELECT/WITH query that RETURNS ROWS.

--- ERROR (verbatim) ---
{e}

--- FAILED SQL ---
{stmt}

User question:
{user_question}

Return ONLY the SQL in one ```sql fenced block.
"""
                raw3 = self.agent.run(repair2).content or ""
                sql3 = extract_sql_block(raw3) or raw3
                stmt2 = pick_resultset_statement(sql3)
                if not stmt2:
                    return f"SQL execution failed:\n{e}\n\nSQL:\n{stmt}"
                df = run_sql("postgres", stmt2)
                stmt = stmt2
        except Exception as e:
            return f"SQL execution failed:\n{e}\n\nSQL:\n{stmt}"

        if df is None or df.empty:
            return "No rows."
        return summarize(df, limit=25)
