# agents/json_utils.py
import json, re

class JsonExtractError(Exception):
    pass

def loads_relaxed(s: str):
    """Extract the first JSON object from a string (handles ```json fences and stray text)."""
    if not s or not isinstance(s, str):
        raise JsonExtractError("empty model response")
    # capture inside ```json ... ``` or plain ``` ... ```
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, re.IGNORECASE | re.DOTALL)
    if m:
        s = m.group(1).strip()
    # if still has prefix (e.g., 'plan: { ... }'), cut to first '{' and last '}'
    if not s.lstrip().startswith("{"):
        start = s.find("{")
        if start == -1:
            raise JsonExtractError("no JSON object found")
        s = s[start:]
        # optionally trim trailing junk after the matching brace block
        depth = 0; end = None
        for i, ch in enumerate(s):
            if ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end:
            s = s[:end]
    return json.loads(s)
