# tools/safety.py
import os

REQUIRE_CONFIRM = os.getenv("REQUIRE_WRITE_CONFIRMATION","true").lower()=="true"

def guard_write(op_text: str, confirmed: bool):
    if REQUIRE_CONFIRM and not confirmed:
        return (False, f"WRITE BLOCKED: {op_text}\nReply with 'confirm' to proceed.")
    return (True, "OK")

