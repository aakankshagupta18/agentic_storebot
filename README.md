# StoreBot: Agno + Ollama multi-agent chatbot

## Prereqs
- Python 3.10+
- MySQL + Postgres running locally
- Ollama running locally with a model (e.g. llama3.2)

```bash
# 1) Environment
cp .env.example .env
# edit DB creds & SMTP if needed

# 2) Python deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3) Databases
# create DBs / users as in your .env, then load:
python db/load_excel_to_dbs.py

# 4) Build Knowledge Graph
python graph/build_graph.py

# 5) Ollama
ollama serve &
ollama pull tinyllama

# 6) Run app
uvicorn app:app --reload --port 8000

