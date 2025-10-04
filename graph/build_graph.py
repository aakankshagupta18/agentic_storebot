from graph_store import GraphStore

# Build nodes for tables + columns and encode where each table lives
# We deliberately do not encode PK/FK constraints—only "joinable by" hints.

TABLES = {
  # table_name : {location, schema (pg/mysql), columns [...]}
  "sales.orders": {
    "location": {"engine": "postgres", "schema": "sales", "table": "orders"},
    "columns": ["Row ID","Order ID","Order Date","Ship Date","Ship Mode","Customer ID","Customer Name",
                "Segment","Country/Region","City","State/Province","Postal Code","Region",
                "Product ID","Category","Sub-Category","Product Name", "Sales", "Quantity", "Discount", "Profit"]
  },
  "ref.returns": {
    "location": {"engine": "postgres", "schema": "ref", "table": "returns"},
    "columns": ["Returned","ID"]
  },
  "regional_managers": {
    "location": {"engine": "postgres", "schema": "ref", "table": "regional_managers"},
    "columns": ["Regional Manager","Regions"]
  },
  "state_managers": {
    "location": {"engine": "postgres", "schema": "ref", "table": "state_managers"},
    "columns": ["State/Province","Manager"]
  },
  "segment_managers": {
    "location": {"engine": "postgres", "schema": "ref", "table": "segment_managers"},
    "columns": ["Segment","Manager"]
  },
  "category_managers": {
    "location": {"engine": "postgres", "schema": "ref", "table": "category_managers"},
    "columns": ["Category","Manager"]
  },
  "customer_succces_managers": {
    "location": {"engine": "postgres", "schema": "ref", "table": "customer_succces_managers"},
    "columns": ["Regions","Manager"]
  },
}

# join hints (undirected semantics modeled as two directed edges)
JOINS = [
  # Orders joinable with Returns via Order ID ~ ID
  ("sales.orders", "ref.returns", {"on": [["Order ID","ID"]]}),
  ("ref.returns", "sales.orders", {"on": [["ID","Order ID"]]}),

  # Orders enrich with region/state/segment/category managers
  ("sales.orders", "regional_managers", {"on": [["Region","Regions"]]}),
  ("sales.orders", "state_managers", {"on": [["State/Province","State/Province"]]}),
  ("sales.orders", "segment_managers", {"on": [["Segment","Segment"]]}),
  ("sales.orders", "category_managers", {"on": [["Category","Category"]]}),
]

def main():
    gs = GraphStore().load()
    G = gs.G
    G.clear()

    for tname, meta in TABLES.items():
        G.add_node(tname, type="table", location=meta["location"])
        for col in meta["columns"]:
            G.add_node(f"{tname}.{col}", type="column", table=tname)
            G.add_edge(tname, f"{tname}.{col}", type="has_column")

    for a,b,attrs in JOINS:
        G.add_edge(a, b, type="join", **attrs)

    gs.save()
    print(f"Wrote graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

if __name__ == "__main__":
    main()

