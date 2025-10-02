from __future__ import annotations
import json
import networkx as nx
from pathlib import Path
from typing import Dict, Any, List

GRAPH_PATH = Path(__file__).parent / "store_graph.json"

class GraphStore:
    def __init__(self, path=GRAPH_PATH):
        self.path = Path(path)
        self.G = nx.DiGraph()

    def load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.G = nx.node_link_graph(data, directed=True)
        return self

    def save(self):
        data = nx.node_link_data(self.G)
        self.path.write_text(json.dumps(data, indent=2))

    def tables(self) -> List[str]:
        return [n for n, d in self.G.nodes(data=True) if d.get("type") == "table"]

    def columns(self, table: str) -> List[str]:
        return [n for n, d in self.G.nodes(data=True)
                if d.get("type") == "column" and d.get("table")==table]

    def resolve_table_location(self, table: str) -> Dict[str, Any]:
        return self.G.nodes[table]["location"]

    def join_path(self, table_a: str, table_b: str):
        # returns simple path across join edges if exists
        try:
            return nx.shortest_path(self.G, table_a, table_b, weight=None)
        except nx.NetworkXNoPath:
            return None

