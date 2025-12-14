import sqlite3
from typing import Dict, List

from config import DB_PATH, PAGERANK_DAMPING, PAGERANK_ITERS
from pregel_lib import pregel, Graph


def load_graph() -> Graph:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT id FROM documents")
    nodes = [row[0] for row in cur.fetchall()]

    graph: Graph = {n: [] for n in nodes}
    cur.execute("SELECT src_id, dst_id FROM links")
    for src, dst in cur.fetchall():
        if src in graph:
            graph[src].append(dst)

    conn.close()
    return graph


def pagerank_pregel():
    graph = load_graph()
    nodes = list(graph.keys())
    N = len(nodes)

    initial_state: Dict[int, float] = {v: 1.0 / N for v in nodes}
    damping = PAGERANK_DAMPING
    base = (1.0 - damping) / N

    def superstep_fn(v, current_value, incoming_messages, neighbors):
        # incoming_messages пустой - вернём то, что нужно разослать
        if not incoming_messages:
            return current_value
        # используем сумму входящих
        s = sum(incoming_messages)
        return base + damping * s

    ranks = pregel(graph, initial_state, superstep_fn, PAGERANK_ITERS)
    return ranks
