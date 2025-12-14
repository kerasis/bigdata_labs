import sqlite3
from collections import defaultdict
from typing import Dict, List, Tuple

from config import DB_PATH, PAGERANK_DAMPING, PAGERANK_ITERS


def load_graph() -> Dict[int, List[int]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT id FROM documents")
    nodes = [row[0] for row in cur.fetchall()]

    graph: Dict[int, List[int]] = {n: [] for n in nodes}
    cur.execute("SELECT src_id, dst_id FROM links")
    for src, dst in cur.fetchall():
        if src in graph:
            graph[src].append(dst)

    conn.close()
    return graph


def map_phase(ranks: Dict[int, float], graph: Dict[int, List[int]]) -> List[Tuple[int, float]]:
    emissions: List[Tuple[int, float]] = []

    for src, outlinks in graph.items():
        rank = ranks[src]
        if outlinks:
            share = rank / len(outlinks)
            for dst in outlinks:
                emissions.append((dst, share))

        # гарантируем наличие ключа для страниц без входящих ссылок
        emissions.append((src, 0.0))

    return emissions


def reduce_phase(emissions: List[Tuple[int, float]], nodes, damping: float) -> Dict[int, float]:
    N = len(nodes)
    base = (1.0 - damping) / N
    new_ranks: Dict[int, float] = {n: base for n in nodes}

    for dst, contrib in emissions:
        new_ranks[dst] += damping * contrib

    return new_ranks


def pagerank_mapreduce():
    graph = load_graph()
    nodes = list(graph.keys())
    N = len(nodes)

    ranks: Dict[int, float] = {n: 1.0 / N for n in nodes}

    for i in range(PAGERANK_ITERS):
        emissions = map_phase(ranks, graph)
        ranks = reduce_phase(emissions, nodes, PAGERANK_DAMPING)

    return ranks
