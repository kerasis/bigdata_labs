# search.py
import math
import sqlite3
from collections import defaultdict
from typing import Dict, List, Tuple

from config import DB_PATH


def load_index():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM documents")
    doc_count = cur.fetchone()[0]

    index: Dict[str, Dict[int, int]] = defaultdict(dict)
    cur.execute("SELECT term, doc_id, tf FROM terms")
    for term, doc_id, tf in cur.fetchall():
        index[term][doc_id] = tf

    # длины документов (сумма tf)
    doc_len: Dict[int, int] = defaultdict(int)
    for term, postings in index.items():
        for doc_id, tf in postings.items():
            doc_len[doc_id] += tf

    # doc_id (title, url)
    cur.execute("SELECT id, title, url FROM documents")
    docs = {row[0]: {"title": row[1], "url": row[2]} for row in cur.fetchall()}

    conn.close()
    return index, doc_len, doc_count, docs


def compute_idf(index, doc_count):
    idf: Dict[str, float] = {}
    for term, postings in index.items():
        df = len(postings)
        if df == 0:
            continue
        idf[term] = math.log(doc_count / df)
    return idf


def normalize_query(query: str) -> List[str]:
    import re
    WORD_RE = re.compile(r"[а-яёa-z0-9]+", re.IGNORECASE)
    return [m.group(0).lower() for m in WORD_RE.finditer(query)]


def search_taat(query: str, top_k: int = 5) -> List[Tuple[int, float]]:
    index, doc_len, doc_count, docs = load_index()
    idf = compute_idf(index, doc_count)
    terms = normalize_query(query)

    scores: Dict[int, float] = defaultdict(float)

    # Term-at-a-time: идём по термам, обновляем accumulator
    for term in terms:
        postings = index.get(term, {})
        w_t = idf.get(term, 0.0)
        for doc_id, tf in postings.items():
            scores[doc_id] += (tf / doc_len[doc_id]) * w_t

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return ranked, docs


def search_daat(query: str, top_k: int = 5) -> List[Tuple[int, float]]:
    index, doc_len, doc_count, docs = load_index()
    idf = compute_idf(index, doc_count)
    terms = normalize_query(query)

    # строим отсортированные по doc_id списки
    postings_lists: List[List[Tuple[int, int]]] = []
    for term in terms:
        postings = index.get(term, {})
        if postings:
            postings_lists.append(sorted(postings.items(), key=lambda x: x[0]))

    if not postings_lists:
        return [], docs

    cursors = [0] * len(postings_lists)
    scores: Dict[int, float] = defaultdict(float)

    while True:
        current_docs = []
        for i, plist in enumerate(postings_lists):
            if cursors[i] < len(plist):
                current_docs.append(plist[cursors[i]][0])

        if not current_docs:
            break

        d = min(current_docs)

        for i, plist in enumerate(postings_lists):
            if cursors[i] >= len(plist):
                continue
            doc_id, tf = plist[cursors[i]]
            if doc_id == d:
                term = terms[i]
                w_t = idf.get(term, 0.0)
                scores[d] += (tf / doc_len[d]) * w_t
                cursors[i] += 1
      
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return ranked, docs