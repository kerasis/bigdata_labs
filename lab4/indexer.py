import re
import sqlite3
from collections import Counter
from typing import Dict

from config import DB_PATH
from crawler import crawl


WORD_RE = re.compile(r"[а-яёa-z0-9]+", re.IGNORECASE)


def tokenize(text: str):
    for match in WORD_RE.finditer(text.lower()):
        yield match.group(0)


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript(
        """
        DROP TABLE IF EXISTS documents;
        DROP TABLE IF EXISTS terms;
        DROP TABLE IF EXISTS links;

        CREATE TABLE documents (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            url  TEXT UNIQUE,
            title TEXT
        );

        CREATE TABLE terms (
            term   TEXT,
            doc_id INTEGER,
            tf     INTEGER,
            PRIMARY KEY (term, doc_id)
        );

        CREATE TABLE links (
            src_id INTEGER,
            dst_id INTEGER,
            PRIMARY KEY (src_id, dst_id)
        );
        """
    )

    conn.commit()
    conn.close()


def index():
    pages = crawl()  # url -> {title, text, links}

    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1) вставляем документы
    url_to_id: Dict[str, int] = {}
    for url, data in pages.items():
        cur.execute(
            "INSERT INTO documents(url, title) VALUES (?, ?)",
            (url, data["title"]),
        )
        url_to_id[url] = cur.lastrowid

    conn.commit()

    # 2) термы
    for url, data in pages.items():
        doc_id = url_to_id[url]
        tokens = list(tokenize(data["text"]))
        freqs = Counter(tokens)
        for term, tf in freqs.items():
            cur.execute(
                "INSERT INTO terms(term, doc_id, tf) VALUES (?, ?, ?)",
                (term, doc_id, tf),
            )

    conn.commit()

    # 3) ссылки только между известными документами
    for url, data in pages.items():
        src_id = url_to_id[url]
        for dst_url in data["links"]:
            if dst_url in url_to_id:
                dst_id = url_to_id[dst_url]
                cur.execute(
                    "INSERT OR IGNORE INTO links(src_id, dst_id) VALUES (?, ?)",
                    (src_id, dst_id),
                )

    conn.commit()
    conn.close()
    print("[index] Indexed", len(pages), "documents")
