import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, List, Tuple, Set

from config import SEED_URLS, ALLOWED_PREFIX


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "mini-search-bot/0.1 (educational project)"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.text


def extract_text_and_links(url: str, html: str) -> Tuple[str, List[str]]:
    soup = BeautifulSoup(html, "lxml")

    # заголовок 
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else url

    # основной текст: все <p> на странице
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n".join(paragraphs)

    # все ссылки вида /wiki/...
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(url, href)
        if full.startswith(ALLOWED_PREFIX):
            links.append(full.split("#")[0])  # урезаем якори

    return title, text, links


def crawl() -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    seen: Set[str] = set()

    for url in SEED_URLS:
        if url in seen:
            continue
        seen.add(url)
        print(f"[crawl] Fetch {url}")
        html = fetch_html(url)
        title, text, links = extract_text_and_links(url, html)
        result[url] = {
            "title": title,
            "text": text,
            "links": links,
        }
        time.sleep(1) 
    return result