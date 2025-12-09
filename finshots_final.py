#!/usr/bin/env python3
"""
finshots_pipeline_simple.py
Lightweight enrichment pipeline that uses only Python standard library.
Compatible with Python 3.14 (no external packages required).

Input: finshots_articles.csv  (must contain 'url' column)
Output: finshots_enriched_simple.csv
"""

import csv
import time
import re
import sys
import html
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from collections import Counter, defaultdict

INPUT_CSV = "finshots_articles.csv"
OUTPUT_CSV = "finshots_enriched_simple.csv"
CHECKPOINT_CSV = "finshots_checkpoint_simple.csv"
SLEEP = 1.0  
USER_AGENT = "Mozilla/5.0 (compatible; FinshotsSimpleBot/1.0; +www.yourname@xyz.com)"

STOPWORDS = {
    "the","and","a","an","in","on","for","to","of","is","are","was","were",
    "by","with","as","that","this","it","from","at","be","has","have","had",
    "but","or","not","we","they","he","she","I","you","your","our","us","their",
    "which","will","its","can","about","more","after","also","one","all","new"
}

POS_WORDS = {"good","great","positive","gain","gains","rise","up","beat","beats","profit","growth","improve","surge","strong","optimistic","bull"}
NEG_WORDS = {"bad","worse","negative","loss","losses","fall","down","miss","missed","drop","weak","decline","declining","bear","pessimistic","crash"}

def fetch_html(url, timeout=20):
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            try:
                text = raw.decode(resp.headers.get_content_charset() or "utf-8", errors="replace")
            except Exception:
                text = raw.decode("utf-8", errors="replace")
            return text
    except (HTTPError, URLError) as e:
        print(f"[fetch error] {url} -> {e}", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"[fetch exception] {url} -> {e}", file=sys.stderr)
        return ""

def extract_text_from_html(html_text):
    if not html_text:
        return ""
    html_text = html.unescape(html_text)

    m = re.search(r"<article\b[^>]*>(.*?)</article>", html_text, flags=re.I | re.S)
    if m:
        content = m.group(1)
    else:
        m2 = re.search(r"<main\b[^>]*>(.*?)</main>", html_text, flags=re.I | re.S)
        if m2:
            content = m2.group(1)
        else:
            ps = re.findall(r"<p\b[^>]*>(.*?)</p>", html_text, flags=re.I | re.S)
            content = "\n".join(ps)

    text = re.sub(r"<script.*?>.*?</script>", " ", content, flags=re.S | re.I)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)

    text = re.sub(r"\s+", " ", text).strip()
    return text

def tokenize_words(text):
    words = re.findall(r"[A-Za-z0-9']{2,}", text.lower())
    return words

def sentence_count(text):
    sents = re.split(r"[.!?]\s+", text.strip())
    sents = [s for s in sents if len(s.strip())>0]
    return max(1, len(sents))

def estimate_syllables(word):
    word = word.lower()
    word = re.sub(r'[^a-z]', '', word)
    if not word:
        return 0
    vowels = "aeiouy"
    sylls = 0
    prev_vowel = False
    for ch in word:
        is_v = ch in vowels
        if is_v and not prev_vowel:
            sylls += 1
        prev_vowel = is_v
    if word.endswith("e") and sylls > 1:
        sylls -= 1
    if sylls == 0:
        sylls = 1
    return sylls

def flesch_reading_ease(text):
    words = tokenize_words(text)
    W = max(1, len(words))
    S = sentence_count(text)
    syllables = sum(estimate_syllables(w) for w in words)
    score = 206.835 - 1.015 * (W / S) - 84.6 * (syllables / W)
    return round(score, 2)

def simple_sentiment(text):
    words = tokenize_words(text)
    pos = sum(1 for w in words if w in POS_WORDS)
    neg = sum(1 for w in words if w in NEG_WORDS)
    if pos + neg == 0:
        return 0.0
    return round((pos - neg) / (pos + neg), 3)

def top_keywords(text, n=8):
    words = tokenize_words(text)
    tokens = [w for w in words if w not in STOPWORDS and len(w) > 2 and not w.isdigit()]
    if not tokens:
        return ""
    counts = Counter(tokens)
    top = [w for w, _ in counts.most_common(n)]
    return ", ".join(top)

def extract_entities_simple(text, min_len=2, top_n=6):
    matches = re.findall(r"\b([A-Z][a-z0-9]{1,}\s+[A-Z][a-z0-9]{1,}(?:\s+[A-Z][a-z0-9]{1,})*)\b", text)
    cleaned = []
    for m in matches:
        words = m.split()
        if len(words) >= min_len and any(len(w) > 2 for w in words):
            cleaned.append(m.strip())
    if not cleaned:
        return ""
    freq = Counter(cleaned)
    top = [w for w, _ in freq.most_common(top_n)]
    return ", ".join(top)

def main():
    try:
        with open(INPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []
    except FileNotFoundError:
        print(f"Input file {INPUT_CSV} not found. Place your CSV in the script folder.", file=sys.stderr)
        return

    new_cols = ["content_simple", "word_count", "reading_time_min", "flesch_simple", "sentiment_simple", "top_keywords", "entities_simple"]
    for c in new_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    for idx, row in enumerate(rows):
        if row.get("content_simple"):
            print(f"[{idx+1}/{len(rows)}] already processed, skipping")
            continue

        url = row.get("url", "").strip()
        if not url:
            print(f"[{idx+1}/{len(rows)}] no url, skipping")
            for c in new_cols:
                row[c] = ""
            continue

        print(f"[{idx+1}/{len(rows)}] fetching {url}")
        html_text = fetch_html(url)
        content = extract_text_from_html(html_text)
        if not content:
            print(f"  -> empty content extracted")
        wc = len(tokenize_words(content))
        reading_min = round(wc / 200.0, 2)
        flesch = flesch_reading_ease(content) if wc > 0 else ""
        sent = simple_sentiment(content) if wc > 0 else 0.0
        keys = top_keywords(content)
        ents = extract_entities_simple(html_text)  

        row["content_simple"] = content
        row["word_count"] = wc
        row["reading_time_min"] = reading_min
        row["flesch_simple"] = flesch
        row["sentiment_simple"] = sent
        row["top_keywords"] = keys
        row["entities_simple"] = ents

        if (idx + 1) % 10 == 0:
            print(f"  -> checkpoint save at row {idx+1}")
            with open(CHECKPOINT_CSV, "w", newline="", encoding="utf-8") as outf:
                writer = csv.DictWriter(outf, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        time.sleep(SLEEP)

    print("Saving final enriched CSV:", OUTPUT_CSV)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Done.")

if __name__ == "__main__":
    main()