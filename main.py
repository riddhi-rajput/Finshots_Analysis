import requests
from bs4 import BeautifulSoup
import time
import csv
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

BASE = "https://finshots.in"
SITEMAP_URL = "https://finshots.in/sitemap.xml"
HEADERS = {"User-Agent": "Riddhi-Scraper/1.0 (+https://github.com/riddhi-rajput)"}  # be polite
RATE_LIMIT = 1.0  # seconds between requests, increase if needed

def allowed_by_robots(base_url, path="/"):
    try:
        robots = requests.get(urljoin(base_url, "robots.txt"), headers=HEADERS, timeout=10)
        if robots.status_code != 200:
            return True
        txt = robots.text
        for line in txt.splitlines():
            line = line.strip()
            if line.startswith("Disallow:"):
                dis = line.split(":", 1)[1].strip()
                if dis and path.startswith(dis):
                    return False
        return True
    except Exception:
        return True

def parse_sitemap(sitemap_url):
    try:
        resp = requests.get(sitemap_url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        urls = []
        for child in root:
            tag = child.tag.lower()
            if tag.endswith('sitemap'):
                loc = child.find('{*}loc')
                if loc is not None:
                    sub = loc.text.strip()
                    urls += parse_sitemap(sub)
            elif tag.endswith('url'):
                loc = child.find('{*}loc')
                if loc is not None:
                    urls.append(loc.text.strip())
        return urls
    except Exception as e:
        print("Could not parse sitemap:", e)
        return []

def extract_article_links_from_archive(archive_url, max_pages=10):
    links = set()
    for page in range(1, max_pages + 1):
        candidates = [
            f"{archive_url}/page/{page}/",
            f"{archive_url}?page={page}",
            f"{archive_url}/{page}/"
        ]
        found = False
        for url in candidates:
            try:
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a['href']
                    if '/archive/' in href or '/20' in href or '/202' in href:
                        links.add(urljoin(BASE, href))
                found = True
                time.sleep(RATE_LIMIT)
            except Exception:
                continue
        if not found:
            break
    return list(links)

def clean_url(u):
    return u.split('?')[0].rstrip('/')

def fetch_article(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        title_tag = soup.find('h1')
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            meta = soup.find('meta', property='og:title') or soup.find('meta', attrs={'name':'title'})
            title = meta['content'].strip() if meta and meta.get('content') else None
        date = None
        time_tag = soup.find('time')
        if time_tag and time_tag.get('datetime'):
            date = time_tag.get('datetime').split('T')[0]
        elif time_tag:
            date = time_tag.get_text(strip=True)
        else:
            meta_time = soup.find('meta', property='article:published_time')
            if meta_time and meta_time.get('content'):
                date = meta_time['content'].split('T')[0]
            else:
                header = soup.find(['header', 'article'])
                if header:
                    txt = header.get_text(" ", strip=True)
                    import re
                    m = re.search(r'\d{1,2}\s+\w+\s+\d{4}', txt)
                    if m:
                        date = m.group(0)
        theme = None
        if time_tag:
            sib = time_tag.find_next_sibling()
            if sib:
                theme_text = sib.get_text(strip=True)
                if theme_text:
                    theme = theme_text
        if not theme:
            candidate = soup.select_one('.post-meta a') or soup.select_one('.byline a') or soup.select_one('.meta a')
            if candidate:
                theme = candidate.get_text(strip=True)
        if not theme:
            hdr = soup.find('header')
            if hdr:
                for span in hdr.find_all(['span','a','div']):
                    text = span.get_text(strip=True)
                    if text and text.isupper() and len(text) < 30:
                        theme = text
                        break
        return {"url": url, "title": title, "date": date, "theme": theme}
    except Exception as e:
        print("Error fetching", url, e)
        return None

def is_likely_article(url):
    u = url.lower()
    bad_prefixes = ['mailto:', 'tel:', 'javascript:']
    if any(u.startswith(bp) for bp in bad_prefixes):
        return False
    non_article_keywords = ['/page/', '/tag/', '/category/', '/author/', '/wp-json', '/feed', '/p/']
    if any(k in u for k in non_article_keywords):
        return False
    if '/archive/' in u or '/20' in u or '-' in urlparse(u).path:
        return True
    return False

def main(n_articles=150, out_csv="finshots_articles.csv"):
    if not allowed_by_robots(BASE, "/"):
        print("robots.txt disallows crawling the site. Exiting.")
        return

    urls = parse_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} urls in sitemap.")
    urls = [clean_url(u) for u in urls if u.startswith("http")]
    candidate_urls = [u for u in urls if is_likely_article(u)]
    print("Candidate articles from sitemap:", len(candidate_urls))
    if len(candidate_urls) < n_articles:
        print("Sitemap had few articles; trying archive crawl fallback...")
        archive_links = extract_article_links_from_archive(BASE + "/archive", max_pages=30)  
        archive_links = [clean_url(u) for u in archive_links]
        print("Archive-derived links:", len(archive_links))
        for u in archive_links:
            if u not in candidate_urls and is_likely_article(u):
                candidate_urls.append(u)
    seen = set()
    final_candidates = []
    for u in candidate_urls:
        if u not in seen:
            seen.add(u)
            final_candidates.append(u)
    print("Total candidate article URLs:", len(final_candidates))
    final_candidates = final_candidates[: max(n_articles*4, n_articles)]  
    results = []
    for idx, url in enumerate(final_candidates):
        if len(results) >= n_articles:
            break
        print(f"[{idx+1}/{len(final_candidates)}] Fetching:", url)
        if not allowed_by_robots(BASE, urlparse(url).path):
            print("Skipping due to robots:", url)
            continue
        article = fetch_article(url)
        time.sleep(RATE_LIMIT)
        if article and article.get('title'):
            results.append(article)

    print(f"Collected {len(results)} articles (requested {n_articles}).")

    keys = ['url','date','theme','title']
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in results[:n_articles]:
            writer.writerow({k: (r.get(k) or "") for k in keys})

    print("Saved", out_csv)

if __name__ == "__main__":
    main(n_articles=150, out_csv="finshots_articles.csv")