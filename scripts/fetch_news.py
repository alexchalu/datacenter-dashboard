#!/usr/bin/env python3
"""
Fetch datacenter and CoreWeave news from multiple sources:
- Google News RSS (multiple query variations)
- Bing News RSS
- Industry RSS feeds (Data Center Dynamics, Data Center Knowledge, etc.)
- Reddit RSS (r/datacenter, r/coreweave, etc.)
- Company blogs (CoreWeave blog)
- Tech news RSS filtered for relevant topics

Outputs JSON files for the dashboard to consume.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from email.utils import parsedate_to_datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

# ============ Feed Configuration ============
# Each feed: (url, type)
# type: 'google' | 'bing' | 'standard' | 'reddit' | 'atom'

FEEDS = {
    'datacenter': [
        # --- Google News (varied queries for breadth) ---
        ('https://news.google.com/rss/search?q=US+datacenter+construction+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=data+center+expansion+United+States+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=hyperscale+data+center+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=datacenter+campus+gigawatt+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22data+center%22+%22under+construction%22+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22data+center%22+billion+investment+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22data+center%22+permit+OR+approved+OR+zoning+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22data+center%22+power+OR+energy+OR+grid+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=AI+infrastructure+datacenter+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22colocation%22+OR+%22colo%22+data+center+expansion+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=Equinix+OR+Digital+Realty+OR+QTS+OR+Vantage+data+center+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=Microsoft+OR+Google+OR+Amazon+OR+Meta+data+center+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),

        # --- Bing News RSS ---
        ('https://www.bing.com/news/search?q=datacenter+construction+US&format=rss', 'bing'),
        ('https://www.bing.com/news/search?q=data+center+expansion+hyperscale&format=rss', 'bing'),
        ('https://www.bing.com/news/search?q=data+center+infrastructure+investment&format=rss', 'bing'),

        # --- Industry RSS Feeds ---
        ('https://www.datacenterdynamics.com/en/rss/', 'standard'),
        ('https://www.datacenterknowledge.com/rss.xml', 'standard'),
        ('https://datacenterfrontier.com/feed/', 'standard'),
        ('https://www.datacenters.com/feed', 'standard'),
        ('https://www.capacitymedia.com/rss', 'standard'),
        ('https://www.broadgroup.com/feed', 'standard'),
        ('https://siliconangle.com/category/datacenter/feed/', 'standard'),

        # --- Reddit ---
        ('https://www.reddit.com/r/datacenter/hot.rss', 'reddit'),
        ('https://www.reddit.com/r/datacenters/hot.rss', 'reddit'),
        ('https://www.reddit.com/search.rss?q=datacenter+construction&t=month&sort=relevance', 'reddit'),
    ],

    'coreweave': [
        # --- Google News (many query variations) ---
        ('https://news.google.com/rss/search?q=CoreWeave+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22CoreWeave%22+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+GPU+cloud+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+data+center+when:30d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+funding+OR+valuation+OR+IPO+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+partnership+OR+contract+OR+deal+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+expansion+OR+campus+OR+facility+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+NVIDIA+OR+Microsoft+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),

        # --- Bing News ---
        ('https://www.bing.com/news/search?q=CoreWeave&format=rss', 'bing'),
        ('https://www.bing.com/news/search?q=%22CoreWeave%22+GPU+cloud&format=rss', 'bing'),
        ('https://www.bing.com/news/search?q=CoreWeave+datacenter+infrastructure&format=rss', 'bing'),

        # --- CoreWeave Blog ---
        ('https://www.coreweave.com/blog/rss.xml', 'atom'),
        ('https://www.coreweave.com/blog/rss', 'atom'),

        # --- Reddit ---
        ('https://www.reddit.com/r/coreweave/hot.rss', 'reddit'),
        ('https://www.reddit.com/search.rss?q=CoreWeave&t=month&sort=relevance', 'reddit'),

        # --- Tech news filtered ---
        ('https://news.google.com/rss/search?q=CoreWeave+site:techcrunch.com+OR+site:theverge.com+OR+site:arstechnica.com+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+site:reuters.com+OR+site:bloomberg.com+OR+site:cnbc.com+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+site:wsj.com+OR+site:ft.com+OR+site:forbes.com+when:60d&hl=en-US&gl=US&ceid=US:en', 'google'),
    ],

    'social': [
        ('https://news.google.com/rss/search?q=datacenter+%22tweet%22+OR+%22twitter%22+OR+%22x.com%22+OR+%22viral%22+when:14d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=CoreWeave+%22tweet%22+OR+%22twitter%22+OR+%22x.com%22+OR+%22viral%22+when:14d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=datacenter+%22trending%22+OR+%22social+media%22+OR+%22went+viral%22+when:14d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://news.google.com/rss/search?q=%22data+center%22+%22Elon+Musk%22+OR+%22Sam+Altman%22+OR+%22Jensen+Huang%22+when:14d&hl=en-US&gl=US&ceid=US:en', 'google'),
        ('https://www.reddit.com/r/datacenter/top.rss?t=week', 'reddit'),
        ('https://www.reddit.com/search.rss?q=CoreWeave+OR+datacenter&t=week&sort=top', 'reddit'),
    ]
}

# ============ Importance Scoring ============
IMPORTANCE = {
    'critical': ['billion', '$1b', '$2b', '$5b', '$10b', '$15b', '$20b', 'hyperscal', 'gigawatt',
                 'national security', 'executive order', 'ipo', 'acquisition', 'merger', 'antitrust',
                 's-1', 'public offering', 'stock market'],
    'high': ['million', '$100m', '$200m', '$500m', '$800m', 'new campus', 'groundbreaking',
             'partnership', 'expansion', 'contract award', 'approved', 'permit', 'nvidia',
             'gpu cluster', 'ai infrastructure', 'power purchase', 'renewable energy deal',
             'revenue', 'earnings', 'valuation', 'series', 'funding round'],
    'medium': ['construction', 'development', 'planned', 'announced', 'progress', 'hiring',
               'power', 'energy', 'cooling', 'fiber', 'network', 'capacity', 'cloud',
               'gpu', 'inference', 'training', 'cluster']
}


def score_importance(title, snippet=''):
    text = f'{title} {snippet}'.lower()
    for level, keywords in IMPORTANCE.items():
        if any(k in text for k in keywords):
            return level
    return 'low'


def detect_tags(title, snippet=''):
    text = f'{title} {snippet}'.lower()
    tags = []
    if re.search(r'construct|build|broke ground|groundbreak', text):
        tags.append('construction')
    if re.search(r'invest|fund|financ|billion|million|\$', text):
        tags.append('investment')
    if re.search(r'expan|grow|new campus|new site|scale', text):
        tags.append('expansion')
    if re.search(r'polic|regulat|permit|govern|legislat|bill|act\b', text):
        tags.append('policy')
    if 'coreweave' in text:
        tags.append('coreweave')
    if re.search(r'tweet|twitter|𝕏|x\.com|viral|thread|trending|social media', text):
        tags.append('social')
    return tags[:3]


def extract_source_google(html_desc):
    """Extract source name from Google News RSS description HTML."""
    match = re.search(r'<font[^>]*>([^<]+)</font>', html_desc or '')
    if match:
        return match.group(1).strip()
    return None


def clean_title(title):
    """Remove source suffix from Google News titles."""
    parts = title.rsplit(' - ', 1)
    return parts[0].strip() if len(parts) > 1 else title.strip()


def parse_date(date_str):
    """Try multiple date formats."""
    if not date_str:
        return datetime.now(timezone.utc).isoformat()
    
    # Try RFC 2822 first (standard RSS)
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        pass
    
    # Try ISO format (Atom feeds)
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00')).isoformat()
    except Exception:
        pass
    
    # Fallback
    return datetime.now(timezone.utc).isoformat()


def fetch_feed(url, feed_type='standard'):
    """Fetch and parse an RSS/Atom feed."""
    items = []
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()

        root = ET.fromstring(data)

        # Determine namespace
        ns = {}
        atom_ns = '{http://www.w3.org/2005/Atom}'
        dc_ns = '{http://purl.org/dc/elements/1.1/}'
        media_ns = '{http://search.yahoo.com/mrss/}'

        # Try RSS items first, then Atom entries
        rss_items = root.findall('.//item')
        atom_entries = root.findall(f'.//{atom_ns}entry') if not rss_items else []

        for entry in rss_items:
            item = parse_rss_item(entry, feed_type, url, dc_ns)
            if item:
                items.append(item)

        for entry in atom_entries:
            item = parse_atom_entry(entry, atom_ns, feed_type, url)
            if item:
                items.append(item)

    except Exception as e:
        print(f'  ⚠ {feed_type}: {url[:80]}... → {e}', file=sys.stderr)

    return items


def parse_rss_item(item, feed_type, feed_url, dc_ns=''):
    """Parse a standard RSS <item> element."""
    title_el = item.find('title')
    link_el = item.find('link')
    desc_el = item.find('description')
    pub_el = item.find('pubDate')
    dc_date = item.find(f'{dc_ns}date') if dc_ns else None
    creator_el = item.find(f'{dc_ns}creator') if dc_ns else None

    if title_el is None or not (title_el.text or '').strip():
        return None

    raw_title = unescape(title_el.text or '')
    link = (link_el.text or '').strip() if link_el is not None else ''

    # Source detection
    source = None
    if feed_type == 'google':
        source = extract_source_google(desc_el.text if desc_el is not None else '')
        if not source and ' - ' in raw_title:
            source = raw_title.rsplit(' - ', 1)[1].strip()
        raw_title = clean_title(raw_title)
    elif feed_type == 'bing':
        source_el = item.find('{http://www.bing.com/schema/news}Source')
        if source_el is not None:
            source = source_el.text
        if not source and ' - ' in raw_title:
            source = raw_title.rsplit(' - ', 1)[1].strip()
        raw_title = clean_title(raw_title)
    elif feed_type == 'reddit':
        source = 'Reddit'
        # Clean Reddit titles
        raw_title = raw_title.strip()
    else:
        # Standard feed - derive source from URL
        source = _source_from_url(feed_url)

    if creator_el is not None and creator_el.text and not source:
        source = creator_el.text.strip()

    # Date
    date_str = None
    if pub_el is not None and pub_el.text:
        date_str = pub_el.text
    elif dc_date is not None and dc_date.text:
        date_str = dc_date.text
    published = parse_date(date_str)

    # Snippet
    snippet = ''
    if desc_el is not None and desc_el.text:
        snippet = re.sub(r'<[^>]+>', '', unescape(desc_el.text)).strip()
        # For Reddit, the description often has "submitted by..." - clean it
        if feed_type == 'reddit':
            snippet = re.sub(r'submitted by.*$', '', snippet, flags=re.IGNORECASE).strip()
            snippet = re.sub(r'\[link\].*$', '', snippet, flags=re.IGNORECASE).strip()
            snippet = re.sub(r'&#x200B;', '', snippet).strip()
        snippet = snippet[:400]

    return {
        'title': raw_title,
        'link': link,
        'source': source or 'Unknown',
        'published': published,
        'snippet': snippet,
        'importance': score_importance(raw_title, snippet),
        'tags': detect_tags(raw_title, snippet)
    }


def parse_atom_entry(entry, ns, feed_type, feed_url):
    """Parse an Atom <entry> element."""
    title_el = entry.find(f'{ns}title')
    link_el = entry.find(f'{ns}link')
    summary_el = entry.find(f'{ns}summary') or entry.find(f'{ns}content')
    updated_el = entry.find(f'{ns}updated') or entry.find(f'{ns}published')
    author_el = entry.find(f'{ns}author/{ns}name')

    if title_el is None or not (title_el.text or '').strip():
        return None

    title = unescape(title_el.text or '').strip()
    link = ''
    if link_el is not None:
        link = link_el.get('href', '') or (link_el.text or '')

    source = _source_from_url(feed_url)
    if author_el is not None and author_el.text:
        source = author_el.text.strip()

    published = parse_date(updated_el.text if updated_el is not None else None)

    snippet = ''
    if summary_el is not None and summary_el.text:
        snippet = re.sub(r'<[^>]+>', '', unescape(summary_el.text)).strip()[:400]

    return {
        'title': title,
        'link': link,
        'source': source or 'Unknown',
        'published': published,
        'snippet': snippet,
        'importance': score_importance(title, snippet),
        'tags': detect_tags(title, snippet)
    }


def _source_from_url(url):
    """Derive a readable source name from a feed URL."""
    source_map = {
        'datacenterdynamics.com': 'Data Center Dynamics',
        'datacenterknowledge.com': 'Data Center Knowledge',
        'datacenterfrontier.com': 'Data Center Frontier',
        'datacenters.com': 'Datacenters.com',
        'capacitymedia.com': 'Capacity Media',
        'broadgroup.com': 'BroadGroup',
        'siliconangle.com': 'SiliconANGLE',
        'coreweave.com': 'CoreWeave Blog',
        'reddit.com': 'Reddit',
        'bing.com': 'Bing News',
        'google.com': 'Google News',
    }
    for domain, name in source_map.items():
        if domain in url:
            return name
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ''
        return host.replace('www.', '').split('.')[0].title()
    except Exception:
        return 'Unknown'


def deduplicate(items):
    """Remove duplicates based on normalized title similarity."""
    seen = {}
    unique = []
    for item in items:
        # Normalize: lowercase, remove non-alnum, take first 80 chars
        key = re.sub(r'[^a-z0-9]', '', item['title'].lower())[:80]
        if len(key) < 10:
            # Very short title - use link as key too
            key += re.sub(r'[^a-z0-9]', '', item['link'].lower())[:40]
        if key not in seen:
            seen[key] = True
            unique.append(item)
    return unique


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    total_all = 0
    for category, feeds in FEEDS.items():
        print(f'\n{"="*50}')
        print(f'Fetching {category.upper()} news ({len(feeds)} feeds)...')
        print(f'{"="*50}')

        all_items = []
        for url, feed_type in feeds:
            items = fetch_feed(url, feed_type)
            if items:
                print(f'  ✓ {feed_type:8s} → {len(items):3d} items  ({url[:70]}...)')
            else:
                print(f'  ✗ {feed_type:8s} →   0 items  ({url[:70]}...)')
            all_items.extend(items)

        # Deduplicate
        unique_items = deduplicate(all_items)

        # Sort: importance first, then date descending within same importance
        imp_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        unique_items.sort(key=lambda x: (
            imp_order.get(x['importance'], 3),
            -(datetime.fromisoformat(x['published'].replace('Z', '+00:00')).timestamp()
              if x['published'] else 0)
        ))

        print(f'\n  📊 {len(all_items)} raw → {len(unique_items)} unique stories for {category}')
        total_all += len(unique_items)

        output_path = os.path.join(DATA_DIR, f'{category}.json')
        with open(output_path, 'w') as f:
            json.dump(unique_items, f, indent=2)

    # Write meta
    meta = {
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'totalStories': total_all,
        'feeds': {k: len(v) for k, v in FEEDS.items()}
    }
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)

    print(f'\n✅ Done! {total_all} total unique stories across all categories.')


if __name__ == '__main__':
    main()
