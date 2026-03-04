#!/usr/bin/env python3
"""
Fetch datacenter and CoreWeave news from Google News RSS feeds.
Outputs JSON files for the dashboard to consume.
"""

import json
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from email.utils import parsedate_to_datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

# Google News RSS search queries
FEEDS = {
    'datacenter': [
        'https://news.google.com/rss/search?q=US+datacenter+construction+when:30d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=data+center+expansion+United+States+when:30d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=hyperscale+datacenter+US+construction+when:30d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=datacenter+campus+gigawatt+US+when:30d&hl=en-US&gl=US&ceid=US:en',
    ],
    'coreweave': [
        'https://news.google.com/rss/search?q=CoreWeave+when:30d&hl=en-US&gl=US&ceid=US:en',
    ],
    'social': [
        # Viral tweets & social posts about datacenters and CoreWeave
        'https://news.google.com/rss/search?q=datacenter+%22tweet%22+OR+%22twitter%22+OR+%22x.com%22+OR+%22viral%22+when:14d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=CoreWeave+%22tweet%22+OR+%22twitter%22+OR+%22x.com%22+OR+%22viral%22+when:14d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=datacenter+%22trending%22+OR+%22social+media%22+OR+%22went+viral%22+when:14d&hl=en-US&gl=US&ceid=US:en',
        'https://news.google.com/rss/search?q=%22data+center%22+%22Elon+Musk%22+OR+%22Sam+Altman%22+OR+%22Jensen+Huang%22+when:14d&hl=en-US&gl=US&ceid=US:en',
    ]
}

# Importance keywords
IMPORTANCE = {
    'critical': ['billion', '$1b', '$2b', '$5b', '$10b', '$15b', '$20b', 'hyperscal', 'gigawatt',
                 'national security', 'executive order', 'ipo', 'acquisition', 'merger', 'antitrust'],
    'high': ['million', '$100m', '$200m', '$500m', '$800m', 'new campus', 'groundbreaking',
             'partnership', 'expansion', 'contract award', 'approved', 'permit', 'nvidia',
             'gpu cluster', 'ai infrastructure', 'power purchase', 'renewable energy deal'],
    'medium': ['construction', 'development', 'planned', 'announced', 'progress', 'hiring',
               'power', 'energy', 'cooling', 'fiber', 'network', 'capacity']
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

def extract_source(html_desc):
    """Extract source name from Google News RSS description HTML."""
    match = re.search(r'<font[^>]*>([^<]+)</font>', html_desc or '')
    if match:
        return match.group(1).strip()
    return 'Unknown'

def clean_title(title):
    """Remove source suffix from Google News titles."""
    # Google News often appends " - Source Name" at the end
    parts = title.rsplit(' - ', 1)
    return parts[0].strip() if len(parts) > 1 else title.strip()

def fetch_rss(url):
    """Fetch and parse an RSS feed."""
    items = []
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; DCPulse/1.0; +https://github.com/alexchalu/datacenter-dashboard)'
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        
        root = ET.fromstring(data)
        
        for item in root.findall('.//item'):
            title_el = item.find('title')
            link_el = item.find('link')
            desc_el = item.find('description')
            pub_el = item.find('pubDate')
            
            if title_el is None or link_el is None:
                continue
            
            raw_title = unescape(title_el.text or '')
            source = extract_source(desc_el.text if desc_el is not None else '')
            title = clean_title(raw_title)
            
            # If source wasn't in description, try the title suffix
            if source == 'Unknown' and ' - ' in raw_title:
                source = raw_title.rsplit(' - ', 1)[1].strip()
            
            # Parse date
            published = None
            if pub_el is not None and pub_el.text:
                try:
                    published = parsedate_to_datetime(pub_el.text).isoformat()
                except Exception:
                    published = datetime.now(timezone.utc).isoformat()
            else:
                published = datetime.now(timezone.utc).isoformat()
            
            # Extract snippet from description (strip HTML)
            snippet = ''
            if desc_el is not None and desc_el.text:
                snippet = re.sub(r'<[^>]+>', '', unescape(desc_el.text)).strip()
                snippet = snippet[:300]
            
            items.append({
                'title': title,
                'link': link_el.text or '',
                'source': source,
                'published': published,
                'snippet': snippet,
                'importance': score_importance(title, snippet),
                'tags': detect_tags(title, snippet)
            })
    except Exception as e:
        print(f'  Warning: Failed to fetch {url}: {e}', file=sys.stderr)
    
    return items

def deduplicate(items):
    """Remove duplicates based on title similarity."""
    seen = {}
    unique = []
    for item in items:
        # Normalize title for dedup
        key = re.sub(r'[^a-z0-9]', '', item['title'].lower())[:60]
        if key not in seen:
            seen[key] = True
            unique.append(item)
    return unique

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    for category, urls in FEEDS.items():
        print(f'Fetching {category} news...')
        all_items = []
        for url in urls:
            items = fetch_rss(url)
            print(f'  Got {len(items)} items from feed')
            all_items.extend(items)
        
        # Deduplicate
        unique_items = deduplicate(all_items)
        
        # Sort by importance then date
        imp_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        unique_items.sort(key=lambda x: (imp_order.get(x['importance'], 3), x['published']), reverse=False)
        # Re-sort: importance ascending (critical first), then date descending within same importance
        unique_items.sort(key=lambda x: (imp_order.get(x['importance'], 3), ''), reverse=False)
        # Actually let's do a proper two-key sort
        unique_items.sort(key=lambda x: (imp_order.get(x['importance'], 3), -(datetime.fromisoformat(x['published'].replace('Z', '+00:00')).timestamp() if x['published'] else 0)))

        print(f'  {len(unique_items)} unique stories for {category}')
        
        output_path = os.path.join(DATA_DIR, f'{category}.json')
        with open(output_path, 'w') as f:
            json.dump(unique_items, f, indent=2)
    
    # Write meta
    meta = {
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'feeds': {k: len(v) for k, v in FEEDS.items()}
    }
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    
    print('Done!')

if __name__ == '__main__':
    main()
