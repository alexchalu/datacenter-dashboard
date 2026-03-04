# DC Pulse — Datacenter & CoreWeave Intelligence Dashboard

A real-time intelligence dashboard tracking US datacenter construction news and CoreWeave updates.

## Features

- 🏗 **US Datacenter News** — Construction, expansion, and infrastructure stories
- ⚡ **CoreWeave Tracker** — Latest news and postings
- 🎯 **Smart Importance Scoring** — Stories auto-ranked by significance
- 🔍 **Search & Filter** — Find what matters fast
- 📱 **Responsive** — Works on desktop and mobile
- 🔄 **Auto-Updated** — GitHub Actions fetches fresh news every 6 hours

## How It Works

1. GitHub Actions runs `scripts/fetch_news.py` every 6 hours
2. The script fetches Google News RSS feeds for datacenter and CoreWeave topics
3. Stories are deduplicated, scored for importance, and tagged
4. JSON data is committed to the `data/` directory
5. The static site (GitHub Pages) reads the JSON and renders the dashboard

## Live Site

👉 **[https://alexchalu.github.io/datacenter-dashboard](https://alexchalu.github.io/datacenter-dashboard)**

## Tech Stack

- Pure HTML/CSS/JavaScript (no frameworks, no build step)
- Python 3 for news fetching (stdlib only, no dependencies)
- GitHub Actions for automation
- GitHub Pages for hosting

**Cost: $0**

---

Built with 🔧 by Rando
