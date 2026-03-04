// ============================================
// Infrastructure Intel — Dashboard App
// ============================================

(function () {
    'use strict';

    // State
    let datacenterArticles = [];
    let coreweaveArticles = [];
    let activeTab = 'datacenter';
    let activeFilter = 'all';
    let searchQuery = '';

    // DOM
    const tabs = document.querySelectorAll('.tab');
    const panels = document.querySelectorAll('.panel');
    const filterBtns = document.querySelectorAll('.filter-btn');
    const searchInput = document.getElementById('searchInput');
    const dcCount = document.getElementById('dcCount');
    const cwCount = document.getElementById('cwCount');
    const lastUpdated = document.getElementById('lastUpdated');
    const dcGrid = document.getElementById('datacenterNews');
    const cwGrid = document.getElementById('coreweaveNews');

    // Initialize
    async function init() {
        setupEventListeners();
        await loadData();
    }

    function setupEventListeners() {
        tabs.forEach(tab => {
            tab.addEventListener('click', () => switchTab(tab.dataset.tab));
        });

        filterBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                filterBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                activeFilter = btn.dataset.value;
                renderActivePanel();
            });
        });

        searchInput.addEventListener('input', debounce((e) => {
            searchQuery = e.target.value.toLowerCase();
            renderActivePanel();
        }, 200));
    }

    function switchTab(tab) {
        activeTab = tab;
        tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
        panels.forEach(p => p.classList.toggle('active', p.id === `${tab}-panel`));
        renderActivePanel();
    }

    async function loadData() {
        try {
            const response = await fetch('data/news.json');
            if (!response.ok) throw new Error('Failed to load news data');
            const data = await response.json();

            datacenterArticles = (data.datacenter || []).sort((a, b) =>
                importanceWeight(b.importance) - importanceWeight(a.importance)
            );
            coreweaveArticles = (data.coreweave || []).sort((a, b) =>
                importanceWeight(b.importance) - importanceWeight(a.importance)
            );

            dcCount.textContent = datacenterArticles.length;
            cwCount.textContent = coreweaveArticles.length;

            if (data.updated) {
                const date = new Date(data.updated);
                lastUpdated.textContent = `Updated ${formatRelativeTime(date)}`;
            }

            renderActivePanel();
        } catch (err) {
            console.error('Error loading data:', err);
            dcGrid.innerHTML = renderEmptyState('📡', 'No data yet', 'News data will appear after the first fetch cycle.');
            cwGrid.innerHTML = renderEmptyState('📡', 'No data yet', 'News data will appear after the first fetch cycle.');
        }
    }

    function renderActivePanel() {
        if (activeTab === 'datacenter') {
            renderArticles(dcGrid, filterArticles(datacenterArticles));
        } else {
            renderArticles(cwGrid, filterArticles(coreweaveArticles));
        }
    }

    function filterArticles(articles) {
        return articles.filter(a => {
            if (activeFilter !== 'all' && a.importance !== activeFilter) return false;
            if (searchQuery) {
                const haystack = `${a.title} ${a.snippet} ${a.source} ${(a.tags || []).join(' ')}`.toLowerCase();
                if (!haystack.includes(searchQuery)) return false;
            }
            return true;
        });
    }

    function renderArticles(container, articles) {
        if (articles.length === 0) {
            container.innerHTML = renderEmptyState(
                '🔍',
                'No articles found',
                searchQuery ? 'Try adjusting your search or filters.' : 'Check back soon — news updates every 4 hours.'
            );
            return;
        }

        container.innerHTML = articles.map((article, i) => `
            <a href="${escapeHtml(article.url)}" target="_blank" rel="noopener noreferrer"
               class="news-card" style="animation-delay: ${i * 0.02}s">
                <div class="card-importance-bar ${article.importance}"></div>
                <div class="card-body">
                    <div class="card-meta">
                        <span class="card-source">${escapeHtml(article.source)}</span>
                        <span class="card-date">${escapeHtml(article.date || '')}</span>
                        <span class="card-importance-badge ${article.importance}">${article.importance}</span>
                    </div>
                    <div class="card-title">${escapeHtml(article.title)}</div>
                    <div class="card-snippet">${escapeHtml(article.snippet)}</div>
                    ${article.tags && article.tags.length ? `
                        <div class="card-tags">
                            ${article.tags.map(t => `<span class="card-tag">${escapeHtml(t)}</span>`).join('')}
                        </div>
                    ` : ''}
                </div>
                <div class="card-arrow">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M5 12h14M12 5l7 7-7 7"/>
                    </svg>
                </div>
            </a>
        `).join('');
    }

    function renderEmptyState(icon, title, desc) {
        return `
            <div class="empty-state">
                <div class="empty-icon">${icon}</div>
                <h3>${title}</h3>
                <p>${desc}</p>
            </div>
        `;
    }

    // Helpers
    function importanceWeight(level) {
        return { critical: 4, high: 3, medium: 2, low: 1 }[level] || 0;
    }

    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function formatRelativeTime(date) {
        const now = new Date();
        const diff = Math.floor((now - date) / 1000);
        if (diff < 60) return 'just now';
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    }

    function debounce(fn, ms) {
        let timer;
        return function (...args) {
            clearTimeout(timer);
            timer = setTimeout(() => fn.apply(this, args), ms);
        };
    }

    // Boot
    document.addEventListener('DOMContentLoaded', init);
})();
