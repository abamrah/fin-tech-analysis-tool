/**
 * Navbar — Shared sidebar + mobile bottom-tab navigation.
 * Injected into all authenticated pages.
 */

function renderNavbar() {
    const user = getUser();
    const currentPage = window.location.pathname.split('/').pop() || 'home.html';

    const navItems = [
        { href: 'hub.html', icon: '🏆', label: 'Hub' },
        { href: 'dashboard.html', icon: '📊', label: 'Dashboard' },
        { href: 'review.html', icon: '📈', label: 'Monthly Review' },
        { href: 'insights.html', icon: '🧠', label: 'AI Insights' },
        { href: 'transactions.html', icon: '💳', label: 'Transactions' },
        { href: 'upload.html', icon: '📄', label: 'Upload Statement' },
        { href: 'planner.html', icon: '📋', label: 'Financial Planner' },
        { href: 'planning.html', icon: '🧮', label: 'Planning Suite' },
        { href: 'budget.html', icon: '💰', label: 'Budget' },
        { href: 'goals.html', icon: '🎯', label: 'Savings Goals' },
        { href: 'flashcards.html', icon: '🃏', label: 'Flashcards' },
        { href: 'advisor.html', icon: '🤖', label: 'AI Advisor' },
    ];

    const sidebar = document.createElement('nav');
    sidebar.className = 'sidebar';
    sidebar.innerHTML = `
        <div class="sidebar-brand">
            <h2><span class="brand-icon">💹</span> FinTech Engine</h2>
        </div>
        <div class="sidebar-nav">
            ${navItems.map(item => `
                <a href="${item.href}" class="nav-item ${currentPage === item.href ? 'active' : ''}">
                    <span class="nav-icon">${item.icon}</span>
                    <span>${item.label}</span>
                </a>
            `).join('')}
        </div>
        <div class="sidebar-footer">
            <div class="user-info">
                <div class="user-name">${user ? user.full_name : 'User'}</div>
                <div class="text-xs text-muted">${user ? user.email : ''}</div>
            </div>
            <button class="btn btn-secondary btn-sm btn-block mt-2" onclick="apiLogout()" style="margin-top:0.75rem">
                🚪 Logout
            </button>
        </div>
    `;

    document.body.prepend(sidebar);

    // Mobile hamburger menu toggle
    const toggle = document.createElement('button');
    toggle.className = 'mobile-menu-toggle';
    toggle.innerHTML = '☰';
    toggle.setAttribute('aria-label', 'Toggle navigation');
    document.body.prepend(toggle);

    // Sidebar overlay (for mobile)
    const overlay = document.createElement('div');
    overlay.className = 'sidebar-overlay';
    document.body.prepend(overlay);

    toggle.addEventListener('click', () => {
        sidebar.classList.toggle('open');
        overlay.classList.toggle('open');
    });
    overlay.addEventListener('click', () => {
        sidebar.classList.remove('open');
        overlay.classList.remove('open');
    });
    // Close sidebar on nav-item click (mobile)
    sidebar.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', () => {
            sidebar.classList.remove('open');
            overlay.classList.remove('open');
        });
    });

    // Ensure main content has the app-container layout
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
        const container = document.createElement('div');
        container.className = 'app-container';
        document.body.removeChild(sidebar);
        document.body.removeChild(mainContent);
        // Keep toggle and overlay outside the container for fixed positioning
        container.appendChild(sidebar);
        container.appendChild(mainContent);
        document.body.appendChild(container);
    }

    // ─── Mobile Bottom Tab Bar ───────────────────
    renderBottomTabs(currentPage);
}

/**
 * Render a 5-tab bottom navigation bar for mobile.
 * Shown on all pages except home.html (which has its own).
 */
function renderBottomTabs(currentPage) {
    if (currentPage === 'home.html') return; // home has built-in tabs

    const tabs = [
        { href: 'home.html', icon: '🏠', label: 'Home' },
        { href: 'dashboard.html', icon: '📊', label: 'Dashboard' },
        { href: 'flashcards.html', icon: '📚', label: 'Learn' },
        { href: 'hub.html', icon: '🏆', label: 'Hub' },
    ];

    const bar = document.createElement('nav');
    bar.className = 'bottom-tabs-global';
    bar.innerHTML = tabs.map(t =>
        `<a href="${t.href}" class="btab ${currentPage === t.href ? 'active' : ''}">
            <span class="btab-icon">${t.icon}</span>
            <span class="btab-label">${t.label}</span>
        </a>`
    ).join('') + `
        <a href="#" class="btab" id="btab-more" onclick="toggleGlobalMore(event)">
            <span class="btab-icon">☰</span>
            <span class="btab-label">More</span>
        </a>
    `;
    document.body.appendChild(bar);

    // Add padding to main-content so bottom tabs don't cover content
    const mc = document.querySelector('.main-content');
    if (mc) mc.style.paddingBottom = '4rem';
}

/* Global More menu for bottom tabs */
let _globalMoreOpen = false;
function toggleGlobalMore(e) {
    e.preventDefault();
    _globalMoreOpen = !_globalMoreOpen;
    let menu = document.getElementById('global-more-menu');
    if (!menu) {
        menu = document.createElement('div');
        menu.id = 'global-more-menu';
        menu.className = 'global-more-menu';
        menu.innerHTML = `
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem;">
                <a href="/transactions.html" class="gmore-link">💳 Transactions</a>
                <a href="/upload.html" class="gmore-link">📄 Upload</a>
                <a href="/planner.html" class="gmore-link">📋 Planner</a>
                <a href="/planning.html" class="gmore-link">🧮 Planning Suite</a>
                <a href="/budget.html" class="gmore-link">💰 Budgets</a>
                <a href="/goals.html" class="gmore-link">🎯 Goals</a>
                <a href="/review.html" class="gmore-link">📈 Review</a>
                <a href="/insights.html" class="gmore-link">🧠 Insights</a>
                <a href="/advisor.html" class="gmore-link">🤖 AI Advisor</a>
            </div>
            <button onclick="apiLogout()" class="gmore-link" style="width:100%;margin-top:0.5rem;color:var(--error);border-color:rgba(239,68,68,0.3);">🚪 Logout</button>
        `;
        document.body.appendChild(menu);
    }
    menu.style.display = _globalMoreOpen ? 'block' : 'none';
    document.getElementById('btab-more').classList.toggle('active', _globalMoreOpen);
}

// Close global more on outside tap
document.addEventListener('click', (e) => {
    if (_globalMoreOpen && !e.target.closest('#global-more-menu') && !e.target.closest('#btab-more')) {
        _globalMoreOpen = false;
        const m = document.getElementById('global-more-menu');
        if (m) m.style.display = 'none';
        const btn = document.getElementById('btab-more');
        if (btn) btn.classList.remove('active');
    }
});

// Auto-render navbar on authenticated pages
document.addEventListener('DOMContentLoaded', () => {
    // Don't render navbar on auth pages or home (home has its own layout)
    const page = window.location.pathname.split('/').pop();
    if (page === 'login.html' || page === 'register.html' || page === 'index.html' || page === '' || page === 'home.html') {
        return;
    }
    if (!isAuthenticated()) {
        window.location.href = '/login.html';
        return;
    }
    renderNavbar();
});
