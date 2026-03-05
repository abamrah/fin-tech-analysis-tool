/**
 * Navbar — Shared sidebar navigation component.
 * Injected into all authenticated pages.
 */

function renderNavbar() {
    const user = getUser();
    const currentPage = window.location.pathname.split('/').pop() || 'dashboard.html';

    const navItems = [
        { href: 'dashboard.html', icon: '📊', label: 'Dashboard' },
        { href: 'insights.html', icon: '🧠', label: 'AI Insights' },
        { href: 'transactions.html', icon: '💳', label: 'Transactions' },
        { href: 'upload.html', icon: '📄', label: 'Upload Statement' },
        { href: 'planner.html', icon: '📋', label: 'Financial Planner' },
        { href: 'budget.html', icon: '💰', label: 'Budget' },
        { href: 'goals.html', icon: '🎯', label: 'Savings Goals' },
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

    // Ensure main content has the app-container layout
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
        const container = document.createElement('div');
        container.className = 'app-container';
        document.body.removeChild(sidebar);
        document.body.removeChild(mainContent);
        container.appendChild(sidebar);
        container.appendChild(mainContent);
        document.body.appendChild(container);
    }
}

// Auto-render navbar on authenticated pages
document.addEventListener('DOMContentLoaded', () => {
    // Don't render navbar on auth pages
    const page = window.location.pathname.split('/').pop();
    if (page === 'login.html' || page === 'register.html' || page === 'index.html' || page === '') {
        return;
    }
    if (!isAuthenticated()) {
        window.location.href = '/login.html';
        return;
    }
    renderNavbar();
});
