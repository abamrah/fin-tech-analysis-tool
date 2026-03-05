/**
 * API Client — Centralized fetch wrapper with JWT authentication.
 */

const API_BASE = '/api';

/** Get stored JWT token */
function getToken() {
    return localStorage.getItem('auth_token');
}

/** Get stored user info */
function getUser() {
    const data = localStorage.getItem('auth_user');
    return data ? JSON.parse(data) : null;
}

/** Store auth data after login/register */
function setAuth(data) {
    localStorage.setItem('auth_token', data.access_token);
    localStorage.setItem('auth_user', JSON.stringify({
        user_id: data.user_id,
        email: data.email,
        full_name: data.full_name,
    }));
}

/** Clear auth data (logout) */
function clearAuth() {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_user');
}

/** Check if user is authenticated */
function isAuthenticated() {
    return !!getToken();
}

/** Redirect to login if not authenticated */
function requireAuth() {
    if (!isAuthenticated()) {
        window.location.href = '/login.html';
        return false;
    }
    return true;
}

/**
 * Authenticated fetch wrapper.
 * Automatically adds Authorization header and handles 401 redirects.
 */
async function fetchAPI(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : `${API_BASE}${endpoint}`;

    const headers = {
        ...(options.headers || {}),
    };

    // Add auth header if token exists (skip for auth endpoints)
    const token = getToken();
    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }

    // Add Content-Type for JSON bodies
    if (options.body && !(options.body instanceof FormData)) {
        headers['Content-Type'] = 'application/json';
        if (typeof options.body === 'object') {
            options.body = JSON.stringify(options.body);
        }
    }

    try {
        const response = await fetch(url, { ...options, headers });

        // Handle 401 — redirect to login
        if (response.status === 401) {
            clearAuth();
            window.location.href = '/login.html';
            return null;
        }

        // Parse response
        const contentType = response.headers.get('content-type');
        let data;
        if (contentType && contentType.includes('application/json')) {
            data = await response.json();
        } else {
            data = await response.text();
        }

        if (!response.ok) {
            const message = data?.detail || data || `Request failed (${response.status})`;
            throw new Error(message);
        }

        return data;
    } catch (error) {
        if (error.message === 'Failed to fetch') {
            throw new Error('Network error — please check your connection');
        }
        throw error;
    }
}

// ─── Auth API ──────────────────────────────

async function apiLogin(email, password) {
    const data = await fetchAPI('/auth/login', {
        method: 'POST',
        body: { email, password },
    });
    if (data) setAuth(data);
    return data;
}

async function apiRegister(email, password, full_name) {
    const data = await fetchAPI('/auth/register', {
        method: 'POST',
        body: { email, password, full_name },
    });
    if (data) setAuth(data);
    return data;
}

function apiLogout() {
    clearAuth();
    window.location.href = '/login.html';
}

// ─── Upload API ────────────────────────────

async function apiUploadStatement(file, accountType, institutionName) {
    const formData = new FormData();
    formData.append('file', file);
    if (accountType) formData.append('account_type', accountType);
    if (institutionName) formData.append('institution_name', institutionName);

    return fetchAPI('/upload-statement', {
        method: 'POST',
        body: formData,
    });
}

async function apiGetStatementStatus(statementId) {
    return fetchAPI(`/upload-statement/${statementId}/status`);
}

// ─── Transaction API ───────────────────────

async function apiGetTransactions(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/transactions?${query}`);
}

async function apiGetTransactionSummary(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/transactions/summary?${query}`);
}

async function apiGetTransactionFilters() {
    return fetchAPI('/transactions/filters');
}

async function apiUpdateTransaction(id, data) {
    return fetchAPI(`/transactions/${id}`, { method: 'PATCH', body: data });
}

async function apiAssignPlannerCategories() {
    return fetchAPI('/transactions/assign-planner-categories', { method: 'POST' });
}

// ─── Dashboard API ─────────────────────────

async function apiGetOverview(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/overview?${query}`);
}

async function apiGetCategories(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/categories?${query}`);
}

async function apiGetMerchants(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/merchants?${query}`);
}

async function apiGetRecurring(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/recurring?${query}`);
}

async function apiGetAnomalies(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/anomalies?${query}`);
}

async function apiGetMonthlySummary(params = {}) {
    const query = new URLSearchParams(params).toString();
    return fetchAPI(`/dashboard/monthly-summary?${query}`);
}

// ─── Budget API ────────────────────────────

async function apiGetBudgets(month) {
    const params = month ? `?month=${month}` : '';
    return fetchAPI(`/budget${params}`);
}

async function apiSeedBudgets() {
    return fetchAPI('/budget/seed', { method: 'POST' });
}

async function apiCreateBudget(data) {
    return fetchAPI('/budget', { method: 'POST', body: data });
}

async function apiUpdateBudget(id, data) {
    return fetchAPI(`/budget/${id}`, { method: 'PUT', body: data });
}

async function apiDeleteBudget(id) {
    return fetchAPI(`/budget/${id}`, { method: 'DELETE' });
}

// ─── Goals API ─────────────────────────────

async function apiGetGoals() {
    return fetchAPI('/goals');
}

async function apiSeedGoals() {
    return fetchAPI('/goals/seed', { method: 'POST' });
}

async function apiCreateGoal(data) {
    return fetchAPI('/goals', { method: 'POST', body: data });
}

async function apiUpdateGoal(id, data) {
    return fetchAPI(`/goals/${id}`, { method: 'PUT', body: data });
}

async function apiDeleteGoal(id) {
    return fetchAPI(`/goals/${id}`, { method: 'DELETE' });
}

// ─── Advisor API ───────────────────────────

async function apiQueryAdvisor(query, conversationId = null) {
    const body = { query };
    if (conversationId) body.conversation_id = conversationId;
    return fetchAPI('/advisor/query', { method: 'POST', body });
}

async function apiClearAdvisorConversations() {
    return fetchAPI('/advisor/conversations', { method: 'DELETE' });
}

async function apiGenerateInsights() {
    return fetchAPI('/advisor/insights', { method: 'POST' });
}

// ─── Financial Planner API ─────────────────

async function apiGetPlan() {
    return fetchAPI('/planner');
}

async function apiSavePlan(plan_data) {
    return fetchAPI('/planner', { method: 'PUT', body: { plan_data } });
}

async function apiGetPlanSummary() {
    return fetchAPI('/planner/summary');
}

async function apiGetPlanAutoPopulate(month = null) {
    const qs = month ? `?month=${month}` : '';
    return fetchAPI('/planner/auto-populate' + qs);
}

async function apiGetAvailableMonths() {
    return fetchAPI('/planner/available-months');
}

async function apiGetMonthlyComparison() {
    return fetchAPI('/planner/monthly-comparison');
}

// ─── Utility Functions ─────────────────────

/** Format currency */
function formatCurrency(amount, currency = 'CAD') {
    const num = typeof amount === 'string' ? parseFloat(amount) : amount;
    return new Intl.NumberFormat('en-CA', {
        style: 'currency',
        currency: currency,
        minimumFractionDigits: 2,
    }).format(num || 0);
}

/** Format date */
function formatDate(dateStr) {
    if (!dateStr) return '—';
    const d = new Date(dateStr + 'T00:00:00');
    return d.toLocaleDateString('en-CA', { year: 'numeric', month: 'short', day: 'numeric' });
}

/** Format percentage */
function formatPct(value) {
    return `${(value || 0).toFixed(1)}%`;
}

/** Show alert message */
function showAlert(container, message, type = 'danger') {
    const icons = { danger: '⚠️', success: '✅', warning: '⚡', info: 'ℹ️' };
    const el = document.createElement('div');
    el.className = `alert alert-${type}`;
    el.innerHTML = `<span>${icons[type] || ''}</span><span>${message}</span>`;

    // Remove existing alerts
    container.querySelectorAll('.alert').forEach(a => a.remove());
    container.prepend(el);

    // Auto-remove after 5s
    setTimeout(() => el.remove(), 5000);
}

/** Debounce function */
function debounce(fn, delay = 300) {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), delay);
    };
}
