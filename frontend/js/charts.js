/**
 * Chart.js wrapper utilities for rendering financial charts.
 */

// Default Chart.js options for dark theme
const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            labels: {
                color: '#94a3b8',
                font: { size: 12 },
            },
        },
        tooltip: {
            backgroundColor: '#1e293b',
            titleColor: '#f1f5f9',
            bodyColor: '#94a3b8',
            borderColor: '#334155',
            borderWidth: 1,
            padding: 10,
            callbacks: {
                label: function(ctx) {
                    let value = ctx.parsed.y !== undefined ? ctx.parsed.y : ctx.parsed;
                    return `${ctx.dataset.label || ctx.label}: $${value.toLocaleString('en-CA', { minimumFractionDigits: 2 })}`;
                }
            }
        },
    },
    scales: {
        x: {
            ticks: { color: '#64748b' },
            grid: { color: 'rgba(51, 65, 85, 0.5)' },
        },
        y: {
            ticks: {
                color: '#64748b',
                callback: (v) => '$' + v.toLocaleString(),
            },
            grid: { color: 'rgba(51, 65, 85, 0.5)' },
        },
    },
};

// Color palette
const CHART_COLORS = [
    '#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7',
    '#06b6d4', '#ec4899', '#f97316', '#14b8a6', '#8b5cf6',
    '#6366f1', '#84cc16', '#e11d48', '#0ea5e9', '#d946ef',
];

// Store chart instances for cleanup
const _chartInstances = {};

function _destroyChart(canvasId) {
    if (_chartInstances[canvasId]) {
        _chartInstances[canvasId].destroy();
        delete _chartInstances[canvasId];
    }
}

/**
 * Render a bar chart (e.g., Income vs Expenses by month).
 */
function renderBarChart(canvasId, labels, datasets) {
    _destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    _chartInstances[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: datasets.map((ds, i) => ({
                label: ds.label,
                data: ds.data,
                backgroundColor: ds.color || CHART_COLORS[i],
                borderColor: ds.borderColor || 'transparent',
                borderWidth: 1,
                borderRadius: 6,
                barPercentage: 0.7,
                ...ds,
            })),
        },
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                legend: {
                    ...chartDefaults.plugins.legend,
                    position: 'top',
                },
            },
        },
    });
}

/**
 * Render a pie/doughnut chart (e.g., Category breakdown).
 */
function renderPieChart(canvasId, labels, data, isDoughnut = true) {
    _destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    _chartInstances[canvasId] = new Chart(ctx, {
        type: isDoughnut ? 'doughnut' : 'pie',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: CHART_COLORS.slice(0, data.length),
                borderColor: '#1e293b',
                borderWidth: 2,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#94a3b8',
                        font: { size: 11 },
                        padding: 12,
                        usePointStyle: true,
                        pointStyleWidth: 10,
                    },
                },
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(ctx) {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : 0;
                            return `${ctx.label}: $${ctx.parsed.toLocaleString('en-CA', { minimumFractionDigits: 2 })} (${pct}%)`;
                        },
                    },
                },
            },
        },
    });
}

/**
 * Render a line chart (e.g., spending trend over time).
 */
function renderLineChart(canvasId, labels, datasets) {
    _destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    _chartInstances[canvasId] = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: datasets.map((ds, i) => ({
                label: ds.label,
                data: ds.data,
                borderColor: ds.color || CHART_COLORS[i],
                backgroundColor: (ds.color || CHART_COLORS[i]) + '20',
                fill: ds.fill !== undefined ? ds.fill : true,
                tension: 0.4,
                pointRadius: 4,
                pointHoverRadius: 6,
                ...ds,
            })),
        },
        options: chartDefaults,
    });
}
