document.addEventListener('DOMContentLoaded', () => {
    const syncAllBtn  = document.getElementById('sync-all-btn');
    const lastUpdated = document.getElementById('last-updated');
    const yearlyTbody = document.getElementById('yearly-tbody');
    let barChart = null, donutChart = null;

    const currentYear = new Date().getFullYear().toString();
    const yearLabel = document.getElementById('current-year-label');
    const renewalsYear = document.getElementById('renewals-year');
    if (yearLabel)    yearLabel.textContent    = currentYear;
    if (renewalsYear) renewalsYear.textContent = currentYear;

    fetchAll();

    // ── Sync button ───────────────────────────────────────────
    if (syncAllBtn) {
        syncAllBtn.addEventListener('click', async () => {
            syncAllBtn.textContent = 'Syncing…';
            syncAllBtn.disabled = true;
            try {
                await Promise.all([
                    fetch('/api/sync/germania', { method: 'POST' }),
                    fetch('/api/sync/private',  { method: 'POST' })
                ]);
                await fetchAll();
            } catch (err) {
                console.error(err);
                alert('Sync error — check the Flask console for details.');
            } finally {
                syncAllBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16" style="margin-right:6px;vertical-align:-2px"><path d="M11.534 7h3.932a.25.25 0 0 1 .192.41l-1.966 2.36a.25.25 0 0 1-.384 0l-1.966-2.36a.25.25 0 0 1 .192-.41zm-11 2h3.932a.25.25 0 0 0 .192-.41L2.692 6.23a.25.25 0 0 0-.384 0L.342 8.59A.25.25 0 0 0 .534 9z"/><path fill-rule="evenodd" d="M8 3c-1.552 0-2.94.707-3.857 1.818a.5.5 0 1 1-.771-.636A6.002 6.002 0 0 1 13.917 7H12.9A5.002 5.002 0 0 0 8 3zM3.1 9a5.002 5.002 0 0 0 7.757 2.209l.746.746A6.002 6.002 0 0 1 2.083 9H3.1z"/></svg>Sync All Data`;
                syncAllBtn.disabled = false;
            }
        });
    }

    // ── Fetch + Render ────────────────────────────────────────
    async function fetchAll() {
        const [metrics, yearly] = await Promise.all([
            fetch('/api/metrics').then(r => r.json()),
            fetch('/api/metrics/yearly').then(r => r.json())
        ]);
        renderKPIs(metrics, yearly);
        renderTable(yearly);
        renderBarChart(yearly);
        renderDonutChart(metrics);
        lastUpdated.textContent = 'Last updated: ' + new Date().toLocaleString();
    }

    // ── KPI Cards ─────────────────────────────────────────────
    function renderKPIs(metrics, yearly) {
        const android = metrics.find(m => m.platform === 'Android') || {};
        const ios     = metrics.find(m => m.platform === 'iOS')     || {};

        // Total (all-time) from app_metrics public data
        set('kpi-android-total', android.downloads ? fmt(android.downloads) + '+' : '—');

        // iOS total (from analytics API)
        const iosTotalDl = ios.downloads || 0;
        set('kpi-ios-total', iosTotalDl > 0 ? fmt(iosTotalDl) : '⏳ Pending');
        set('kpi-ios-note',  iosTotalDl > 0 ? 'All-time · Apple App Store' : 'Analytics API · ready tomorrow');

        // Ratings
        const aR = android.rating || 0;
        const iR = ios.rating     || 0;
        set('kpi-android-rating', aR ? aR.toFixed(1) : '—');
        set('kpi-ios-rating',     iR ? iR.toFixed(1) : '—');
        set('kpi-android-stars',  stars(aR));
        set('kpi-ios-stars',      stars(iR));

        // YTD (current year from yearly_metrics)
        const aYTD = yearly.find(m => m.platform === 'Android' && m.year === currentYear);
        const iYTD = yearly.find(m => m.platform === 'iOS'     && m.year === currentYear);
        set('kpi-android-ytd', aYTD?.downloads > 0 ? fmt(aYTD.downloads) : '—');
        set('kpi-ios-ytd',     iYTD?.downloads > 0 ? fmt(iYTD.downloads) : '⏳ Pending');

        // iOS subscription renewals
        const iSubs = iYTD?.subscriptions || 0;
        set('kpi-ios-subs', iSubs > 0 ? fmt(iSubs) : '—');
    }

    // ── Table ─────────────────────────────────────────────────
    function renderTable(metrics) {
        if (!yearlyTbody) return;
        if (!metrics.length) {
            yearlyTbody.innerHTML = '<tr><td colspan="5" class="table-empty">No data yet — click Sync All Data.</td></tr>';
            return;
        }
        yearlyTbody.innerHTML = metrics.map(m => {
            const isIos = m.platform === 'iOS';
            const icon = isIos
                ? '<svg viewBox="0 0 24 24" width="12" height="12" fill="currentColor"><path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.8-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11"/></svg>'
                : '<svg viewBox="0 0 24 24" width="12" height="12" fill="currentColor"><path d="M17.523 15.341a.5.5 0 1 1-.001 1 .5.5 0 0 1 .001-1m-11.046 0a.5.5 0 1 1-.001 1 .5.5 0 0 1 .001-1M17.7 9.6l1.8-3.1a.4.4 0 0 0-.7-.4l-1.8 3.1A10.8 10.8 0 0 0 12 8.3c-1.8 0-3.5.5-5 1.3L5.2 6.1a.4.4 0 1 0-.7.4l1.8 3.1C4 11 2.2 13.4 2 16.3h20c-.2-2.9-2-5.3-4.3-6.7"/></svg>';

            const dlCell = m.downloads > 0
                ? `<span class="num">${fmt(m.downloads)}</span>`
                : '<span class="pending-cell">⏳ Pending — ready tomorrow</span>';

            const uninstallCell = !isIos && m.uninstalls > 0
                ? `<span class="num">${fmt(m.uninstalls)}</span>`
                : (isIos ? '—' : '<span style="color:#94a3b8">N/A</span>');

            const subsCell = isIos && m.subscriptions > 0
                ? `<span class="num">${fmt(m.subscriptions)}</span>`
                : (isIos ? '<span style="color:#94a3b8">—</span>' : '—');

            return `<tr>
                <td><span class="year-badge">${esc(m.year)}</span></td>
                <td><span class="badge ${isIos ? 'badge-ios' : 'badge-android'}">${icon} ${esc(m.platform)}</span></td>
                <td>${dlCell}</td>
                <td>${uninstallCell}</td>
                <td>${subsCell}</td>
            </tr>`;
        }).join('');
    }

    // ── Bar Chart ─────────────────────────────────────────────
    function renderBarChart(metrics) {
        const years = [...new Set(metrics.map(m => m.year))].sort();
        const aData = years.map(y => { const r = metrics.find(m => m.year === y && m.platform === 'Android'); return r?.downloads || 0; });
        const iData = years.map(y => { const r = metrics.find(m => m.year === y && m.platform === 'iOS');     return r?.downloads || 0; });
        const ctx = document.getElementById('yearly-bar-chart')?.getContext('2d');
        if (!ctx) return;
        if (barChart) barChart.destroy();
        barChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: years,
                datasets: [
                    { label: 'Android', data: aData, backgroundColor: 'rgba(34,197,94,.85)', borderRadius: 8, borderSkipped: false },
                    { label: 'iOS',     data: iData, backgroundColor: 'rgba(59,130,246,.85)', borderRadius: 8, borderSkipped: false }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { position: 'top', labels: { boxWidth: 12, borderRadius: 4, font: { family: 'Inter', size: 12 } } },
                           tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${fmt(c.parsed.y)}` } } },
                scales: { x: { grid: { display: false } }, y: { grid: { color: '#f1f5f9' }, ticks: { callback: v => fmtShort(v) } } }
            }
        });
    }

    // ── Donut Chart ───────────────────────────────────────────
    function renderDonutChart(metrics) {
        const a = metrics.find(m => m.platform === 'Android')?.downloads || 0;
        const i = metrics.find(m => m.platform === 'iOS')?.downloads     || 0;
        const ctx = document.getElementById('platform-donut-chart')?.getContext('2d');
        if (!ctx) return;
        if (donutChart) donutChart.destroy();
        donutChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Android', 'iOS'],
                datasets: [{ data: [a, i], backgroundColor: ['rgba(34,197,94,.85)', 'rgba(59,130,246,.85)'], borderWidth: 0, hoverOffset: 6 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false, cutout: '70%',
                plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, borderRadius: 4, font: { family: 'Inter', size: 12 } } },
                           tooltip: { callbacks: { label: c => ` ${c.label}: ${fmt(c.parsed)}` } } }
            }
        });
    }

    // ── Helpers ───────────────────────────────────────────────
    function set(id, val) { const el = document.getElementById(id); if (el) el.innerHTML = val; }
    function fmt(n)        { return new Intl.NumberFormat().format(n); }
    function fmtShort(n)   { if (n >= 1e6) return (n/1e6).toFixed(1)+'M'; if (n >= 1e3) return (n/1e3).toFixed(0)+'K'; return n; }
    function esc(s)        { if (!s) return ''; return s.toString().replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    function stars(r)      { const f = Math.round(r); return Array.from({length:5}, (_,i) => i < f ? '⭐' : '☆').join(''); }
});
