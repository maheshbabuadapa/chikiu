document.addEventListener('DOMContentLoaded', () => {
    const metricsGrid = document.getElementById('metrics-grid');

    const privateSyncBtn = document.getElementById('sync-private-btn');
    const historicalTbody = document.getElementById('historical-tbody');
    const yearlyTbody = document.getElementById('yearly-tbody');

    // Fetch and display metrics on load
    fetchMetrics();
    fetchHistoricalMetrics();

    const syncBtn = document.getElementById('sync-germania-btn');
    if (syncBtn) {
        syncBtn.addEventListener('click', async () => {
            syncBtn.textContent = 'Syncing...';
            syncBtn.disabled = true;
            try {
                const response = await fetch('/api/sync/germania', { method: 'POST' });
                if (response.ok) {
                    await fetchMetrics();
                } else {
                    const err = await response.json();
                    alert("Failed to sync: " + (err.error || "Unknown error"));
                }
            } catch (error) {
                console.error("Error syncing:", error);
                alert("Error syncing data.");
            } finally {
                syncBtn.textContent = 'Sync Public Data';
                syncBtn.disabled = false;
            }
        });
    }

    if (privateSyncBtn) {
        privateSyncBtn.addEventListener('click', async () => {
            privateSyncBtn.textContent = 'Syncing...';
            privateSyncBtn.disabled = true;
            try {
                const response = await fetch('/api/sync/private', { method: 'POST' });
                if (response.ok) {
                    await fetchHistoricalMetrics();
                    alert("Private data synced successfully.");
                } else {
                    const err = await response.json();
                    alert("Failed to sync private data: " + (err.error || "Unknown error"));
                }
            } catch (error) {
                console.error("Error syncing private data:", error);
                alert("Error syncing private data.");
            } finally {
                privateSyncBtn.textContent = 'Sync Private Data';
                privateSyncBtn.disabled = false;
            }
        });
    }


    async function fetchMetrics() {
        try {
            const response = await fetch('/api/metrics');
            const metrics = await response.json();
            renderMetrics(metrics);
        } catch (error) {
            console.error("Error fetching metrics:", error);
            metricsGrid.innerHTML = '<p style="color: red;">Error loading metrics.</p>';
        }
    }

    async function fetchHistoricalMetrics() {
        try {
            const response = await fetch('/api/metrics/monthly');
            const metrics = await response.json();
            renderHistoricalMetrics(metrics);
        } catch (error) {
            console.error("Error fetching historical metrics:", error);
            if (historicalTbody) historicalTbody.innerHTML = '<tr><td colspan="5" style="padding: 1rem; color: red;">Error loading historical data.</td></tr>';
        }
    }

    function renderHistoricalMetrics(metrics) {
        if (!historicalTbody || !yearlyTbody) return;
        historicalTbody.innerHTML = ''; // Clear current table
        yearlyTbody.innerHTML = '';

        if (metrics.length === 0) {
            const emptyMsg = '<tr><td colspan="5" style="padding: 2rem; text-align: center; color: var(--text-muted);">No historical data yet. Sync private data to populate this table.</td></tr>';
            historicalTbody.innerHTML = emptyMsg;
            yearlyTbody.innerHTML = '<tr><td colspan="3" style="padding: 2rem; text-align: center; color: var(--text-muted);">No yearly data yet.</td></tr>';
            return;
        }

        const currentYear = new Date().getFullYear().toString();
        const last3Years = [currentYear, (parseInt(currentYear) - 1).toString(), (parseInt(currentYear) - 2).toString()];

        const currentYearMetrics = metrics.filter(m => m.month_year.startsWith(currentYear));
        
        const yearlyData = {};
        metrics.forEach(m => {
            const year = m.month_year.substring(0, 4);
            if (!last3Years.includes(year)) return;
            const key = `${year}-${m.platform}`;
            if (!yearlyData[key]) {
                yearlyData[key] = { year, platform: m.platform, downloads: 0 };
            }
            yearlyData[key].downloads += m.downloads;
        });

        const yearlyMetrics = Object.values(yearlyData).sort((a, b) => b.year.localeCompare(a.year));

        if (yearlyMetrics.length === 0) {
            yearlyTbody.innerHTML = '<tr><td colspan="3" style="padding: 2rem; text-align: center; color: var(--text-muted);">No data available for the last 3 years.</td></tr>';
        } else {
            yearlyMetrics.forEach(metric => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid var(--glass-border)';
                const platformClass = metric.platform === 'iOS' ? 'platform-ios' : 'platform-android';
                tr.innerHTML = `
                    <td style="padding: 1rem; font-weight: 500;">${escapeHTML(metric.year)}</td>
                    <td style="padding: 1rem;"><span class="platform-badge ${platformClass}">${escapeHTML(metric.platform)}</span></td>
                    <td style="padding: 1rem;">${new Intl.NumberFormat().format(metric.downloads)}</td>
                `;
                yearlyTbody.appendChild(tr);
            });
        }

        if (currentYearMetrics.length === 0) {
            historicalTbody.innerHTML = '<tr><td colspan="5" style="padding: 2rem; text-align: center; color: var(--text-muted);">No data available for the current year.</td></tr>';
        } else {
            currentYearMetrics.forEach(metric => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid var(--glass-border)';
                const platformClass = metric.platform === 'iOS' ? 'platform-ios' : 'platform-android';
                tr.innerHTML = `
                    <td style="padding: 1rem; font-weight: 500;">${escapeHTML(metric.month_year)}</td>
                    <td style="padding: 1rem;">${escapeHTML(metric.app_name)}</td>
                    <td style="padding: 1rem;"><span class="platform-badge ${platformClass}">${escapeHTML(metric.platform)}</span></td>
                    <td style="padding: 1rem;">${new Intl.NumberFormat().format(metric.downloads)}</td>
                    <td style="padding: 1rem;">${new Intl.NumberFormat().format(metric.uninstalls)}</td>
                `;
                historicalTbody.appendChild(tr);
            });
        }
    }

    function renderMetrics(metrics) {
        metricsGrid.innerHTML = ''; // Clear current grid

        if (metrics.length === 0) {
            metricsGrid.innerHTML = '<p style="color: var(--text-muted); grid-column: 1/-1; text-align: center; padding: 2rem;">No metrics added yet. Add your first app above!</p>';
            return;
        }

        metrics.forEach(metric => {
            const card = document.createElement('div');
            card.className = 'metric-card';

            const platformClass = metric.platform === 'iOS' ? 'platform-ios' : 'platform-android';

            // Format downloads number (e.g., 10000 -> 10,000)
            let formattedDownloads = new Intl.NumberFormat().format(metric.downloads);
            if (metric.platform === 'iOS' && metric.downloads === 0) {
                formattedDownloads = "N/A";
            }
            if (metric.platform === 'Android' && metric.downloads > 0) {
                formattedDownloads += "+";
            }

            card.innerHTML = `
                <div class="metric-card-header">
                    <div class="metric-card-title">${escapeHTML(metric.app_name)}</div>
                    <div class="platform-badge ${platformClass}">${escapeHTML(metric.platform)}</div>
                </div>
                <div class="metric-stats">
                    <div class="stat-item">
                        <span class="stat-label">Downloads</span>
                        <span class="stat-value">${formattedDownloads}</span>
                    </div>
                    <div class="stat-item" style="text-align: right;">
                        <span class="stat-label">Rating</span>
                        <span class="stat-value">⭐ ${metric.rating.toFixed(1)}</span>
                    </div>
                </div>
            `;
            metricsGrid.appendChild(card);
        });
    }

    // Basic HTML escaping to prevent XSS
    function escapeHTML(str) {
        if (!str) return '';
        return str.toString()
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
});
