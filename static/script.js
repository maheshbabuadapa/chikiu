document.addEventListener('DOMContentLoaded', () => {
    const metricsGrid = document.getElementById('metrics-grid');

    const syncAllBtn = document.getElementById('sync-all-btn');
    const yearlyTbody = document.getElementById('yearly-tbody');

    // Fetch and display metrics on load
    fetchMetrics();
    fetchYearlyMetrics();

    if (syncAllBtn) {
        syncAllBtn.addEventListener('click', async () => {
            syncAllBtn.textContent = 'Syncing...';
            syncAllBtn.disabled = true;
            try {
                // Run both sync operations concurrently
                const [publicResponse, privateResponse] = await Promise.all([
                    fetch('/api/sync/germania', { method: 'POST' }),
                    fetch('/api/sync/private', { method: 'POST' })
                ]);
                
                let successMessage = "";
                let errorMessage = "";
                
                if (publicResponse.ok && privateResponse.ok) {
                    successMessage = "All data synced successfully.";
                } else {
                    if (!publicResponse.ok) {
                        const err = await publicResponse.json();
                        errorMessage += "Public Sync Failed: " + (err.error || "Unknown error") + "\n";
                    }
                    if (!privateResponse.ok) {
                        const err = await privateResponse.json();
                        errorMessage += "Private Sync Failed: " + (err.error || "Unknown error") + "\n";
                    }
                }
                
                await Promise.all([fetchMetrics(), fetchYearlyMetrics()]);
                
                if (errorMessage) {
                    alert(errorMessage);
                } else if (successMessage) {
                    alert(successMessage);
                }
            } catch (error) {
                console.error("Error syncing data:", error);
                alert("Error syncing data.");
            } finally {
                syncAllBtn.textContent = 'Sync All Data';
                syncAllBtn.disabled = false;
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

    async function fetchYearlyMetrics() {
        try {
            const response = await fetch('/api/metrics/yearly');
            const metrics = await response.json();
            renderYearlyMetrics(metrics);
        } catch (error) {
            console.error("Error fetching yearly metrics:", error);
            if (yearlyTbody) yearlyTbody.innerHTML = '<tr><td colspan="5" style="padding: 1rem; color: red;">Error loading yearly data.</td></tr>';
        }
    }

    function renderYearlyMetrics(metrics) {
        if (!yearlyTbody) return;
        yearlyTbody.innerHTML = '';

        if (metrics.length === 0) {
            yearlyTbody.innerHTML = '<tr><td colspan="5" style="padding: 2rem; text-align: center; color: var(--text-muted);">No yearly data yet. Click "Sync All Data" to populate this table.</td></tr>';
            return;
        }

        metrics.forEach(metric => {
            const tr = document.createElement('tr');
            tr.style.borderBottom = '1px solid var(--glass-border)';
            const platformClass = metric.platform === 'iOS' ? 'platform-ios' : 'platform-android';
            tr.innerHTML = `
                <td style="padding: 1rem; font-weight: 500;">${escapeHTML(metric.year)}</td>
                <td style="padding: 1rem;">${escapeHTML(metric.app_name)}</td>
                <td style="padding: 1rem;"><span class="platform-badge ${platformClass}">${escapeHTML(metric.platform)}</span></td>
                <td style="padding: 1rem;">${new Intl.NumberFormat().format(metric.downloads)}</td>
                <td style="padding: 1rem;">${metric.uninstalls > 0 ? new Intl.NumberFormat().format(metric.uninstalls) : 'N/A'}</td>
            `;
            yearlyTbody.appendChild(tr);
        });
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
