window.renderDetailCharts = function (raw) {
    var labels = raw.labels;

    var isDark = document.documentElement.dataset.theme === 'dark'
        || document.documentElement.classList.contains('dark')
        || window.matchMedia('(prefers-color-scheme: dark)').matches;
    var gridColor  = isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.07)';
    var labelColor = isDark ? '#aaa' : '#555';

    function fmt24h(ms) {
        var d = new Date(ms);
        return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
    }

    function formatDuration(s) {
        if (s === null || s === undefined) return '—';
        if (s < 1)    return s.toFixed(3) + 's';
        if (s < 60)   return s.toFixed(2) + 's';
        if (s < 3600) return Math.floor(s / 60) + 'm ' + Math.floor(s % 60) + 's';
        return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
    }

    function makeChart(canvasId, datasets) {
        var ctx = document.getElementById(canvasId).getContext('2d');
        return new Chart(ctx, {
            type: 'line',
            data: { labels: labels, datasets: datasets },
            options: {
                responsive: true,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: labelColor } },
                    tooltip: {
                        callbacks: {
                            label: function (ctx) {
                                var v = ctx.raw;
                                if (v === null || v === undefined) return ctx.dataset.label + ': —';
                                return ctx.dataset.label + ': ' + formatDuration(v);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm',
                            displayFormats: { hour: 'HH:mm', minute: 'HH:mm', day: 'yyyy-MM-dd' },
                        },
                        ticks: { color: labelColor, callback: fmt24h },
                        grid: { color: gridColor }
                    },
                    y: {
                        ticks: {
                            color: labelColor,
                            callback: function (v) { return formatDuration(v); }
                        },
                        grid: { color: gridColor }
                    }
                }
            }
        });
    }

    (function () {
        var ctx = document.getElementById('countChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Success',
                        data: raw.success_counts,
                        backgroundColor: 'rgba(92,184,92,0.7)',
                        borderColor: '#5cb85c',
                        borderWidth: 1,
                        stack: 'counts',
                    },
                    {
                        label: 'Failure',
                        data: raw.failure_counts,
                        backgroundColor: 'rgba(224,83,58,0.7)',
                        borderColor: '#e0533a',
                        borderWidth: 1,
                        stack: 'counts',
                    },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { labels: { color: labelColor } } },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm',
                            displayFormats: { hour: 'HH:mm', day: 'yyyy-MM-dd' },
                        },
                        ticks: { color: labelColor, source: 'data', callback: fmt24h },
                        grid: { color: gridColor },
                        stacked: true,
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { color: labelColor, precision: 0 },
                        grid: { color: gridColor },
                        stacked: true,
                    }
                }
            }
        });
    }());

    (function () {
        var tp = raw.throughput;
        if (!tp.queued.length && !tp.started.length) return;
        var ctx = document.getElementById('throughputChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [
                    {
                        label: 'Queued',
                        data: tp.queued,
                        borderColor: '#f0ad4e',
                        backgroundColor: 'rgba(240,173,78,0.08)',
                        borderWidth: 2,
                        stepped: 'after',
                        pointRadius: 0,
                        tension: 0,
                    },
                    {
                        label: 'Started',
                        data: tp.started,
                        borderColor: '#4a90d9',
                        backgroundColor: 'rgba(74,144,217,0.08)',
                        borderWidth: 2,
                        stepped: 'after',
                        pointRadius: 0,
                        tension: 0,
                    },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: labelColor } },
                    tooltip: { mode: 'index', intersect: false },
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                            displayFormats: {
                                millisecond: 'HH:mm:ss',
                                second: 'HH:mm:ss',
                                minute: 'HH:mm',
                                hour: 'HH:mm',
                                day: 'yyyy-MM-dd',
                            },
                        },
                        ticks: { color: labelColor, callback: fmt24h },
                        grid: { color: gridColor },
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { color: labelColor, precision: 0 },
                        grid: { color: gridColor },
                        title: { display: true, text: 'Cumulative count', color: labelColor },
                    }
                }
            }
        });
    }());

    makeChart('runtimeChart', [
        { label: 'Avg', data: raw.runtime.avg, borderColor: '#4a90d9', backgroundColor: 'rgba(74,144,217,0.08)', tension: 0.3, spanGaps: true },
        { label: 'Max', data: raw.runtime.max, borderColor: '#e0533a', backgroundColor: 'rgba(224,83,58,0.06)', borderDash: [4,3], tension: 0.3, spanGaps: true },
        { label: 'Min', data: raw.runtime.min, borderColor: '#5cb85c', backgroundColor: 'rgba(92,184,92,0.06)', borderDash: [4,3], tension: 0.3, spanGaps: true },
    ]);

    makeChart('waitChart', [
        { label: 'Avg', data: raw.wait.avg, borderColor: '#f0ad4e', backgroundColor: 'rgba(240,173,78,0.08)', tension: 0.3, spanGaps: true },
        { label: 'Max', data: raw.wait.max, borderColor: '#e0533a', backgroundColor: 'rgba(224,83,58,0.06)', borderDash: [4,3], tension: 0.3, spanGaps: true },
        { label: 'Min', data: raw.wait.min, borderColor: '#5cb85c', backgroundColor: 'rgba(92,184,92,0.06)', borderDash: [4,3], tension: 0.3, spanGaps: true },
    ]);
};
