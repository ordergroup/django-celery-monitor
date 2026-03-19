(function () {
    const QUEUE_COLORS = [
        '#417690', '#28a745', '#fd7e14', '#6f42c1', '#dc3545',
        '#20c997', '#ffc107', '#0dcaf0', '#e83e8c', '#6c757d',
    ];

    function getColor(i) {
        if (i < QUEUE_COLORS.length) return QUEUE_COLORS[i];
        const hue = (i * 137.508) % 360;
        return `hsl(${hue.toFixed(1)}, 65%, 50%)`;
    }

    let chart = null;

    function isDark() {
        return (
            document.documentElement.classList.contains('dark') ||
            document.documentElement.dataset.theme === 'dark' ||
            window.matchMedia('(prefers-color-scheme: dark)').matches
        );
    }

    function gridColor() { return isDark() ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.07)'; }
    function tickColor() { return isDark() ? 'rgb(161,161,170)' : '#666'; }

    function buildDatasets(queues) {
        return Object.entries(queues).map(([name, data], i) => ({
            label: name,
            data: data,
            borderColor: getColor(i),
            backgroundColor: getColor(i) + '22',
            borderWidth: 1.5,
            pointRadius: 0,
            pointHoverRadius: 4,
            tension: 0.3,
            spanGaps: true,
        }));
    }

    function showResetBtn() {
        const btn = document.getElementById('queueHistoryResetZoom');
        if (btn) btn.style.display = '';
    }

    window.resetQueueHistoryZoom = function () {
        if (chart) {
            chart.resetZoom();
            const btn = document.getElementById('queueHistoryResetZoom');
            if (btn) btn.style.display = 'none';
        }
    };

    function pad2(n) { return String(n).padStart(2, '0'); }
    function formatMs(ms) {
        const d = new Date(ms);
        const mon = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getMonth()];
        return mon + ' ' + d.getDate() + ', ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes());
    }

    function initChart(queues) {
        const ctx = document.getElementById('queueHistoryChart');
        if (!ctx) return;

        if (chart) {
            chart.data.datasets = buildDatasets(queues);
            chart.update();
            return;
        }

        chart = new Chart(ctx, {
            type: 'line',
            data: { datasets: buildDatasets(queues) },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'nearest', intersect: false, axis: 'x' },
                plugins: {
                    legend: {
                        labels: { color: tickColor(), boxWidth: 12, font: { size: 12 } },
                    },
                    tooltip: {
                        mode: 'nearest',
                        intersect: false,
                        callbacks: { title: items => formatMs(items[0].parsed.x) },
                    },
                    zoom: {
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            mode: 'x',
                            onZoom: showResetBtn,
                        },
                        pan: {
                            enabled: true,
                            mode: 'x',
                            onPan: showResetBtn,
                        },
                    },
                },
                scales: {
                    x: {
                        type: 'linear',
                        ticks: {
                            color: tickColor(),
                            font: { size: 11 },
                            maxTicksLimit: 24,
                            callback: (val) => formatMs(val),
                        },
                        grid: { color: gridColor() },
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { color: tickColor(), font: { size: 11 }, precision: 0 },
                        grid: { color: gridColor() },
                        title: { display: true, text: 'Queue length', color: tickColor(), font: { size: 11 } },
                    },
                },
            },
        });
    }

    function loadHistory() {
        const ctx = document.getElementById('queueHistoryChart');
        if (!ctx) return;
        const url = ctx.dataset.url;
        if (!url) return;
        fetch(url)
            .then(r => r.json())
            .then(data => initChart(data.queues))
            .catch(() => { });
    }

    document.addEventListener('DOMContentLoaded', function () {
        loadHistory();
        setInterval(loadHistory, 60000);
    });
}());
