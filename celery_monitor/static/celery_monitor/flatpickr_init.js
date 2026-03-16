(function() {
    var theme = document.documentElement.dataset.theme;
    var isDark = theme === 'dark' || (theme === 'auto' && window.matchMedia('(prefers-color-scheme: dark)').matches);
    var link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = isDark
        ? 'https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css'
        : 'https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css';
    document.head.appendChild(link);
})();
