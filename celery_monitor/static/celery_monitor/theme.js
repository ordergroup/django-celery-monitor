(function () {
  // ── Theme / style helpers ─────────────────────────────────
  // Detection order:
  // 1. Unfold stores theme in localStorage under "_x_adminTheme" via Alpine $persist
  // 2. Unfold/Tailwind adds .dark class to <html> (may not be set yet if Alpine hasn't run)
  // 3. Classic Django admin 4.2+ uses [data-theme="dark"] on <html>
  // 4. OS preference as final fallback
  function resolveIsDark() {
    const stored =
      localStorage.getItem("adminTheme") ??
      localStorage.getItem("_x_adminTheme");
    if (stored) {
      const val = stored.replace(/^"|"$/g, "");
      if (val === "dark") return true;
      if (val === "light") return false;
      // "auto" — fall through to OS preference
    }
    if (document.documentElement.classList.contains("dark")) return true;
    if (document.documentElement.dataset.theme === "dark") return true;
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  }

  const isDark = resolveIsDark();
  
  // Apply dark mode class to document root for CSS targeting
  if (isDark) {
    document.documentElement.classList.add("celery-monitor-dark");
  } else {
    document.documentElement.classList.remove("celery-monitor-dark");
  }
})();
