// Apply dark mode class early to prevent flash of light theme
// This mirrors what Alpine.js will do later, but applies it immediately
(function() {
  const stored = localStorage.getItem("adminTheme") || localStorage.getItem("_x_adminTheme");
  if (stored) {
    const val = stored.replace(/^"|"$/g, "");
    if (val === "dark" && !document.documentElement.classList.contains("dark")) {
      document.documentElement.classList.add("dark");
    }
  }
})();
