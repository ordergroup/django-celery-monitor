document.addEventListener('htmx:configRequest', function (e) {
    var meta = document.querySelector('meta[name=csrf-token]');
    if (meta) e.detail.headers['X-CSRFToken'] = meta.content;
});

function toggleAccordion(id) {
    var element = document.getElementById(id);
    var icon = document.getElementById(id + '-icon');
    if (element.style.display === 'none') {
        element.style.display = 'block';
        icon.textContent = '▼';
        htmx.trigger(element, 'accordion-open');
    } else {
        element.style.display = 'none';
        icon.textContent = '▶';
    }
}

window._revokedTaskIds = new Set();

function markTaskRevoked(btn, taskId) {
    window._revokedTaskIds.add(taskId);
    btn.disabled = true;
    btn.textContent = 'Revoked';
    btn.classList.remove('kill-btn--term');
    btn.classList.add('kill-btn--revoked');
}

function applyRevokedStates() {
    window._revokedTaskIds.forEach(function (id) {
        var btn = document.querySelector('[data-task-id="' + id + '"]');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Revoked';
            btn.classList.remove('kill-btn--term');
            btn.classList.add('kill-btn--revoked');
        }
    });
}
