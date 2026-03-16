var fpFrom, fpTo;

function initFlatpickrDateRange() {
    var opts = { enableTime: true, time_24hr: true, dateFormat: 'Y-m-d\\TH:i', allowInput: true };
    var fromEl = document.getElementById('date_from');
    var toEl = document.getElementById('date_to');
    if (fromEl) fpFrom = flatpickr(fromEl, opts);
    if (toEl) fpTo = flatpickr(toEl, opts);
}

function clearCustomDates() {
    if (fpFrom) fpFrom.clear();
    if (fpTo) fpTo.clear();
}

function clearHoursParam() {
    var el = document.getElementById('hours');
    if (el) el.value = '';
}
