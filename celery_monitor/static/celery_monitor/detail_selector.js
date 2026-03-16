(function () {
    function fuzzyScore(search) {
        var s = search.toLowerCase();
        return function (item) {
            var text = (item.text || '').toLowerCase();
            var score = 0, idx = 0;
            for (var i = 0; i < s.length; i++) {
                var pos = text.indexOf(s[i], idx);
                if (pos === -1) return 0;
                score += 1 / (pos - idx + 1);
                idx = pos + 1;
            }
            return score;
        };
    }

    window.initDetailSelector = function (selectId, paramName, placeholder) {
        new TomSelect('#' + selectId, {
            maxOptions: null,
            placeholder: placeholder,
            score: fuzzyScore,
            onInitialize: function () {
                var el = this.control_input;
                el.setAttribute('autocomplete', 'off');
                el.setAttribute('data-bwignore', 'true');
                el.setAttribute('data-lpignore', 'true');
                el.setAttribute('data-form-type', 'other');
            },
            onChange: function (value) {
                if (!value) return;
                var params = new URLSearchParams(window.location.search);
                params.set(paramName, value);
                window.location.href = window.location.pathname + '?' + params.toString();
            },
        });
    };
}());
