(function () {
    function syncSearchState() {
        const dialog = document.getElementById("search-dialog");
        const isOpen = !!dialog && dialog.hasAttribute("open");

        document.documentElement.classList.toggle("search-open", isOpen);
    }

    function boot() {
        const dialog = document.getElementById("search-dialog");
        if (!dialog) return;

        const observer = new MutationObserver(syncSearchState);
        observer.observe(dialog, {
            attributes: true,
            attributeFilter: ["open"],
        });

        syncSearchState();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();

// ....................... //

document.addEventListener("DOMContentLoaded", () => {
    const prefix = "Forze - ";
    if (!document.title.startsWith(prefix)) {
        document.title = prefix + document.title;
    }
});
