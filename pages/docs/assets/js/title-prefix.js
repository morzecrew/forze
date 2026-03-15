document.addEventListener("DOMContentLoaded", () => {
    const prefix = "Forze - ";
    if (!document.title.startsWith(prefix)) {
        document.title = prefix + document.title;
    }
});
