const initRegisterLookup = () => {
  document.querySelectorAll("[data-register-search]").forEach((form) => {
    const nameInput = form.querySelector("[data-player-name]");
    const ratingInput = form.querySelector("[data-player-rating]");
    const results = form.querySelector("[data-player-results]");
    let controller = null;

    const hideResults = () => {
      results.hidden = true;
      results.innerHTML = "";
    };

    const selectPlayer = (item) => {
      nameInput.value = item.name;
      if (item.rating !== null && item.rating !== undefined) {
        ratingInput.value = item.rating;
      }
      hideResults();
    };

    nameInput.addEventListener("input", async () => {
      const term = nameInput.value.trim();
      if (term.length < 2) {
        hideResults();
        return;
      }
      controller?.abort();
      controller = new AbortController();
      try {
        const response = await fetch(`/register/lookup?q=${encodeURIComponent(term)}`, {
          signal: controller.signal,
        });
        const payload = await response.json();
        const items = payload.items || [];
        if (!items.length) {
          hideResults();
          return;
        }
        results.innerHTML = "";
        items.forEach((item) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "autocomplete-option";
          button.textContent = `${item.name}${item.member ? " · member" : ""}${item.rating ? ` · ${item.rating}` : ""}`;
          button.addEventListener("click", () => selectPlayer(item));
          results.appendChild(button);
        });
        results.hidden = false;
      } catch (error) {
        if (error.name !== "AbortError") {
          hideResults();
        }
      }
    });

    document.addEventListener("click", (event) => {
      if (!form.contains(event.target)) {
        hideResults();
      }
    });
  });
};

document.addEventListener("DOMContentLoaded", initRegisterLookup);
