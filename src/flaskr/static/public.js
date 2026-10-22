const initPublicLiveUpdates = () => {
  const root = document.querySelector("[data-public-live-root]");
  if (!root) {
    return;
  }

  let currentVersion = root.dataset.liveVersion || "";
  let controller = null;
  const currentSelection = () => ({
    view: root.dataset.liveView || "boards",
    roundNo: root.dataset.liveRoundNo || "",
  });
  const rememberSelection = (method, selection, url) => {
    window.history[method]({ ...window.history.state, publicRound: selection }, "", url);
  };
  rememberSelection("replaceState", currentSelection(), window.location.href);

  const refresh = async (selection = currentSelection(), navigation = null) => {
    if (!navigation && (controller || document.hidden)) return;
    controller?.abort();
    const requestController = new AbortController();
    controller = requestController;
    const followLatestRound = selection.view === "boards" && (
      !selection.roundNo || selection.roundNo === root.dataset.liveLatestRound
    );
    const url = new URL(root.dataset.liveUrl, window.location.origin);
    url.searchParams.set("view", selection.view);
    if (selection.roundNo && !followLatestRound) url.searchParams.set("round_no", selection.roundNo);
    if (navigation) root.setAttribute("aria-busy", "true");
    try {
      const response = await fetch(url, {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        signal: requestController.signal,
      });
      if (!response.ok) throw new Error("Could not load this round.");
      const payload = await response.json();
      if (controller !== requestController) return;
      if (!payload.version || typeof payload.html !== "string") throw new Error("Invalid round response.");
      root.dataset.liveView = payload.view;
      root.dataset.liveLatestRound = String(payload.latest_round || "");
      root.dataset.liveRoundNo = selection.roundNo
        ? String(followLatestRound ? payload.selected_round || "" : selection.roundNo) : "";
      if (payload.version !== currentVersion) {
        const { scrollX, scrollY } = window;
        root.innerHTML = payload.html;
        window.scrollTo({ left: scrollX, top: scrollY, behavior: "instant" });
        currentVersion = payload.version;
      }
      if (navigation?.push) {
        rememberSelection("pushState", currentSelection(), navigation.url);
      } else if (!navigation && followLatestRound && selection.roundNo && payload.round_url) {
        rememberSelection("replaceState", currentSelection(), payload.round_url);
      }
    } catch (error) {
      if (error.name !== "AbortError" && navigation) window.location.assign(navigation.url);
    } finally {
      if (controller === requestController) {
        controller = null;
        root.removeAttribute("aria-busy");
      }
    }
  };

  root.addEventListener("click", (event) => {
    const link = event.target.closest("a[data-public-navigation]");
    if (!link || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey
        || link.target || link.hasAttribute("download")) return;
    event.preventDefault();
    refresh({ view: link.dataset.view, roundNo: link.dataset.roundNo }, { url: link.href, push: true });
  });
  window.addEventListener("popstate", (event) => {
    if (event.state?.publicRound) refresh(event.state.publicRound, { url: window.location.href });
  });
  window.setInterval(() => refresh(), 5000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refresh();
    }
  });
};

document.addEventListener("DOMContentLoaded", initPublicLiveUpdates);
