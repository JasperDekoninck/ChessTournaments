const initPublicLiveUpdates = () => {
  const root = document.querySelector("[data-public-live-root]");
  if (!root) {
    return;
  }

  let currentVersion = root.dataset.liveVersion || "";
  let inFlight = false;

  const liveUrl = () => {
    const url = new URL(root.dataset.liveUrl, window.location.origin);
    const view = root.dataset.liveView || "";
    const roundNo = root.dataset.liveRoundNo || "";
    if (view) {
      url.searchParams.set("view", view);
    }
    if (roundNo) {
      url.searchParams.set("round_no", roundNo);
    }
    return url;
  };

  const refresh = async () => {
    if (inFlight || document.hidden) {
      return;
    }
    inFlight = true;
    try {
      const response = await fetch(liveUrl(), {
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      if (!payload.version || payload.version === currentVersion || typeof payload.html !== "string") {
        return;
      }
      currentVersion = payload.version;
      root.innerHTML = payload.html;
    } catch (error) {
      return;
    } finally {
      inFlight = false;
    }
  };

  window.setInterval(refresh, 5000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refresh();
    }
  });
};

document.addEventListener("DOMContentLoaded", initPublicLiveUpdates);
