(function () {
  "use strict";

  const BUTTON_ID = "keyframe-ai-take-notes-btn";
  const RETRY_DELAY_MS = 1000;
  const MAX_ATTEMPTS = 30;

  let injectionRun = 0;

  function getVideoId() {
    return new URLSearchParams(window.location.search).get("v");
  }

  function isWatchPage() {
    return window.location.pathname === "/watch";
  }

  function setButton(text, state) {
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;

    const tooltip = btn.querySelector(".keyframe-ai-tooltip");
    if (tooltip) {
      tooltip.textContent = text || "Take Notes";
    }
    btn.setAttribute("aria-label", text || "Take Notes");

    btn.classList.toggle("keyframe-ai-btn--error", state === "error");
    btn.classList.toggle("keyframe-ai-btn--ok", state === "ok");
    btn.classList.toggle("keyframe-ai-btn--busy", state === "busy");
    btn.disabled = state !== "idle";
  }

  function resetButtonLater(delayMs) {
    setTimeout(() => setButton("Take Notes", "idle"), delayMs);
  }

  function getVideoTitle() {
    const el =
      document.querySelector("h1.ytd-watch-metadata yt-formatted-string") ||
      document.querySelector("h1.title yt-formatted-string") ||
      document.querySelector("#title h1 yt-formatted-string") ||
      document.querySelector("h1 yt-formatted-string");
    if (el && el.textContent.trim()) {
      return el.textContent.trim();
    }
    const docTitle = document.title || "";
    return docTitle.replace(/ - YouTube$/, "").trim() || "YouTube Video";
  }

  function handleClick(event) {
    event.preventDefault();
    event.stopPropagation(); // Do not trigger video playback toggle

    const videoId = getVideoId();
    if (!videoId) {
      setButton("No video found", "error");
      resetButtonLater(3000);
      return;
    }

    const title = getVideoTitle();
    setButton("Queuing notes...", "busy");

    try {
      chrome.runtime.sendMessage(
        { type: "START_JOB", videoId, url: window.location.href, title },
        (response) => {
          if (chrome.runtime.lastError) {
            console.error("[KeyFrame-AI]", chrome.runtime.lastError.message);
            setButton("Error - retry", "error");
          } else if (response && response.success) {
            setButton("Notes Queued!", "ok");
          } else {
            setButton((response && response.error) || "Failed", "error");
          }
          resetButtonLater(4000);
        },
      );
    } catch (err) {
      setButton("Reload page", "error");
      resetButtonLater(4000);
    }
  }

  function createButton() {
    const btn = document.createElement("button");
    btn.id = BUTTON_ID;
    btn.className = "ytp-button keyframe-ai-btn";
    btn.setAttribute("aria-label", "Take Notes");

    // Centered 24x24 SVG inside the 48x48 button
    btn.innerHTML = `
      <svg class="keyframe-ai-svg" viewBox="0 0 24 24" width="24" height="24">
        <g class="keyframe-ai-state keyframe-ai-state--idle">
          <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 14H7v-2h10v2zm0-4H7v-2h10v2zm0-4H7V7h10v2z" fill="#ffffff"></path>
        </g>
        <g class="keyframe-ai-state keyframe-ai-state--busy">
          <circle cx="12" cy="12" r="9" stroke="#ffffff" stroke-opacity="0.3" stroke-width="2.2" fill="none"></circle>
          <path d="M12 3a9 9 0 0 1 9 9" stroke="#ffffff" stroke-width="2.2" stroke-linecap="round" fill="none"></path>
        </g>
        <g class="keyframe-ai-state keyframe-ai-state--ok">
          <polyline points="20 6 9 17 4 12" stroke="#16a34a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" fill="none"></polyline>
        </g>
        <g class="keyframe-ai-state keyframe-ai-state--error">
          <circle cx="12" cy="12" r="9" stroke="#dc2626" stroke-width="2.5" fill="none"></circle>
          <line x1="12" y1="8" x2="12" y2="12" stroke="#dc2626" stroke-width="2.5" stroke-linecap="round"></line>
          <line x1="12" y1="16" x2="12.01" y2="16" stroke="#dc2626" stroke-width="2.5" stroke-linecap="round"></line>
        </g>
      </svg>
      <div class="keyframe-ai-tooltip">Take Notes</div>
    `;

    btn.addEventListener("click", handleClick);
    return btn;
  }

  function injectButton(run, attempt) {
    if (run !== injectionRun) return;
    if (!isWatchPage() || document.getElementById(BUTTON_ID)) return;

    const controls =
      document.querySelector(".ytp-right-controls") ||
      document.querySelector(".ytp-left-controls");

    if (controls) {
      controls.prepend(createButton());
      return;
    }
    if (attempt < MAX_ATTEMPTS) {
      setTimeout(() => injectButton(run, attempt + 1), RETRY_DELAY_MS);
    }
  }

  function refresh() {
    injectionRun += 1;
    const old = document.getElementById(BUTTON_ID);
    if (old) old.remove();
    injectButton(injectionRun, 0);
  }

  document.addEventListener("yt-navigate-finish", refresh);
  refresh();
})();
