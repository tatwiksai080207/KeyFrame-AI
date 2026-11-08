
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
    btn.querySelector(".keyframe-ai-btn-label").textContent = text;
    btn.classList.toggle("keyframe-ai-btn--error", state === "error");
    btn.classList.toggle("keyframe-ai-btn--ok", state === "ok");
    btn.disabled = state !== "idle";
  }

  function resetButtonLater(delayMs) {
    setTimeout(() => setButton("Take Notes", "idle"), delayMs);
  }

  function handleClick(event) {
    event.preventDefault();
    event.stopPropagation(); // do not let yt treat this as a player click

    const videoId = getVideoId();
    if (!videoId) {
      setButton("No video found", "error");
      resetButtonLater(3000);
      return;
    }

    setButton("Queuing...", "busy");

    try {
      chrome.runtime.sendMessage(
        { type: "START_JOB", videoId, url: window.location.href },
        (response) => {
          if (chrome.runtime.lastError) {
            console.error("[KeyFrame-AI]", chrome.runtime.lastError.message);
            setButton("Error - retry", "error");
          } else if (response && response.success) {
            setButton("Queued", "ok");
          } else {
            setButton((response && response.error) || "Failed", "error");
          }
          resetButtonLater(4000);
        },
      );
    } catch (err) {
      setButton("Reload the page", "error");
    }
  }

  function createButton() {
    const btn = document.createElement("button");
    btn.id = BUTTON_ID;
    btn.className = "keyframe-ai-btn";
    btn.title = "Generate study notes for this video";

    const label = document.createElement("span");
    label.className = "keyframe-ai-btn-label";
    label.textContent = "Take Notes";
    btn.append(label);

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
