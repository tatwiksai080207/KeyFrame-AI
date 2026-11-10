// Shows the active job's progress as readable stages and holds the settings form

const POLL_INTERVAL_MS = 2000;
const STAGE_ORDER = [
  "QUEUED",
  "TRANSCRIBING",
  "SUMMARIZING",
  "EXTRACTING_FRAMES",
  "PUBLISHING",
];
const DONE_INDEX = STAGE_ORDER.length; 
const STAGE_LABELS = {
  QUEUED: "Waiting in line",
  TRANSCRIBING: "Getting the transcript",
  SUMMARIZING: "Writing the summary",
  EXTRACTING_FRAMES: "Capturing key frames",
  PUBLISHING: "Creating the Notion page",
};

const ERROR_MESSAGES = {
  VIDEO_TOO_LONG:
    "This video is longer than 20 minutes, which is not supported.",
  TRANSCRIPT_NOT_FOUND: "No transcript is available for this video.",
  TRANSCRIPT_FETCH_ERROR:
    "The transcript could not be fetched. The video may be private or unavailable.",
  NO_TRANSCRIPT_AVAILABLE: "No transcript is available for this video.",
  RATE_LIMIT_EXHAUSTED:
    "The AI service is busy right now. Please try again in a few minutes.",
  RETRY_BUDGET_EXHAUSTED:
    "Gave up after several attempts. Please try again later.",
  LLM_SYNTHESIS_ERROR: "The AI could not summarize this video.",
  NOTION_PUBLISH_FAILED:
    "Could not create the Notion page. Check that the integration can access the destination page.",
  MISSING_NOTION_PARENT_PAGE_ID: "No Notion destination page is configured.",
};

const $ = (id) => document.getElementById(id);
let pollTimer = null;

function isFinished(state) {
  return state === "COMPLETED" || state === "COMPLETED_WITH_WARNINGS";
}

function isFailed(state) {
  return state === "FAILED" || state === "DEAD_LETTERED";
}

function formatTime(totalSec) {
  const minutes = Math.floor(totalSec / 60);
  const seconds = String(totalSec % 60).padStart(2, "0");
  return `${minutes}:${seconds}`;
}

function describeWarning(warning) {
  if (
    warning.code === "FRAME_EXTRACTION_FAILED" &&
    Number.isFinite(warning.timestamp_sec)
  ) {
    return `The frame at ${formatTime(warning.timestamp_sec)} could not be captured and was left out.`;
  }
  return warning.message || "A minor issue occurred.";
}

function describeError(status) {
  if (status.status === "DEAD_LETTERED") {
    return "This video could not be processed after several attempts.";
  }
  const error = status.error || {};
  return ERROR_MESSAGES[error.code] || error.message || "Something went wrong.";
}

function setBanner(kind, lines) {
  const banner = $("banner");
  if (!kind) {
    banner.hidden = true;
    banner.replaceChildren();
    return;
  }
  banner.className = `banner ${kind}`;
  banner.replaceChildren(
    ...lines.map((text) => {
      const line = document.createElement("div");
      line.textContent = text; 
      return line;
    }),
  );
  banner.hidden = false;
}

function renderStages(state) {
  const failed = isFailed(state);
  const finished = isFinished(state);
  const current = finished ? DONE_INDEX : STAGE_ORDER.indexOf(state);

  document.querySelectorAll("#stages li").forEach((li, index) => {
    li.classList.toggle("done", !failed && (finished || index < current));
    li.classList.toggle("active", !failed && !finished && index === current);
  });
}

function renderLink(status) {
  const link = $("notion-link");
  link.hidden = true;
  const url = status.result && status.result.notion_page_url;
  if (!isFinished(status.status) || !url) return;
  try {
    if (new URL(url).protocol !== "https:") return; // never put a non-https link in the UI
  } catch (err) {
    return;
  }
  link.href = url;
  link.hidden = false;
}

function render(status) {
  $("empty-state").hidden = Boolean(status);
  $("job-view").hidden = !status;
  if (!status) return;

  const state = status.status;
  const failed = isFailed(state);
  const finished = isFinished(state);
  const progress = finished ? 100 : failed ? 0 : status.progress || 0;

  $("video-title").textContent =
    status.video_title || "Preparing your video...";
  $("progress-bar").style.width = `${progress}%`;
  $("progress-text").textContent = failed
    ? "Stopped"
    : finished
      ? "Done"
      : `${progress}% - ${STAGE_LABELS[state] || "Working"}`;

  renderStages(state);
  renderLink(status);

  if (failed) {
    setBanner("error", [describeError(status)]);
  } else if (state === "COMPLETED_WITH_WARNINGS") {
    const warnings = (status.warnings || []).map(describeWarning);
    setBanner("warning", ["Completed with warnings:", ...warnings.slice(0, 3)]);
  } else {
    setBanner(null);
  }
}

function showConnectionProblem(message) {
  const box = $("conn-error");
  box.textContent = message || "";
  box.hidden = !message;
}

async function refresh() {
  try {
    const response = await chrome.runtime.sendMessage({ type: "GET_STATUS" });
    if (response && response.success) {
      showConnectionProblem("");
      render(response.status);
      if (
        !response.status ||
        isFinished(response.status.status) ||
        isFailed(response.status.status)
      ) {
        stopPolling();
      }
    } else {
      showConnectionProblem(
        (response && response.error) || "Could not load the status.",
      );
    }
  } catch (err) {
    showConnectionProblem(
      "Could not talk to the extension. Try reopening the popup.",
    );
  }
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(refresh, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function loadSettings() {
  const settings = await chrome.storage.local.get({
    api_base_url: "http://localhost:8000",
    notion_token: "",
  });
  $("api-input").value = settings.api_base_url;
  $("token-input").placeholder = settings.notion_token
    ? "Saved - type to replace"
    : "secret_...";
}

async function saveSettings() {
  const status = $("settings-status");
  const apiBase = $("api-input").value.trim().replace(/\/+$/, "");

  try {
    const url = new URL(apiBase);
    if (url.protocol !== "http:" && url.protocol !== "https:")
      throw new Error("bad protocol");
  } catch (err) {
    status.textContent =
      "Enter a valid server address, e.g. http://localhost:8000";
    return;
  }

  const updates = { api_base_url: apiBase };
  const token = $("token-input").value.trim();
  if (token) updates.notion_token = token;

  await chrome.storage.local.set(updates);
  $("token-input").value = "";
  await loadSettings();
  status.textContent = "Saved.";
}

async function init() {
  $("save-settings").addEventListener("click", saveSettings);
  loadSettings();

  const { active_job_status } =
    await chrome.storage.local.get("active_job_status");
  render(active_job_status || null);
  await refresh();
  if (
    active_job_status &&
    !isFinished(active_job_status.status) &&
    !isFailed(active_job_status.status)
  ) {
    startPolling();
  }

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === "local" && changes.active_job_id) {
      refresh();
      startPolling();
    }
  });
}

init();
