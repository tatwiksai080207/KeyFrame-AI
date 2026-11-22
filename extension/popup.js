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
  QUEUED: "Waiting in queue",
  TRANSCRIBING: "Extracting transcript",
  SUMMARIZING: "Writing study summary",
  EXTRACTING_FRAMES: "Capturing key frames",
  PUBLISHING: "Creating Notion page",
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
  const error = status.error || {};
  const msg = (error.message || "").toLowerCase();
  if (msg.includes("blocking requests") || msg.includes("too many requests") || msg.includes("429")) {
    return "YouTube temporarily rate limited caption fetching for this IP. Please wait a few minutes before retrying.";
  }
  if (status.status === "DEAD_LETTERED") {
    return error.message || "This video could not be processed after several attempts.";
  }
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
      : `${progress}%`;

  const badge = $("job-badge");
  if (badge) {
    if (failed) {
      badge.textContent = "Stopped";
      badge.className = "state-badge badge--error";
    } else if (state === "COMPLETED_WITH_WARNINGS") {
      badge.textContent = "Warnings";
      badge.className = "state-badge badge--warning";
    } else if (finished) {
      badge.textContent = "Done";
      badge.className = "state-badge badge--success";
    } else {
      badge.textContent = "Processing";
      badge.className = "state-badge";
    }
  }

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

function renderJobsList(jobs, activeJobId) {
  const section = $("jobs-section");
  const list = $("jobs-list");
  const countBadge = $("jobs-count-badge");

  if (!jobs || jobs.length <= 1) {
    if (section) section.hidden = true;
    return;
  }

  if (section) section.hidden = false;
  if (countBadge) {
    countBadge.textContent = `${jobs.length} videos`;
  }

  list.replaceChildren(
    ...jobs.map((job) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "job-chip";
      chip.setAttribute("role", "tab");
      const isActive = job.job_id === activeJobId;
      if (isActive) {
        chip.classList.add("is-active");
        chip.setAttribute("aria-selected", "true");
      } else {
        chip.setAttribute("aria-selected", "false");
      }

      const dot = document.createElement("span");
      dot.className = "job-chip-dot";
      let statusDesc = "Queued";

      if (isFinished(job.status)) {
        dot.classList.add("dot--done");
        statusDesc = "Done";
      } else if (isFailed(job.status)) {
        dot.classList.add("dot--error");
        statusDesc = "Stopped";
      } else {
        dot.classList.add("dot--running");
        const stageLabel = STAGE_LABELS[job.status] || "Processing";
        statusDesc = job.progress ? `${stageLabel} · ${job.progress}%` : stageLabel;
      }

      const body = document.createElement("span");
      body.className = "job-chip-body";

      const title = document.createElement("span");
      title.className = "job-chip-title";
      title.textContent = job.video_title || "YouTube Video";

      const statusEl = document.createElement("span");
      statusEl.className = "job-chip-status";
      statusEl.textContent = statusDesc;

      body.appendChild(title);
      body.appendChild(statusEl);

      chip.appendChild(dot);
      chip.appendChild(body);

      chip.addEventListener("click", async () => {
        if (job.job_id === activeJobId) return;
        render(job);
        renderJobsList(jobs, job.job_id);
        try {
          await chrome.runtime.sendMessage({
            type: "SELECT_JOB",
            job_id: job.job_id,
          });
        } catch (err) {
          // ignore
        }
        refresh();
      });

      return chip;
    }),
  );
}

async function refresh() {
  try {
    const response = await chrome.runtime.sendMessage({ type: "GET_STATUS" });
    if (response && response.success) {
      showConnectionProblem("");
      const jobs = response.jobs || [];
      const activeStatus = response.status;
      render(activeStatus);
      renderJobsList(jobs, activeStatus ? activeStatus.job_id : null);

      const anyRunning = jobs.some(
        (j) => !isFinished(j.status) && !isFailed(j.status),
      );
      if (!anyRunning) {
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

async function renderConnection() {
  const { notion_token, notion_workspace } = await chrome.storage.local.get({
    notion_token: "",
    notion_workspace: "",
  });
  const connected = Boolean(notion_token);
  const statusEl = $("connection-status");
  const btn = $("connect-notion");
  const widget = $("connect");

  if (statusEl) {
    statusEl.textContent = connected
      ? (notion_workspace || "Connected")
      : "Not connected";
  }
  if (btn) {
    btn.textContent = connected ? "Change" : "Connect";
  }
  if (widget) {
    widget.classList.toggle("is-connected", connected);
    widget.title = connected
      ? `Connected to ${notion_workspace || "Notion"}`
      : "Notion account not connected";
  }
}

async function connectNotion() {
  const button = $("connect-notion");
  button.disabled = true;
  button.textContent = "Connecting...";
  try {
    const response = await chrome.runtime.sendMessage({
      type: "CONNECT_NOTION",
    });
    showConnectionProblem(
      response && response.success
        ? ""
        : (response && response.error) || "Could not connect to Notion.",
    );
  } catch (err) {
    // Popup closes during Notion OAuth; background continues
  }
  button.disabled = false;
  renderConnection();
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
    status.style.color = "var(--red)";
    return;
  }

  const updates = { api_base_url: apiBase };
  const token = $("token-input").value.trim();
  if (token) updates.notion_token = token;

  await chrome.storage.local.set(updates);
  $("token-input").value = "";
  await loadSettings();
  status.textContent = "Saved";
  status.style.color = "var(--green)";
  setTimeout(() => {
    if (status.textContent === "Saved") status.textContent = "";
  }, 2500);
}

async function init() {
  $("save-settings").addEventListener("click", saveSettings);
  loadSettings();

  $("connect-notion").addEventListener("click", connectNotion);
  renderConnection();

  const clearBtn = $("clear-finished-btn");
  if (clearBtn) {
    clearBtn.addEventListener("click", async () => {
      try {
        const response = await chrome.runtime.sendMessage({
          type: "CLEAR_FINISHED_JOBS",
        });
        if (response && response.success) {
          render(response.status);
          renderJobsList(response.jobs, response.status ? response.status.job_id : null);
        }
      } catch (err) {
        // ignore
      }
    });
  }

  const { active_job_status, jobs_list, active_job_id } =
    await chrome.storage.local.get({
      active_job_status: null,
      jobs_list: [],
      active_job_id: null,
    });
  render(active_job_status || null);
  renderJobsList(jobs_list, active_job_id);

  await refresh();

  const hasRunning = (jobs_list || []).some(
    (j) => !isFinished(j.status) && !isFailed(j.status),
  );
  if (
    hasRunning ||
    (active_job_status &&
      !isFinished(active_job_status.status) &&
      !isFailed(active_job_status.status))
  ) {
    startPolling();
  }

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area !== "local") return;
    if (changes.notion_token || changes.notion_workspace) renderConnection();
    if (changes.active_job_id || changes.jobs_list) {
      refresh();
      startPolling();
    }
  });
}

init();
