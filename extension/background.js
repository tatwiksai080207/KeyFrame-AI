
// KeyFrame-AI background service worker.

const DEFAULT_API_BASE = "http://localhost:8000";
const POLL_ALARM = "keyframe-poll";
const TERMINAL_STATES = [
  "COMPLETED",
  "COMPLETED_WITH_WARNINGS",
  "FAILED",
  "DEAD_LETTERED",
];

async function getSettings() {
  return chrome.storage.local.get({
    api_base_url: DEFAULT_API_BASE,
    notion_token: "",
  });
}

// Calls the gateway
async function apiRequest(path, options) {
  const { api_base_url } = await getSettings();
  let response;
  try {
    response = await fetch(api_base_url + path, options);
  } catch (err) {
    throw new Error("Cannot reach the KeyFrame-AI server. Is it running?");
  }
  let body = null;
  try {
    body = await response.json();
  } catch (err) {

  }
  if (!response.ok) {
    const message = body && body.error && body.error.message;
    const error = new Error(message || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return body;
}

async function startJob(videoId) {
  const { notion_token } = await getSettings();
  if (!notion_token) {
    throw new Error("Connect Notion first.");
  }
  const job = await apiRequest("/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      youtube_url: `https://www.youtube.com/watch?v=${videoId}`,
      notion_token,
    }),
  });
  await chrome.storage.local.set({
    active_job_id: job.job_id,
    active_job_status: {
      job_id: job.job_id,
      status: job.status,
      progress: 0,
      warnings: [],
    },
  });
  await chrome.alarms.create(POLL_ALARM, { periodInMinutes: 0.5 });
  return job;
}

async function refreshActiveJob() {
  const { active_job_id } = await chrome.storage.local.get("active_job_id");
  if (!active_job_id) {
    await chrome.alarms.clear(POLL_ALARM);
    return null;
  }
  try {
    const status = await apiRequest(`/jobs/${active_job_id}`);
    await chrome.storage.local.set({ active_job_status: status });
    if (TERMINAL_STATES.includes(status.status)) {
      await chrome.alarms.clear(POLL_ALARM);
    }
    return status;
  } catch (err) {
    if (err.status === 404) {
      await chrome.storage.local.remove(["active_job_id", "active_job_status"]);
      await chrome.alarms.clear(POLL_ALARM);
    }
    throw err;
  }
}

// One-time Notion sign-in
async function connectNotion() {
  const { client_id } = await apiRequest("/auth/notion/config");
  const redirectUri = chrome.identity.getRedirectURL("notion");
  const state = crypto.randomUUID(); // protects against forged redirects (CSRF)

  const authUrl =
    "https://api.notion.com/v1/oauth/authorize?" +
    new URLSearchParams({
      client_id,
      response_type: "code",
      owner: "user",
      redirect_uri: redirectUri,
      state,
    });

  let responseUrl;
  try {
    responseUrl = await chrome.identity.launchWebAuthFlow({ url: authUrl, interactive: true });
  } catch (err) {
    throw new Error("Notion sign-in was cancelled.");
  }

  const params = new URL(responseUrl).searchParams;
  if (params.get("error")) throw new Error("Notion access was not granted.");
  if (params.get("state") !== state) throw new Error("Sign-in was interrupted. Please try again.");

  const result = await apiRequest("/auth/notion/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code: params.get("code"), redirect_uri: redirectUri }),
  });

  await chrome.storage.local.set({
    notion_token: result.access_token,
    notion_workspace: result.workspace_name || "Notion",
  });
  return result.workspace_name;
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "START_JOB") {
    startJob(message.videoId)
      .then((job) => sendResponse({ success: true, job_id: job.job_id }))
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true; 
  }
  if (message.type === "CONNECT_NOTION") {
    connectNotion()
      .then((workspace) => sendResponse({ success: true, workspace }))
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true;
  }
  if (message.type === "GET_STATUS") {
    refreshActiveJob()
      .then((status) => sendResponse({ success: true, status }))
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true;
  }
  return false;
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === POLL_ALARM) {
    refreshActiveJob().catch((err) =>
      console.warn("[KeyFrame-AI] poll failed:", err.message),
    );
  }
});
