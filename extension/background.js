
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

async function startJob(videoId, title) {
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
      video_title: title,
    }),
  });

  const newEntry = {
    job_id: job.job_id,
    video_id: videoId,
    video_title: title || "Preparing video...",
    status: job.status || "QUEUED",
    progress: 0,
    created_at: Date.now(),
    warnings: [],
    error: null,
    result: null,
  };

  const stored = await chrome.storage.local.get({ jobs_list: [] });
  let jobsList = Array.isArray(stored.jobs_list) ? stored.jobs_list : [];
  jobsList = [newEntry, ...jobsList.filter((j) => j.job_id !== job.job_id)].slice(0, 10);

  await chrome.storage.local.set({
    active_job_id: job.job_id,
    active_job_status: newEntry,
    jobs_list: jobsList,
  });

  await chrome.alarms.create(POLL_ALARM, { periodInMinutes: 0.5 });
  return { ...job, video_title: newEntry.video_title };
}

async function refreshJobs() {
  const data = await chrome.storage.local.get({
    active_job_id: null,
    active_job_status: null,
    jobs_list: [],
  });

  let jobsList = Array.isArray(data.jobs_list) ? data.jobs_list : [];
  let activeJobId = data.active_job_id;
  let activeStatus = data.active_job_status;

  if (jobsList.length === 0 && !activeJobId) {
    await chrome.alarms.clear(POLL_ALARM);
    return { active_status: null, jobs: [] };
  }

  if (jobsList.length === 0 && activeJobId) {
    jobsList = [activeStatus || { job_id: activeJobId, status: "QUEUED", progress: 0 }];
  }

  let hasNonTerminal = false;
  const updatedList = await Promise.all(
    jobsList.map(async (item) => {
      if (TERMINAL_STATES.includes(item.status)) {
        return item;
      }
      try {
        const fresh = await apiRequest(`/jobs/${item.job_id}`);
        const freshHasCyrillic = /[а-яА-ЯёЁ]/.test(fresh.video_title || "");
        const itemHasLatin = /[a-zA-Z]/.test(item.video_title || "") && item.video_title !== "Preparing video...";
        let chosenTitle = fresh.video_title || item.video_title || "YouTube Video";
        if (freshHasCyrillic) {
          if (itemHasLatin) {
            chosenTitle = item.video_title;
          } else {
            chosenTitle = "YouTube Video";
          }
        }
        const merged = {
          ...item,
          ...fresh,
          video_title: chosenTitle,
        };
        if (!TERMINAL_STATES.includes(merged.status)) {
          hasNonTerminal = true;
        }
        return merged;
      } catch (err) {
        if (err.status === 404) {
          return null;
        }
        hasNonTerminal = true;
        return item;
      }
    }),
  );

  const cleanList = updatedList.filter(Boolean);

  if (activeJobId) {
    const activeEntry = cleanList.find((j) => j.job_id === activeJobId);
    if (activeEntry) {
      activeStatus = activeEntry;
    } else if (cleanList.length > 0) {
      activeJobId = cleanList[0].job_id;
      activeStatus = cleanList[0];
    } else {
      activeJobId = null;
      activeStatus = null;
    }
  } else if (cleanList.length > 0) {
    activeJobId = cleanList[0].job_id;
    activeStatus = cleanList[0];
  }

  await chrome.storage.local.set({
    jobs_list: cleanList,
    active_job_id: activeJobId,
    active_job_status: activeStatus,
  });

  if (!hasNonTerminal) {
    await chrome.alarms.clear(POLL_ALARM);
  }

  return { active_status: activeStatus, jobs: cleanList };
}

async function selectJob(jobId) {
  const data = await chrome.storage.local.get({ jobs_list: [] });
  const jobsList = Array.isArray(data.jobs_list) ? data.jobs_list : [];
  const selected = jobsList.find((j) => j.job_id === jobId);
  if (!selected) return null;

  await chrome.storage.local.set({
    active_job_id: selected.job_id,
    active_job_status: selected,
  });

  return selected;
}

async function clearFinishedJobs() {
  const data = await chrome.storage.local.get({
    jobs_list: [],
    active_job_id: null,
  });
  let jobsList = Array.isArray(data.jobs_list) ? data.jobs_list : [];
  const remaining = jobsList.filter((j) => !TERMINAL_STATES.includes(j.status));
  let activeId = data.active_job_id;
  let activeStatus = null;

  if (activeId && remaining.some((j) => j.job_id === activeId)) {
    activeStatus = remaining.find((j) => j.job_id === activeId);
  } else if (remaining.length > 0) {
    activeId = remaining[0].job_id;
    activeStatus = remaining[0];
  } else {
    activeId = null;
    activeStatus = null;
  }

  await chrome.storage.local.set({
    jobs_list: remaining,
    active_job_id: activeId,
    active_job_status: activeStatus,
  });

  return { active_status: activeStatus, jobs: remaining };
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
    startJob(message.videoId, message.title)
      .then((job) => sendResponse({ success: true, job_id: job.job_id, video_title: job.video_title }))
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
    refreshJobs()
      .then(({ active_status, jobs }) =>
        sendResponse({ success: true, status: active_status, jobs }),
      )
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true;
  }
  if (message.type === "SELECT_JOB") {
    selectJob(message.job_id)
      .then((status) => sendResponse({ success: true, status }))
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true;
  }
  if (message.type === "CLEAR_FINISHED_JOBS") {
    clearFinishedJobs()
      .then(({ active_status, jobs }) =>
        sendResponse({ success: true, status: active_status, jobs }),
      )
      .catch((err) => sendResponse({ success: false, error: err.message }));
    return true;
  }
  return false;
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === POLL_ALARM) {
    refreshJobs().catch((err) =>
      console.warn("[KeyFrame-AI] poll failed:", err.message),
    );
  }
});
