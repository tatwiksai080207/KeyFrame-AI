# KeyFrame AI: Distributed Multimodal Video Note Taking Agent

KeyFrame AI is an asynchronous, distributed system that transforms YouTube videos into structured Notion study notes combining organized text summaries, key resource links, and real extracted video frames of informative slides or architecture diagrams paired with explanatory captions.

The system is driven entirely from an unpacked Google Chrome browser extension. The backend processes tasks asynchronously across a message queue backed worker pool with distributed rate limiting, fault tolerance, and automatic crash recovery.

---

## 1. System Architecture

The application consists of four independent runtime components orchestrated through Docker Compose:

1. **Browser Extension (Client Layer):** An unpacked Manifest V3 Chrome extension that injects a Take Notes action button directly into the YouTube video player toolbar, performs one time Notion OAuth authentication, and tracks real time progress through a multi stage popup interface.
2. **API Gateway (Ingress Layer):** A FastAPI service that validates YouTube URLs, persists initial job states to PostgreSQL, places processing tasks onto RabbitMQ queues, and provides status, result, and health check endpoints. The gateway accepts submissions immediately without performing heavy pipeline work synchronously.
3. **Message Broker (Communication Layer):** A RabbitMQ broker configuring durable direct exchanges (`notes.direct`), dead letter exchanges (`notes.dlx`), and dedicated per stage queues (`jobs.transcribe`, `jobs.summarize`, `jobs.frame_extract`, `jobs.publish`, `jobs.dlq`).
4. **Worker Pool (Execution Layer):** A scalable cluster of independent worker processes (minimum two instances running concurrently). Each worker pulls tasks from RabbitMQ stage queues with prefetch limits, performs distributed rate limited calls, executes direct stream video seeking via FFmpeg, and updates persistent PostgreSQL job state.

---

## 2. Base URL and Service Endpoints

* **Gateway Base URL:** `http://localhost:8000`
* **Health Check Endpoint:** `GET http://localhost:8000/healthz`
* **Job Submission Endpoint:** `POST http://localhost:8000/jobs`
* **Job Polling Endpoint:** `GET http://localhost:8000/jobs/{job_id}`
* **Job Outcome Endpoint:** `GET http://localhost:8000/jobs/{job_id}/results`
* **RabbitMQ Management Dashboard:** `http://localhost:15672` (Username: `guest`, Password: `guest`)

---

## 3. Notion Destination Behavior

When a job is submitted:
1. If the submission supplies a `notion_parent_id`, the study notes page is published inside that designated Notion page or database.
2. If `notion_parent_id` is omitted, the worker automatically publishes the page under the default workspace page defined by the environment variable `NOTION_DEFAULT_PARENT_PAGE_ID`.
3. The default target page provided in the environment configuration is:
   `4167e1b37a1480e385bfc65f2bb279f8`
   This corresponds to the designated Notion workspace parent page granted during integration setup.

---

## 4. Required Environment Variables

All services load configuration from the `.env` file at the repository root. A template is provided in `.env.example`.

| Variable Name | Required | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `DATABASE_URL` | Yes | `postgresql+asyncpg://postgres:postgres@postgres:5432/notes_agent` | Asynchronous PostgreSQL connection string. |
| `RABBITMQ_URL` | Yes | `amqp://guest:guest@rabbitmq:5672/` | AMQP connection string for message broker. |
| `REDIS_URL` | Yes | `redis://redis:6379/0` | Redis connection URL for distributed rate limiting and state coordination. |
| `LLM_PROVIDER` | Yes | `gemini` | Multimodal synthesis provider (`gemini` or `openai`). |
| `GEMINI_API_KEY` | Yes (if Gemini) | `""` | Google AI Studio API key. |
| `OPENAI_API_KEY` | Optional | `""` | OpenAI API key when provider is set to openai. |
| `LLM_MODEL` | Yes | `gemini-3.5-flash-lite` | Synthesis model. Uses lightweight, high quota tier with automatic fallback. |
| `LLM_RATE_LIMIT_RPM` | Yes | `15` | Maximum allowed LLM requests per minute enforced across all workers via Redis token bucket. |
| `NOTION_OAUTH_CLIENT_ID` | Yes | Configured in `.env` | Notion public OAuth integration client ID. |
| `NOTION_OAUTH_CLIENT_SECRET` | Yes | Configured in `.env` | Notion integration client secret. |
| `NOTION_DEFAULT_PARENT_PAGE_ID` | Yes | `4167e1b37a1480e385bfc65f2bb279f8` | Default Notion page ID where notes are created when no parent is supplied. |
| `FRAME_STORAGE_DIR` | Yes | `/shared/frames` | Directory path shared between gateway and workers for extracted video frames. |
| `MAX_VIDEO_DURATION_SEC` | Yes | `1200` | Strict Constraint A duration boundary (1200 seconds / 20 minutes). |
| `MAX_RETRY_ATTEMPTS` | Yes | `5` | Maximum retry attempts before routing an unrecoverable job to the Dead Letter Queue. |
| `RETRY_BACKOFF_SCHEDULE` | Yes | `[10, 30, 90]` | Exponential backoff delay schedule in seconds for transient errors and HTTP 429 rate limits. |

---

## 5. Quick Start Instructions

A grader or evaluator can start the full system from the terminal using the steps below without modifying source code.

### Step 1: Verify Prerequisites
Ensure Docker, Docker Compose, and Google Chrome are installed on your machine.

### Step 2: Configure Environment
Confirm that the `.env` file exists at the root of the project with your valid `GEMINI_API_KEY` populated.

### Step 3: Launch Backend Infrastructure
Run the following command from the repository root:

```bash
docker compose up -d --build
```

This starts:
* PostgreSQL on port 5432
* Redis on port 6379
* RabbitMQ on ports 5672 and 15672
* API Gateway on port 8000
* Two independent worker processes (`cloudnov_20-worker-1` and `cloudnov_20-worker-2`)

### Step 4: Verify System Health
Run a sanity check against the health endpoint:

```bash
curl -s http://localhost:8000/healthz
```

Expected response:
```json
{"status": "healthy", "postgres": "ok", "redis": "ok", "rabbitmq": "ok"}
```

### Step 5: Install Chrome Browser Extension
1. Open Google Chrome and navigate to `chrome://extensions/`.
2. Enable **Developer mode** toggle in the top right corner.
3. Click **Load unpacked** in the top left corner.
4. Select the `extension` folder located inside this project directory (`c:\Projects\Cloud Nov_20\extension`).
5. The KeyFrame AI icon will appear in your browser toolbar.

### Step 6: Process a Video
1. Open any YouTube video under 20 minutes (for example, search for any programming or system tutorial).
2. Click the blue **Take Notes** button injected directly inside the video player toolbar (or click the extension icon and authenticate with Notion).
3. Open the extension popup to watch the live human readable progression:
   `Queued` -> `Transcribing` -> `Summarizing` -> `Extracting Frames` -> `Publishing` -> `Done`
4. When finished, click the **View in Notion** button to open the published study notes with embedded slides and captions.

---

## 6. Verification of Graded Failure Scenarios and Demonstrations

The system includes automated tests and live mechanisms for all four mandatory failure scenarios:

### Scenario 1: External Rate Limits (HTTP 429)
* **Behavior:** When an external service (YouTube, Gemini, or Notion) returns HTTP 429 or quota warnings, the worker does not fail the job. Instead, the task is requeued with exponential backoff delay (`10s`, `30s`, `90s`) while retry budget remains. The popup UI keeps the job in an active waiting state rather than surfacing an error.
* **Demonstration:** Submit multiple videos in rapid succession. The distributed Redis token bucket rate limiter coordinates LLM calls across all workers so that global throughput never exceeds the configured rate limit.

### Scenario 2: Worker Crash Recovery (SIGKILL Mid Pipeline)
* **Behavior:** RabbitMQ tasks use manual acknowledgment (`message.ack()`). Acknowledgment occurs strictly after a stage's computation commits durably to PostgreSQL and the subsequent stage message is published.
* **Demonstration:**
  1. Submit a video job.
  2. While the worker is actively extracting frames or summarizing, kill one worker container:
     ```bash
     docker kill cloudnov_20-worker-1
     ```
  3. RabbitMQ detects the channel disconnection immediately and reassigns the unacknowledged message to `cloudnov_20-worker-2`.
  4. The second worker resumes the stage seamlessly. The job completes with zero lost tasks and zero duplicate pages on Notion.

### Scenario 3: Graceful Degradation on Missing Video Frames
* **Behavior:** If one or more video frames cannot be captured over HTTP stream seeking (due to network throttling or unseekable chunks), the worker does not abort the job. It logs the failed timestamp, records an informative warning, and completes publication with whatever frames succeeded. If all frames fail, a clean text only study document is published.
* **Demonstration:** The popup UI displays `Completed with warnings` and provides clear explanations (e.g. `The frame at 13:42 could not be captured and was left out`), accompanied by a functional Notion page URL.

### Scenario 4: Dead Letter Queue Routing (Bounded Retries)
* **Behavior:** If a job fails permanently after 5 attempts (for example, attempting to process a private or deleted video), the worker stops retrying, marks the job status as `DEAD_LETTERED`, and routes the payload to `jobs.dlq` via `notes.dlx` for operator inspection.
* **Demonstration:** Submit a private or invalid video URL. After the bounded retry budget is exhausted, the job transitions to `DEAD_LETTERED` and appears in the RabbitMQ management dashboard under `jobs.dlq`.

---

## 7. Hard Constraints Verification

* **Constraint A (Video Length):** Videos up to 20 minutes (1200 seconds) are supported. Videos longer than 20 minutes are deterministically rejected in Stage 1 with error code `VIDEO_TOO_LONG` without downloading or processing heavy assets.
* **Constraint B (Real Frames Only):** Every image appearing on the final Notion page is an authentic frame extracted directly from the video stream using FFmpeg. Images are never artificially generated or substituted. Every inserted slide is accompanied by an explanatory LLM caption describing what is shown and why it was flagged.

---

## 8. Automated Test Suite

Run the complete test suite locally:

```bash
pytest -v
```

All 63 automated unit and integration tests validate constraints, API contracts, consumer manual acks, fault tolerance, and pipeline execution.
