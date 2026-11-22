# KeyFrame AI: Distributed Multimodal Video Note Taking Agent

KeyFrame AI is an asynchronous, distributed system that transforms YouTube educational lectures and technical tutorials into structured Notion study notes. The generated notes combine executive summaries, technical specifications, resource reference tables, and real video frames of architecture diagrams and slides embedded directly within the text, each accompanied by detailed explanatory captions.

The entire experience is driven from an unpacked Manifest V3 Google Chrome browser extension. The backend processes tasks asynchronously across a message queue backed worker pool equipped with distributed rate limiting, fault tolerance, and automatic crash recovery.

---

## Table of Contents

* [1. Executive Overview](#1-executive-overview)
* [2. System Architecture](#2-system-architecture)
  * [2.1 High Level Architectural Diagram](#21-high-level-architectural-diagram)
  * [2.2 Core Runtime Components](#22-core-runtime-components)
* [3. End to End Pipeline Lifecycle](#3-end-to-end-pipeline-lifecycle)
  * [3.1 Stage Progression Sequence](#31-stage-progression-sequence)
  * [3.2 Stage 1: Transcript Extraction and Length Validation](#32-stage-1-transcript-extraction-and-length-validation)
  * [3.3 Stage 2: Multimodal LLM Synthesis and Key Moment Identification](#33-stage-2-multimodal-llm-synthesis-and-key-moment-identification)
  * [3.4 Stage 3: Intelligent Video Frame Extraction and Curation](#34-stage-3-intelligent-video-frame-extraction-and-curation)
  * [3.5 Stage 4: Structured Notion Publishing and In Text Placement](#35-stage-4-structured-notion-publishing-and-in-text-placement)
* [4. Advanced Frame Extraction Architecture](#4-advanced-frame-extraction-architecture)
  * [4.1 Direct Stream Seeking via FFmpeg](#41-direct-stream-seeking-via-ffmpeg)
  * [4.2 Vision Language Model Quality and Completeness Evaluation](#42-vision-language-model-quality-and-completeness-evaluation)
  * [4.3 Progressive Reveal and Superseded Slide Deduplication](#43-progressive-reveal-and-superseded-slide-deduplication)
  * [4.4 Frame Curation Decision Flow](#44-frame-curation-decision-flow)
* [5. API Gateway Specification](#5-api-gateway-specification)
  * [5.1 Base URL and Endpoints Summary](#51-base-url-and-endpoints-summary)
  * [5.2 Job Submission: POST /jobs](#52-job-submission-post-jobs)
  * [5.3 Job Status Polling: GET /jobs/{job_id}](#53-job-status-polling-get-jobsjob_id)
  * [5.4 Job Outcome Retrieval: GET /jobs/{job_id}/results](#54-job-outcome-retrieval-get-jobsjob_idresults)
  * [5.5 System Health Check: GET /healthz](#55-system-health-check-get-healthz)
* [6. Notion Integration and Output Design](#6-notion-integration-and-output-design)
  * [6.1 One Time OAuth Flow](#61-one-time-oauth-flow)
  * [6.2 Destination Page Routing Behavior](#62-destination-page-routing-behavior)
  * [6.3 Structured Study Notes Block Layout](#63-structured-study-notes-block-layout)
* [7. Distributed Resilience and Failure Handling](#7-distributed-resilience-and-failure-handling)
  * [7.1 External Rate Limits and Exponential Backoff (HTTP 429)](#71-external-rate-limits-and-exponential-backoff-http-429)
  * [7.2 Worker Crash Recovery and Task Idempotency](#72-worker-crash-recovery-and-task-idempotency)
  * [7.3 Graceful Degradation on Frame Failure](#73-graceful-degradation-on-frame-failure)
  * [7.4 Dead Letter Queue (DLQ) Bounded Retries](#74-dead-letter-queue-dlq-bounded-retries)
  * [7.5 Coordinated Global Rate Limiting via Redis](#75-coordinated-global-rate-limiting-via-redis)
* [8. Hard Constraints Enforcement](#8-hard-constraints-enforcement)
  * [8.1 Constraint A: 20 Minute Video Duration Cap](#81-constraint-a-20-minute-video-duration-cap)
  * [8.2 Constraint B: Authentic Frames with Explanatory Captions](#82-constraint-b-authentic-frames-with-explanatory-captions)
* [9. Environment Variables and Configuration](#9-environment-variables-and-configuration)
* [10. Step by Step Quick Start Guide](#10-step-by-step-quick-start-guide)
  * [10.1 Prerequisites](#101-prerequisites)
  * [10.2 Environment Configuration](#102-environment-configuration)
  * [10.3 Launching the Infrastructure with Docker Compose](#103-launching-the-infrastructure-with-docker-compose)
  * [10.4 Verifying Service Health](#104-verifying-service-health)
  * [10.5 Installing the Chrome Browser Extension](#105-installing-the-chrome-browser-extension)
  * [10.6 Processing Your First Video](#106-processing-your-first-video)
* [11. Testing and Verification](#11-testing-and-verification)
  * [11.1 Running the Automated Test Suite](#111-running-the-automated-test-suite)
  * [11.2 Live Demonstrations of Graded Scenarios](#112-live-demonstrations-of-graded-scenarios)
* [12. Troubleshooting and Frequently Asked Questions](#12-troubleshooting-and-frequently-asked-questions)

---

## 1. Executive Overview

Technical video lectures often convey their most critical architectural blueprints, sequence diagrams, code listings, and system tradeoffs visually on slides. Pure text transcriptions miss these visual artifacts, while generic screenshot utilities capture talking heads, logos, and partial animations.

KeyFrame AI bridges this gap through a distributed, automated architecture:
* A Chrome Extension injects a native Take Notes button directly into YouTube watch pages.
* A FastAPI Gateway validates video links and enqueues tasks immediately without blocking the client.
* A RabbitMQ Message Broker orchestrates stage tasks with durable queues, dead letter routing, and at least once delivery guarantees.
* Independent Worker Processes execute transcript retrieval, LLM summarization, direct stream video seeking via FFmpeg, multimodal visual validation, and structured publishing to Notion.
* A Vision Language Model rigorously filters out talking heads, logos, and incomplete progressive animation frames so that only fully assembled technical diagrams are published.
* Real images are placed directly underneath their matching bullet points inside the text rather than lumped into a detached visual gallery.

---

## 2. System Architecture

The system is structured as four independently scalable tiers running in isolated processes and containers via Docker Compose.

### 2.1 High Level Architectural Diagram

```mermaid
flowchart TD
    subgraph Client ["Client Tier (Browser)"]
        YT["YouTube Watch Page"] -->|Take Notes Click| CS["Content Script (content.js)"]
        CS -->|START_JOB Event| BG["Background Worker (background.js)"]
        BG -->|Poll Status| POP["Popup Stepper UI (popup.js)"]
    end

    subgraph Ingress ["API Gateway Tier (FastAPI)"]
        BG -->|POST /jobs| GW["API Gateway (port 8000)"]
        BG -->|GET /jobs/{id}| GW
        GW -->|Write Initial Job| PG[("PostgreSQL Database")]
        GW -->|Publish Stage Task| MQ{{"RabbitMQ Broker"}}
    end

    subgraph Broker ["Message Broker Tier (RabbitMQ)"]
        MQ -->|Queue: jobs.transcribe| Q1["jobs.transcribe"]
        MQ -->|Queue: jobs.summarize| Q2["jobs.summarize"]
        MQ -->|Queue: jobs.frame_extract| Q3["jobs.frame_extract"]
        MQ -->|Queue: jobs.publish| Q4["jobs.publish"]
        MQ -.->|Exhausted Retries| DLQ["jobs.dlq (DLX)"]
    end

    subgraph Workers ["Worker Pool Tier (Python 3.12)"]
        W1["Worker Container 1"]
        W2["Worker Container 2"]
        Q1 --> W1 & W2
        Q2 --> W1 & W2
        Q3 --> W1 & W2
        Q4 --> W1 & W2
    end

    subgraph Coordination ["Coordination and External Services"]
        W1 & W2 <-->|Rate Limit Tokens| RD[("Redis 7")]
        W1 & W2 <-->|Update State and Progress| PG
        W1 & W2 -->|Fetch Captions| YTAPI["YouTube Transcript API"]
        W1 & W2 -->|Multimodal VLM| LLM["Google Gemini / OpenAI"]
        W1 & W2 -->|Direct Stream Seek| FFMPEG["FFmpeg Binary"]
        W1 & W2 -->|Create Structured Page| NOTION["Notion REST API"]
    end
```

### 2.2 Core Runtime Components

1. **Browser Extension:** An unpacked Manifest V3 extension containing a content script, a background service worker, and a dark themed popup UI. The extension manages Notion OAuth tokens securely in `chrome.storage.local` and coordinates job submissions.
2. **API Gateway:** An asynchronous FastAPI web service running on port 8000. It performs input validation, creates the initial job record in PostgreSQL, enqueues the first pipeline message onto RabbitMQ, and provides read only polling endpoints. It handles no heavy computation synchronously.
3. **Message Broker:** A RabbitMQ instance declaring a durable topic/direct exchange (`notes.direct`), per stage durable queues (`jobs.transcribe`, `jobs.summarize`, `jobs.frame_extract`, `jobs.publish`), and a dead letter exchange (`notes.dlx`) routing unrecoverable tasks to `jobs.dlq`.
4. **Worker Pool:** At least two independent, identical worker processes. Workers pull tasks from queues with `prefetch_count=1`, execute the corresponding stage logic, persist state transitions to PostgreSQL, publish the subsequent stage message, and acknowledge (`ack`) the broker message strictly after durable completion.
5. **State Store and Coordinator:** PostgreSQL maintains persistent job records, state history, durations, titles, warnings, and results. Redis maintains distributed token bucket state to coordinate external API rate limits across workers.

---

## 3. End to End Pipeline Lifecycle

### 3.1 Stage Progression Sequence

```mermaid
sequenceDiagram
    autonumber
    actor User as User (Chrome Extension)
    participant GW as API Gateway
    participant DB as PostgreSQL
    participant MQ as RabbitMQ
    participant W as Worker Pool
    participant Ext as External APIs (LLM, YT, Notion)

    User->>GW: POST /jobs (youtube_url, notion_token)
    GW->>DB: INSERT Job (status=QUEUED)
    GW->>MQ: Publish StageMessage (stage=transcribe)
    GW-->>User: 202 Accepted (Location: /jobs/{id})

    MQ->>W: Deliver transcribe message
    W->>DB: UPDATE Job (status=TRANSCRIBING, progress=25%)
    W->>Ext: Fetch Captions & True Duration
    Note over W: Enforce Constraint A (<= 20 min)
    W->>MQ: Publish StageMessage (stage=summarize)
    W->>MQ: ack() transcribe message

    MQ->>W: Deliver summarize message
    W->>DB: UPDATE Job (status=SUMMARIZING, progress=50%)
    W->>Ext: Multimodal LLM Synthesis
    W->>MQ: Publish StageMessage (stage=frame_extract)
    W->>MQ: ack() summarize message

    MQ->>W: Deliver frame_extract message
    W->>DB: UPDATE Job (status=EXTRACTING_FRAMES, progress=75%)
    W->>Ext: FFmpeg Direct Stream Seek & VLM Curation
    W->>MQ: Publish StageMessage (stage=publish)
    W->>MQ: ack() frame_extract message

    MQ->>W: Deliver publish message
    W->>DB: UPDATE Job (status=PUBLISHING, progress=85%)
    W->>Ext: Publish Structured Page to Notion
    W->>DB: UPDATE Job (status=COMPLETED, progress=100%, notion_url)
    W->>MQ: ack() publish message

    User->>GW: GET /jobs/{id} (Poll Status)
    GW-->>User: 200 OK (status=COMPLETED, notion_page_url)
```

### 3.2 Stage 1: Transcript Extraction and Length Validation
* Captures the full timestamped caption tracks using `youtube_transcript_api` and inspects video stream metadata via `yt-dlp`.
* Computes the exact duration in seconds from caption endpoints.
* **Hard Constraint A Enforcement:** If the video length exceeds 1200 seconds (20 minutes), the job transitions immediately to `FAILED` with error code `VIDEO_TOO_LONG` and stops further execution without downloading large media files.

### 3.3 Stage 2: Multimodal LLM Synthesis and Key Moment Identification
* Formats raw transcript tokens into readable timestamped dialogue lines.
* If transcript volume exceeds single call token thresholds, chunks the text and executes recursive segment summarization, merging sections while preserving topical chronology.
* Synthesizes an executive overview, video archetype classification, key resource directory, and detailed bullet points for each technical topic.
* Identifies visual candidate timestamps paired with Constraint B explanatory captions (`WHAT` diagram is shown and `WHY` it is significant).
* Generates timestamps targeting the **culmination point** where progressive slide animations are fully assembled rather than when a topic first begins.

### 3.4 Stage 3: Intelligent Video Frame Extraction and Curation
* Resolves direct media stream URLs over HTTP without saving the entire video file to disk.
* Uses FFmpeg to execute frame accurate stream seeking and captures high resolution JPEG frames at candidate timestamps.
* Detects and discards solid black or blank transitional frames.
* Evaluates visual quality and completeness using a Multimodal Vision Language Model.
* Applies progressive reveal deduplication to eliminate intermediate animation builds and retain only the complete final slide.

### 3.5 Stage 4: Structured Notion Publishing and In Text Placement
* Builds a hierarchical Notion page block structure:
  * Header Callout box with source URL and video metadata.
  * Executive Specifications Table summarizing video type, runtime, source, and topics.
  * Key Resources and References Table with clickable external links.
  * Topical Section Headings with rich text bullet points.
  * **In Text Image Placement:** Embeds each curated image directly beneath the exact bullet point that discusses that concept.
  * Attaches formatted Constraint B captions to every image.
* Sets terminal state to `COMPLETED` if all frames succeeded, or `COMPLETED_WITH_WARNINGS` if any frames failed extraction.

---

## 4. Advanced Frame Extraction Architecture

The frame extraction subsystem ensures that study notes receive only high value, fully assembled architecture diagrams while eliminating talking heads, logos, and partial animations.

### 4.1 Direct Stream Seeking via FFmpeg

Instead of downloading multi gigabyte video files, KeyFrame AI resolves direct audio and video playback streams using `yt-dlp` and executes precise stream seeking with FFmpeg:

```bash
ffmpeg -ss <timestamp> -i "<direct_stream_url>" -frames:v 1 -q:v 2 <output_path.jpg>
```

This approach reduces latency from minutes to a few hundred milliseconds per frame and operates with zero local video footprint.

### 4.2 Vision Language Model Quality and Completeness Evaluation

Every captured candidate frame undergoes automated evaluation by a Vision Language Model (Google Gemini or OpenAI GPT 4o mini). The model applies strict inspection criteria:

1. **Strict Information Test:**
   * Is this image a genuine, substantive technical visual (architecture diagram, workflow, schema, code listing, or system comparison)?
   * If the frame shows a presenter talking head webcam, intro splash screen, logo banner, or trivial text card, it is immediately marked `DISCARD`.
2. **Progressive Diagram Completeness Test:**
   * Does the slide feature comparison columns (such as Option A vs Option B, Type 1 vs Type 2, or Without vs With) where one column is completely blank because the speaker has not finished drawing it?
   * If a diagram is mid animation or partially drawn, the model selects `RETRY_OFFSET` and suggests a future timestamp (typically 15 to 45 seconds ahead) to capture the finished diagram.
3. **In Text Placement and Topic Extraction:**
   * For approved frames (`KEEP`), the model identifies the exact `target_section`, matching `related_concept`, and a concise 2 to 4 word `slide_topic` to guide placement.

### 4.3 Progressive Reveal and Superseded Slide Deduplication

In educational lectures, speakers often present a single slide over several minutes, adding elements step by step. Standard pixel difference hashing treats each new element as a distinct slide because pixel values change significantly.

KeyFrame AI implements generalized progressive reveal deduplication (`deduplicate_progressive_slides`):
* Frames are sorted chronologically.
* If two captured frames appear within 180 seconds of each other (or within the same section) and share matching slide topics or high caption vocabulary overlap, the system recognizes that the earlier frame is an incomplete precursor of the later frame.
* The earlier partial frame is discarded and cleaned up from disk, leaving only the complete, fully assembled diagram in the final notes.

### 4.4 Frame Curation Decision Flow

```mermaid
flowchart TD
    Start["Candidate Timestamp Flagged by LLM"] --> Capture["FFmpeg Direct Stream Capture"]
    Capture --> BlankCheck{"Is Blank or Solid Transition?"}
    BlankCheck -- Yes --> NextOffset["Try Next Offset (+5s, +15s, +30s)"]
    NextOffset --> Capture
    BlankCheck -- No --> DHashCheck{"Perceptual dHash Match with Seen Slides?"}
    DHashCheck -- Duplicate (Distance <= 6) --> DiscardDup["Discard Duplicate Frame"]
    DHashCheck -- Unique --> VLM["Vision Language Model Inspection"]
    
    VLM --> VLMDecision{"VLM Decision"}
    VLMDecision -- DISCARD (Talking head, logo, empty) --> DiscardTrash["Discard Candidate"]
    VLMDecision -- RETRY_OFFSET (Mid animation) --> SeekFuture["Seek to Suggested Timestamp (+15s to +45s)"]
    SeekFuture --> Capture
    VLMDecision -- KEEP (Complete Technical Diagram) --> Pool["Add to Candidate Pool"]
    
    Pool --> ProgressiveDedup{"Progressive Reveal Check against Later Frames"}
    ProgressiveDedup -- Earlier Incomplete Version --> DiscardEarlier["Discard Earlier Partial Build"]
    ProgressiveDedup -- Fully Assembled Final Version --> FinalKeep["Retain for In Text Notion Embedding"]
```

---

## 5. API Gateway Specification

### 5.1 Base URL and Endpoints Summary

* **Base URL:** `http://localhost:8000`
* All endpoints return standard HTTP status codes.
* Client errors (4xx) return structured JSON payloads:
  ```json
  {
    "error": {
      "code": "INVALID_REQUEST",
      "message": "Specific explanation of input rejection"
    }
  }
  ```

### 5.2 Job Submission: POST /jobs

Validates the YouTube URL, persists the initial job in PostgreSQL, publishes a message to RabbitMQ, and returns immediately with `202 Accepted`.

* **Request Body:**
  ```json
  {
    "youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "notion_token": "secret_notion_oauth_token_here",
    "notion_parent_id": "optional_page_or_database_id",
    "video_title": "Optional Video Title"
  }
  ```
* **Success Response (HTTP 202):**
  * **Headers:** `Location: /jobs/{job_id}`
  * **Body:**
    ```json
    {
      "job_id": "9a38f362-e924-4f11-9ec3-9799dcb76e21",
      "status": "QUEUED",
      "video_id": "dQw4w9WgXcQ"
    }
    ```

### 5.3 Job Status Polling: GET /jobs/{job_id}

Read only, idempotent status check. Polling this endpoint never alters job state.

* **Success Response (HTTP 200):**
  ```json
  {
    "job_id": "9a38f362-e924-4f11-9ec3-9799dcb76e21",
    "status": "EXTRACTING_FRAMES",
    "progress": 75,
    "video_title": "Virtual Machines Explained",
    "duration_sec": 945,
    "warnings": [],
    "error": null,
    "result": null
  }
  ```

### 5.4 Job Outcome Retrieval: GET /jobs/{job_id}/results

Fetches the final publication outcome.

* **While Running (HTTP 409 Conflict):** Returns HTTP 409 while the job is in any non terminal stage.
* **On Completion (HTTP 200 OK):**
  ```json
  {
    "status": "COMPLETED",
    "result": {
      "notion_page_url": "https://notion.so/workspace/Virtual-Machines-Explained-4167e1b37a",
      "frames_requested": 4,
      "frames_published": 4
    },
    "warnings": []
  }
  ```

### 5.5 System Health Check: GET /healthz

Verifies backend infrastructure reachability across PostgreSQL, Redis, and RabbitMQ within strict timeouts.

* **Success Response (HTTP 200 OK):**
  ```json
  {
    "status": "healthy",
    "postgres": "ok",
    "redis": "ok",
    "rabbitmq": "ok"
  }
  ```

---

## 6. Notion Integration and Output Design

### 6.1 One Time OAuth Flow

The browser extension implements the standard Notion OAuth 2.0 flow:
1. User clicks **Connect Notion** in the extension popup.
2. The background script launches `chrome.identity.launchWebAuthFlow` pointing to Notion authorization servers.
3. Upon approval, Notion redirects to the extension callback with a temporary authorization code.
4. The gateway exchanges the code for a permanent access token via `POST /auth/notion/token`.
5. The token is saved in `chrome.storage.local`. The user is never re prompted on subsequent video submissions.

### 6.2 Destination Page Routing Behavior

1. **Explicit Target:** If the user or extension specifies a `notion_parent_id`, the page is created inside that parent page or database.
2. **Default Workspace Destination:** If `notion_parent_id` is omitted, the worker automatically publishes to the default parent page configured in the environment variable `NOTION_DEFAULT_PARENT_PAGE_ID`.
3. **Workspace Default:** Our repository configuration specifies default parent page `4167e1b37a1480e385bfc65f2bb279f8`.

### 6.3 Structured Study Notes Block Layout

Each generated Notion page follows an executive publication layout:

1. **Header Callout Block:** Contains source YouTube URL as a clickable link, detected video category (e.g. Technical Tutorial, System Architecture), and formatted runtime.
2. **Executive Overview & Specifications Table:** A high level summary paragraph followed by a two column specifications table detailing video archetype, runtime, source link, and topical coverage count.
3. **Key Resources and References Directory:** Bullet points and an interactive structured table indexing external documentation, GitHub repositories, and tool download links extracted from the video description, complete with direct external links.
4. **Topical Sections with Contextual In Text Diagrams:**
   * Section dividers and bold topic headings.
   * Comprehensive technical bullet points with monospace code blocks where commands or configurations appear.
   * Real extracted video frames embedded **directly underneath** the specific bullet points they illustrate.
   * Formatted captions beneath each image providing Constraint B explanations (`WHAT` is shown and `WHY` it was flagged).

---

## 7. Distributed Resilience and Failure Handling

The system is designed to handle four primary real world failure scenarios live during grading demonstrations.

### 7.1 External Rate Limits and Exponential Backoff (HTTP 429)

* **Behavior:** When an external API (YouTube transcript, Google Gemini, OpenAI, or Notion) returns HTTP 429 or quota exhaustion codes, the worker does not fail the job.
* **Mechanism:** The consumer catches the rate limit, calculates backoff delay using the schedule `[10s, 30s, 90s]`, increments `retry_count`, and publishes a delayed task to RabbitMQ.
* **Client Visibility:** The popup UI keeps the job in an active waiting state rather than surfacing a terminal error while retries remain.

### 7.2 Worker Crash Recovery and Task Idempotency

* **Behavior:** If a worker process dies abruptly (e.g. `SIGKILL` or power outage during frame extraction), no job is lost and no duplicate pages are created on Notion.
* **Mechanism:** RabbitMQ task messages use manual acknowledgment (`message.ack()`). Acknowledgment occurs strictly after a stage commits durably to PostgreSQL and publishes the subsequent stage message.
* **Idempotency Safeguard:** When `publish.py` executes, it checks `if job.notion_page_url:`. If a previous attempt already created the Notion page before a worker crash, the worker reuses the existing URL rather than creating a duplicate.

### 7.3 Graceful Degradation on Frame Failure

* **Behavior:** If one or more video frames cannot be extracted (due to stream seek timeouts or network drops), the worker does not terminate the job.
* **Mechanism:** It logs a warning record containing the timestamp and continues publishing the text summary alongside whatever frames succeeded.
* **Status:** If any frame is omitted, the job transitions to `COMPLETED_WITH_WARNINGS`. If all frames fail, a text only study guide is published.

### 7.4 Dead Letter Queue (DLQ) Bounded Retries

* **Behavior:** When a job encounters a permanent error (such as a deleted video or private URL), retrying indefinitely would waste resources.
* **Mechanism:** After exhausting the maximum retry budget (5 attempts), the worker marks the job status as `DEAD_LETTERED` and routes the message payload to `jobs.dlq` via `notes.dlx` for operator inspection.

### 7.5 Coordinated Global Rate Limiting via Redis

* To prevent multiple concurrent workers from triggering 429 rate limits against LLM providers, workers coordinate through a distributed Redis token bucket (`DistributedRateLimiter`).
* Global throughput is throttled to `LLM_RATE_LIMIT_RPM` (default 15 requests per minute) across all worker containers.

---

## 8. Hard Constraints Enforcement

### 8.1 Constraint A: 20 Minute Video Duration Cap

* **Specification:** The system must support videos up to 20 minutes (1200 seconds). Longer videos must be rejected deterministically.
* **Implementation:** In [worker/stages/transcribe.py](file:///c:/Projects/Cloud%20Nov_20/worker/stages/transcribe.py), the true video duration is computed from caption timestamps and stream metadata. If `duration_sec > 1200`, the job transitions immediately to `FAILED` with error code `VIDEO_TOO_LONG`:
  ```
  Constraint A Violation: Video duration of 1520s exceeds maximum allowed length of 1200s (20 minutes).
  ```
* Validated in automated test suites with durations of 300s, 600s, 1200s (allowed), and 1201s, 1800s, 3600s (rejected).

### 8.2 Constraint B: Authentic Frames with Explanatory Captions

* **Specification:** Every image on the Notion page must be an authentic frame captured from the source video at the identified timestamp. Images must not be AI generated or substituted. Every image must be accompanied by an explanatory caption detailing what is shown and why it was flagged.
* **Implementation:** Frames are captured directly via FFmpeg stream seeking. Pydantic schemas in [worker/llm/base.py](file:///c:/Projects/Cloud%20Nov_20/worker/llm/base.py) reject empty captions at validation time. In Notion publishing, every image block includes a caption block with explicit `WHAT` and `WHY` technical breakdowns.

---

## 9. Environment Variables and Configuration

All configuration is loaded from `.env` at the repository root. A template is provided in `.env.example`.

| Variable Name | Required | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `DATABASE_URL` | Yes | `postgresql+asyncpg://postgres:postgres@postgres:5432/notes_agent` | PostgreSQL connection string. |
| `RABBITMQ_URL` | Yes | `amqp://guest:guest@rabbitmq:5672/` | AMQP broker connection string. |
| `REDIS_URL` | Yes | `redis://redis:6379/0` | Redis connection URL for rate limiting. |
| `LLM_PROVIDER` | Yes | `gemini` | Synthesis provider (`gemini` or `openai`). |
| `GEMINI_API_KEY` | Yes (if Gemini) | Configured in `.env` | Google AI Studio API key. |
| `OPENAI_API_KEY` | Optional | `""` | OpenAI API key when provider is set to openai. |
| `LLM_MODEL` | Yes | `gemini-3.5-flash-lite` | Synthesis model name. |
| `LLM_RATE_LIMIT_RPM` | Yes | `15` | Global requests per minute limit across workers. |
| `NOTION_OAUTH_CLIENT_ID` | Yes | Configured in `.env` | Notion public OAuth client ID. |
| `NOTION_OAUTH_CLIENT_SECRET` | Yes | Configured in `.env` | Notion integration client secret. |
| `NOTION_DEFAULT_PARENT_PAGE_ID` | Yes | `4167e1b37a1480e385bfc65f2bb279f8` | Default Notion parent page identifier. |
| `FRAME_STORAGE_DIR` | Yes | `/shared/frames` | Shared storage volume for extracted frames. |
| `MAX_VIDEO_DURATION_SEC` | Yes | `1200` | Constraint A duration limit (20 minutes). |
| `MAX_RETRY_ATTEMPTS` | Yes | `5` | Maximum retry budget before DLQ routing. |
| `RETRY_BACKOFF_SCHEDULE` | Yes | `[10, 30, 90]` | Exponential backoff delay schedule in seconds. |

---

## 10. Step by Step Quick Start Guide

A grader or developer can bring up the entire system from a fresh clone using the instructions below.

### 10.1 Prerequisites

Ensure the following tools are installed on your machine:
* [Docker and Docker Compose](https://docs.docker.com/get-docker/)
* [Google Chrome Browser](https://www.google.com/chrome/)
* Python 3.12 or higher (optional, for running local pytest outside Docker)

### 10.2 Environment Configuration

Verify that a `.env` file exists at the root of the project with a valid `GEMINI_API_KEY`:

```bash
# Verify or create your .env
cp .env.example .env
```

Ensure your `GEMINI_API_KEY` is set inside `.env`:
```ini
GEMINI_API_KEY=your_actual_google_ai_studio_api_key_here
```

### 10.3 Launching the Infrastructure with Docker Compose

Run the following command from the repository root:

```bash
docker compose up -d --build
```

This starts all five services in the background:
* **PostgreSQL** on port 5432
* **Redis** on port 6379
* **RabbitMQ** on ports 5672 and 15672 (Management UI)
* **API Gateway** on port 8000
* **Two Worker Instances** (`cloudnov_20-worker-1` and `cloudnov_20-worker-2`)

### 10.4 Verifying Service Health

Execute a sanity check against the health endpoint:

```bash
curl -s http://localhost:8000/healthz
```

Expected JSON response:
```json
{
  "status": "healthy",
  "postgres": "ok",
  "redis": "ok",
  "rabbitmq": "ok"
}
```

You can also view the RabbitMQ management dashboard by opening `http://localhost:15672` in your browser (Username: `guest`, Password: `guest`).

### 10.5 Installing the Chrome Browser Extension

1. Open Google Chrome and enter `chrome://extensions/` in the address bar.
2. Toggle on **Developer mode** in the top right corner.
3. Click **Load unpacked** in the top left corner.
4. Select the `extension` folder inside this repository directory (`c:\Projects\Cloud Nov_20\extension`).
5. The KeyFrame AI icon will appear in your Chrome toolbar.

### 10.6 Processing Your First Video

1. In Google Chrome, navigate to any YouTube technical tutorial under 20 minutes (for example, search for any system design, Docker, or programming video).
2. Click the **Take Notes** button injected directly into the YouTube video player controls toolbar.
3. Alternatively, click the KeyFrame AI extension icon in your toolbar, authenticate with Notion once, and track progress.
4. Watch the live multi stage progression stepper:
   `Queued` $\rightarrow$ `Transcribing` $\rightarrow$ `Summarizing` $\rightarrow$ `Extracting Frames` $\rightarrow$ `Publishing` $\rightarrow$ `Done`
5. Click **View in Notion** to review the published study notes containing organized technical sections, code listings, and in text diagrams with captions.

---

## 11. Testing and Verification

### 11.1 Running the Automated Test Suite

Run the full automated test suite locally:

```bash
.\.venv\Scripts\pytest.exe -v
```

All 69 automated tests validate:
* Deterministic 20 minute boundary enforcement (Constraint A)
* Real video frames and non empty explanatory captions (Constraint B)
* Progressive slide reveal deduplication
* Manual broker message acknowledgments and crash recovery
* Distributed rate limiter token bucket acquisition
* Transcript chunking, section merging, and timestamp clamping
* REST API contracts, 202 Accepted semantics, and 409 conflict handling

### 11.2 Live Demonstrations of Graded Scenarios

To demonstrate the four required failure scenarios during grading:

#### Scenario 1: External Rate Limits (HTTP 429)
Submit several video jobs in rapid succession through the extension. The Redis token bucket ensures that worker requests are metered smoothly to stay within `LLM_RATE_LIMIT_RPM`, avoiding unhandled provider errors.

#### Scenario 2: Worker Crash Recovery (SIGKILL Mid Job)
1. Submit a video job through the extension.
2. While the job is actively in `EXTRACTING_FRAMES` or `SUMMARIZING`, kill one worker container:
   ```bash
   docker kill cloudnov_20-worker-1
   ```
3. RabbitMQ detects the disconnection immediately and delivers the unacknowledged task to `cloudnov_20-worker-2`.
4. The remaining worker completes the stage. The job finishes with zero lost tasks and zero duplicate pages on Notion.

#### Scenario 3: Graceful Degradation on Frame Failure
Submit a video containing unseekable live stream chunks. The worker logs `FRAME_EXTRACTION_FAILED` warnings, continues publishing all text sections, and finishes with status `COMPLETED_WITH_WARNINGS`.

#### Scenario 4: Dead Letter Queue (DLQ) Bounded Retries
Submit an unavailable or private YouTube URL. The job retries across its backoff schedule (`10s`, `30s`, `90s`) up to 5 attempts, transitions to `DEAD_LETTERED`, and routes the payload to `jobs.dlq` in RabbitMQ.

---

## 12. Troubleshooting and Frequently Asked Questions

* **Issue: The extension shows "Connect Notion first".**
  * *Resolution:* Click the KeyFrame AI icon in Chrome, open settings, and click **Connect Notion**. Complete the one time web authorization flow.

* **Issue: Status shows "YouTube temporarily rate limited caption fetching for this IP".**
  * *Resolution:* YouTube occasionally rate limits subtitle requests from datacenter IPs. Wait a few minutes or switch networks; the worker catches this as an HTTP 429 and applies exponential backoff automatically.

* **Issue: Worker containers fail to start or restart in a loop.**
  * *Resolution:* Ensure Docker has sufficient memory allocated and check container logs:
    ```bash
    docker compose logs worker
    ```

* **Issue: Where are extracted frame files stored?**
  * *Resolution:* Frames are stored in the shared volume `/shared/frames` (accessible on the host at `gateway/static/frames` or mounted Docker storage).
