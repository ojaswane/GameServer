# Control Panel

The Control Panel is the Rust coordinator between browser clients and game agents. It accepts game-session requests, tracks agent workers and their capacity, and lets an available worker claim a queued session to set up.

## Request flow

The intended end-to-end flow is:

1. A game agent registers with the Control Panel and sends periodic heartbeats.
2. The browser requests a game session. It sends an `Idempotency-Key` so retrying the same request does not create a duplicate.
3. The Control Panel creates the session in the `queued` state.
4. An online game agent with an available slot polls the game-setup endpoint. The Control Panel assigns it one queued session and marks it `starting`.
5. The agent prepares the game and reports the result. The Control Panel can then return connection details to the browser.
6. The browser connects to the streaming host and sends player input through the streaming system.

The current code implements worker registration, heartbeat tracking, session creation, worker polling/assignment, and a WebSocket relay for WebRTC signaling messages. Agent status reporting, browser session-status lookup, media capture/stream setup, and player input are not implemented yet. Session and idempotency data are currently in memory and are lost when the server restarts.

## Run locally

From this directory:

```sh
cargo run
```

The server binds to `0.0.0.0:8080` (`http://<server-ip>:8080` from another device on a network that can reach it).

## HTTP API

### Health check

```http
GET /health
```

Example response:

```json
{"status":"OK"}
```

### Register a game agent

```http
POST /workers
Content-Type: application/json

{
  "id": "agent-1",
  "max_sessions": 2
}
```

The response includes the worker's status, active session count, and available slots. Worker IDs must be unique while the server is running. `max_sessions` must be greater than zero.

### List workers

```http
GET /workers
```

### Send a heartbeat

```http
POST /workers/agent-1/heartbeat
```

Workers are marked offline after 30 seconds without a heartbeat. The monitoring loop checks every 5 seconds.

### Request a game session

```http
POST /sessions
Idempotency-Key: 3d9e2bb6-5592-4e62-b9f5-789a1e045c0f
Content-Type: application/json

{
  "game_name": "RDR2"
}
```

The first request creates a `queued` session. Retrying with the same key and game name returns that session. Reusing a key for a different game returns `409 Conflict`. A missing or invalid key returns `400 Bad Request`.

The response includes `client_id` (currently the same value as the session ID) and `client_ip`, the remote IP observed by the Control Panel for this request.

Example response:

```json
{
  "id": "session-...",
  "game_name": "RDR2",
  "worker_id": null,
  "status": "queued",
  "stream_url": null,
  "created_at": 0,
  "updated_at": 0,
  "failure_reason": null
}
```

The timestamp values are Unix time in milliseconds; the values above are placeholders.

### Claim a queued session for setup

```http
POST /workers/agent-1/game-setup
```

An online worker with an available slot receives one queued session. The Control Panel assigns the worker, changes the session status to `starting`, and reserves a worker slot. If no session is queued, the response body is `null`. Offline workers receive `503 Service Unavailable`, workers without capacity receive `409 Conflict`, and an unknown worker receives `404 Not Found`.

Worker registration records the remote IP observed by the Control Panel, and worker responses include it. Once the session is assigned, the connection metadata can be fetched with:

```http
GET /sessions/{session_id}/connection-info
```

The response includes client/session ID and IP, plus assigned worker ID and IP. These are IPs observed on the HTTP requests. A future WebSocket handler should capture the remote address for each WebSocket connection directly. This endpoint is unauthenticated like the rest of the development API, so use it only on a trusted network.

### WebSocket signaling

After registering and claiming a session, the worker opens a socket for that session:

```text
ws://<server>:8080/ws/workers/{worker_id}/sessions/{session_id}
```

After the worker claims a session, the browser opens:

```text
ws://<server>:8080/ws/sessions/{session_id}
```

The browser sends its SDP offer or ICE candidate as a WebSocket text frame. The server forwards that frame unchanged to the worker. The worker sends its answer or ICE candidate as a text frame, which the server forwards unchanged to the browser. Binary frames are forwarded unchanged too. The session ID is carried in the WebSocket route, so the Control Panel does not decode signaling payloads or add an envelope. It only handles WebSocket control frames such as ping/pong itself.

The relay carries signaling frames only. Audio/video must flow over the WebRTC peer connection. Both WebSocket routes are currently unauthenticated and should only be used on a trusted development network.

Example response when a session is assigned:

```json
{
  "id": "session-...",
  "game_name": "RDR2",
  "worker_id": "agent-1",
  "status": "starting",
  "stream_url": null,
  "created_at": 0,
  "updated_at": 0,
  "failure_reason": null
}
```

## Session lifecycle

The model defines these statuses:

`queued` → `starting` → `ready` → `running` → `closing` → `closed`

A session can also enter `failed`. At present, the API advances sessions from `queued` to `starting`; endpoints for agent status reports and the remaining transitions are still to be added.

## Current limitations

- Worker and session registries are in-memory only; use the database for durable state before deployment.
- Workers still poll `POST /workers/{worker_id}/game-setup` to claim sessions, then open one WebSocket per session for signaling.
- A worker slot is reserved when a session is assigned. There is not yet an endpoint to report completion or failure and release that slot.
- `stream_url` is not populated yet, and there is no browser endpoint to fetch session status.
- Worker registration, heartbeats, and session requests are not authenticated yet; use these endpoints only in a trusted development environment.
