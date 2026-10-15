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

The current code implements worker registration, heartbeat tracking, session creation, and worker polling/assignment. Persistent WebSocket dispatch, agent status reporting, browser session-status lookup, stream setup, and player input are not implemented yet. Session and idempotency data are currently in memory and are lost when the server restarts.

## Run locally

From this directory:

```sh
cargo run
```

The server listens for now on `http://172.20.10.2:3000`.

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
- The diagram shows a persistent WebSocket between the game agent and server. The current implementation instead has the agent poll `POST /workers/{worker_id}/game-setup`.
- A worker slot is reserved when a session is assigned. There is not yet an endpoint to report completion or failure and release that slot.
- `stream_url` is not populated yet, and there is no browser endpoint to fetch session status.
- Worker registration, heartbeats, and session requests are not authenticated yet; use these endpoints only in a trusted development environment.
