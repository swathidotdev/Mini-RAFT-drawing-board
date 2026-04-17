/**
 * Mini-RAFT Replica Server
 * Person 3 — Distributed Real-Time Drawing Board
 *
 * All 3 replicas run this same file.
 * Identity comes from environment variables:
 *
 *   REPLICA_ID   = "replica1" | "replica2" | "replica3"
 *   REPLICA_PORT = 4001 | 4002 | 4003
 *   PEERS        = "http://replica2:4002,http://replica3:4003"   (the OTHER two)
 *   GATEWAY_URL  = "http://gateway:3000"
 */

const express = require("express");
const axios   = require("axios");

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});
app.options("*", (req, res) => res.sendStatus(204));

// ─────────────────────────────────────────────────────────────
//  ENVIRONMENT CONFIG
// ─────────────────────────────────────────────────────────────
const REPLICA_ID  = process.env.REPLICA_ID   || "replica1";
const PORT        = parseInt(process.env.REPLICA_PORT || "4001");
const PEER_URLS   = (process.env.PEERS || "").split(",").filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL  || "http://gateway:3000";

const HEARTBEAT_MS    = 150;
const ELECTION_MIN_MS = 500;
const ELECTION_MAX_MS = 800;

// ─────────────────────────────────────────────────────────────
//  RAFT STATE
// ─────────────────────────────────────────────────────────────
let state       = "follower";   // "follower" | "candidate" | "leader"
let currentTerm = 0;
let votedFor    = null;
let leaderId    = null;

// Append-only log: [{ index, term, stroke }, ...]
let log         = [];
let commitIndex = -1;

let electionTimer  = null;
let heartbeatTimer = null;

// ─────────────────────────────────────────────────────────────
//  LOGGING
// ─────────────────────────────────────────────────────────────
function logEvent(msg, detail = "") {
  const time = new Date().toISOString().slice(11, 23);
  console.log(`[${time}] [${REPLICA_ID}] [term:${currentTerm}] [${state.toUpperCase()}] ${msg}${detail ? " | " + detail : ""}`);
}

// ─────────────────────────────────────────────────────────────
//  TIMERS
// ─────────────────────────────────────────────────────────────
function clearElectionTimer() {
  if (electionTimer) { clearTimeout(electionTimer); electionTimer = null; }
}

function clearHeartbeatTimer() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
}

function resetElectionTimer() {
  clearElectionTimer();
  const timeout = ELECTION_MIN_MS + Math.floor(Math.random() * (ELECTION_MAX_MS - ELECTION_MIN_MS));
  electionTimer = setTimeout(() => {
    logEvent("Election timeout — starting election");
    startElection();
  }, timeout);
}

// ─────────────────────────────────────────────────────────────
//  STATE TRANSITIONS
// ─────────────────────────────────────────────────────────────
function becomeFollower(term, fromLeader = null) {
  clearHeartbeatTimer();
  state       = "follower";
  currentTerm = term;
  votedFor    = null;
  if (fromLeader) leaderId = fromLeader;
  logEvent("→ FOLLOWER", fromLeader ? `leader=${fromLeader}` : "");
  resetElectionTimer();
}

function becomeLeader() {
  clearElectionTimer();
  state    = "leader";
  leaderId = REPLICA_ID;
  logEvent("→ LEADER 🏆 Won the election");
  sendHeartbeats();
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_MS);
  notifyGateway();
}

// ─────────────────────────────────────────────────────────────
//  LEADER ELECTION
// ─────────────────────────────────────────────────────────────
async function startElection() {
  state = "candidate";
  currentTerm += 1;
  votedFor = REPLICA_ID;
  leaderId = null;

  const lastLogIndex = log.length - 1;
  const lastLogTerm  = lastLogIndex >= 0 ? log[lastLogIndex].term : -1;

  logEvent("→ CANDIDATE", `requesting votes from ${PEER_URLS.length} peers`);

  let votes = 1; // vote for ourselves
  const majority = Math.floor((PEER_URLS.length + 1) / 2) + 1;

  await Promise.allSettled(
    PEER_URLS.map(async (peer) => {
      try {
        const res = await axios.post(`${peer}/request-vote`, {
          term: currentTerm,
          candidateId: REPLICA_ID,
          lastLogIndex,
          lastLogTerm,
        }, { timeout: 300 });

        if (res.data.voteGranted) {
          votes++;
          logEvent("Vote received", `from ${peer} (votes=${votes})`);
        } else if (res.data.term > currentTerm) {
          becomeFollower(res.data.term);
        }
      } catch {
        logEvent("Peer unreachable during election", peer);
      }
    })
  );

  if (state !== "candidate") return; // stepped down during voting

  if (votes >= majority) {
    becomeLeader();
  } else {
    logEvent("Lost election", `votes=${votes}/${PEER_URLS.length + 1} needed=${majority}`);
    becomeFollower(currentTerm);
  }
}

// ─────────────────────────────────────────────────────────────
//  HEARTBEAT (leader sends every 150ms)
// ─────────────────────────────────────────────────────────────
async function sendHeartbeats() {
  if (state !== "leader") return;
  for (const peer of PEER_URLS) {
    try {
      await axios.post(`${peer}/heartbeat`, {
        term: currentTerm,
        leaderId: REPLICA_ID,
        commitIndex,
      }, { timeout: 200 });
    } catch { /* peer temporarily down — ignore */ }
  }
}

// ─────────────────────────────────────────────────────────────
//  LOG REPLICATION
// ─────────────────────────────────────────────────────────────
async function replicateStroke(stroke) {
  if (state !== "leader") return false;

  // 1. Append to own log
  const entry = { index: log.length, term: currentTerm, stroke };
  log.push(entry);
  logEvent("Appended stroke to log", `index=${entry.index}`);

  // 2. Send AppendEntries to all followers
  let acks    = 1; // leader counts itself
  const majority = Math.floor((PEER_URLS.length + 1) / 2) + 1;

  await Promise.allSettled(
    PEER_URLS.map(async (peer) => {
      try {
        const prevLogIndex = entry.index - 1;
        const prevLogTerm  = prevLogIndex >= 0 ? log[prevLogIndex].term : -1;

        const res = await axios.post(`${peer}/append-entries`, {
          term:         currentTerm,
          leaderId:     REPLICA_ID,
          prevLogIndex,
          prevLogTerm,
          entries:      [entry],
          leaderCommit: commitIndex,
        }, { timeout: 300 });

        if (res.data.success) {
          acks++;
          logEvent("AppendEntries ack", `from ${peer} (acks=${acks})`);
        } else if (res.data.term > currentTerm) {
          becomeFollower(res.data.term);
        } else if (res.data.logLength !== undefined) {
          // Follower is behind — send catch-up
          catchUpFollower(peer, res.data.logLength);
        }
      } catch {
        logEvent("AppendEntries failed", peer);
      }
    })
  );

  if (state !== "leader") return false;

  // 3. Commit if majority acknowledged
  if (acks >= majority) {
    commitIndex = entry.index;
    logEvent("Stroke COMMITTED ✓", `index=${commitIndex}`);
    return true;
  }

  logEvent("Stroke NOT committed — no majority", `acks=${acks}`);
  return false;
}

// ─────────────────────────────────────────────────────────────
//  CATCH-UP (leader pushes missing entries to a lagging follower)
// ─────────────────────────────────────────────────────────────
async function catchUpFollower(peer, fromIndex) {
  try {
    const missing = log.slice(fromIndex, commitIndex + 1);
    if (missing.length === 0) return;
    await axios.post(`${peer}/sync-log`, {
      term:        currentTerm,
      leaderId:    REPLICA_ID,
      entries:     missing,
      leaderCommit: commitIndex,
    }, { timeout: 500 });
    logEvent("Sent catch-up entries", `to ${peer} from index ${fromIndex}`);
  } catch {
    logEvent("Catch-up failed", peer);
  }
}

// ─────────────────────────────────────────────────────────────
//  NOTIFY GATEWAY OF NEW LEADER
// ─────────────────────────────────────────────────────────────
async function notifyGateway() {
  try {
    await axios.post(`${GATEWAY_URL}/leader`, {
      leaderId,
      leaderUrl: `http://${REPLICA_ID}:${PORT}`,
    }, { timeout: 500 });
    logEvent("Notified gateway of leadership");
  } catch {
    logEvent("Could not reach gateway — will retry");
  }
}

// ─────────────────────────────────────────────────────────────
//  HTTP ENDPOINTS
// ─────────────────────────────────────────────────────────────

// GET /status — health check + debug info
app.get("/status", (req, res) => {
  res.json({ replicaId: REPLICA_ID, state, term: currentTerm, leaderId, logLength: log.length, commitIndex });
});

// ── /request-vote ─────────────────────────────────────────────
app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;

  if (term > currentTerm) becomeFollower(term);

  if (term < currentTerm)
    return res.json({ term: currentTerm, voteGranted: false });

  if (votedFor !== null && votedFor !== candidateId)
    return res.json({ term: currentTerm, voteGranted: false });

  // Candidate log must be at least as up-to-date as ours
  const myLastIndex = log.length - 1;
  const myLastTerm  = myLastIndex >= 0 ? log[myLastIndex].term : -1;
  const logOk = lastLogTerm > myLastTerm || (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);

  if (!logOk)
    return res.json({ term: currentTerm, voteGranted: false });

  votedFor = candidateId;
  resetElectionTimer();
  logEvent("Voted for", candidateId);
  return res.json({ term: currentTerm, voteGranted: true });
});

// ── /heartbeat ────────────────────────────────────────────────
app.post("/heartbeat", (req, res) => {
  const { term, leaderId: incomingLeader, commitIndex: leaderCommit } = req.body;

  if (term < currentTerm)
    return res.json({ term: currentTerm, success: false });

  if (term > currentTerm || state !== "follower") {
    becomeFollower(term, incomingLeader);
  } else {
    leaderId = incomingLeader;
    resetElectionTimer();
  }

  if (leaderCommit > commitIndex && leaderCommit < log.length) {
    commitIndex = leaderCommit;
  }

  return res.json({ term: currentTerm, success: true });
});

// ── /append-entries ───────────────────────────────────────────
app.post("/append-entries", (req, res) => {
  const { term, leaderId: incomingLeader, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

  if (term < currentTerm)
    return res.json({ term: currentTerm, success: false });

  if (term > currentTerm || state !== "follower") {
    becomeFollower(term, incomingLeader);
  } else {
    leaderId = incomingLeader;
    resetElectionTimer();
  }

  // Consistency check
  if (prevLogIndex >= 0) {
    const prev = log[prevLogIndex];
    if (!prev || prev.term !== prevLogTerm) {
      logEvent("AppendEntries FAILED consistency check", `prevLogIndex=${prevLogIndex}`);
      return res.json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  // Append entries (truncate conflicts)
  for (const entry of entries) {
    if (entry.index < log.length) {
      if (log[entry.index].term !== entry.term) {
        log = log.slice(0, entry.index);
        log.push(entry);
      }
    } else {
      log.push(entry);
    }
    logEvent("Appended entry", `index=${entry.index}`);
  }

  if (leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
    logEvent("CommitIndex updated", `commitIndex=${commitIndex}`);
  }

  return res.json({ term: currentTerm, success: true });
});

// ── /sync-log ─────────────────────────────────────────────────
// Leader pushes all missing entries to a restarted follower
app.post("/sync-log", (req, res) => {
  const { term, leaderId: incomingLeader, entries, leaderCommit } = req.body;

  if (term < currentTerm)
    return res.json({ term: currentTerm, success: false });

  becomeFollower(term, incomingLeader);

  if (entries && entries.length > 0) {
    const startIndex = entries[0].index;
    log = log.slice(0, startIndex).concat(entries);
    commitIndex = leaderCommit !== undefined ? leaderCommit : log.length - 1;
    logEvent("Sync-log complete", `received ${entries.length} entries, commitIndex=${commitIndex}`);
  }

  return res.json({ term: currentTerm, success: true });
});

// ── /log ──────────────────────────────────────────────────────
// Returns committed log entries from index N onward
app.get("/log", (req, res) => {
  const from = parseInt(req.query.from || "0");
  res.json({
    entries: log.slice(0, commitIndex + 1).slice(from),
    commitIndex,
    term: currentTerm,
  });
});

// ── /stroke ───────────────────────────────────────────────────
// Called by gateway when a client draws a stroke
app.post("/stroke", async (req, res) => {
  const { stroke } = req.body;

  if (state !== "leader") {
    return res.status(403).json({
      error: "Not the leader",
      leaderId,
      leaderUrl: leaderId ? `http://${leaderId}:${getPortForReplica(leaderId)}` : null,
    });
  }

  const committed = await replicateStroke(stroke);

  if (committed) {
    try {
      await axios.post(`${GATEWAY_URL}/broadcast`, { stroke, logIndex: commitIndex }, { timeout: 500 });
    } catch {
      logEvent("Could not broadcast to gateway");
    }
    return res.json({ success: true, logIndex: commitIndex });
  }

  return res.status(500).json({ error: "Failed to achieve majority" });
});

// ─────────────────────────────────────────────────────────────
//  UTILITY
// ─────────────────────────────────────────────────────────────
function getPortForReplica(id) {
  const num = parseInt((id || "").replace("replica", "") || "1");
  return 4000 + num;
}

// ─────────────────────────────────────────────────────────────
//  START
// ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log("╔══════════════════════════════════════════╗");
  console.log(`║  ${REPLICA_ID} starting on port ${PORT}              ║`);
  console.log(`║  Peers: ${PEER_URLS.length} configured                    ║`);
  console.log("╚══════════════════════════════════════════╝");
  becomeFollower(0);
});
