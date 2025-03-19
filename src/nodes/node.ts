import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { Value } from "../types";
import { delay } from "../utils";

export async function node(
  node_id: number, 
  N: number, 
  F: number, 
  initial_value: Value,
  is_faulty: boolean,
  nodesAreReady: () => boolean,
  setNodeIsReady: (index: number) => void
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  
  const state = {
    killed: false,
    x: is_faulty ? null : initial_value,
    decided: is_faulty ? null : false,
    k: is_faulty ? null : 0,
  };

  const phaseOneMessages: Record<number, { value: Value; from: number }[]> = {};
  const phaseTwoMessages: Record<number, { value: Value; from: number }[]> = {};

  let consensus_running = false; 
  node.get("/status", (req, res) => {
    if (is_faulty) {
      res.status(500).send("faulty");
    } else {
      res.status(200).send("live");
    }
  });

  node.post("/message", (req, res) => {
    if (state.killed || is_faulty) {
      res.status(500).json({ error: "Node is dead or faulty" });
      return;
    }

    const { phase, value, k, from } = req.body;

    if (phase === 1) {
      if (!phaseOneMessages[k]) {
        phaseOneMessages[k] = [];
      }
      phaseOneMessages[k].push({ value, from });
    } else if (phase === 2) {
      if (!phaseTwoMessages[k]) {
        phaseTwoMessages[k] = [];
      }
      phaseTwoMessages[k].push({ value, from });
    }

    res.status(200).json({ success: true });
  });

  node.get("/start", async (req, res) => {
    if (is_faulty || state.killed) {
      res.status(500).json({ error: "Node is faulty or killed" });
      return;
    }

    res.status(200).json({ success: true });

    if (!consensus_running) {
      consensus_running = true;
      startConsensus();
    }
  });

  node.get("/stop", async (req, res) => {
    state.killed = true;
    consensus_running = false;
    res.status(200).json({ success: true });
  });

  node.get("/getState", (req, res) => {
    res.status(200).json(state);
  });

  async function startConsensus() {
    if (N === 1 && !is_faulty) {
      state.decided = true;
      return;
    }

    while (!state.decided && !state.killed) {
      await runPhaseOne();
      await runPhaseTwo();

      if (!state.decided && !state.killed) {
        state.k = (state.k as number) + 1;
      }
      
      await delay(10);
    }
  }

  async function runPhaseOne() {
    if (state.killed || state.decided) return;

    await broadcastMessage(1, state.x as Value, state.k as number);

    await waitForMessages(1);

    processPhaseOneMessages();
  }

  async function runPhaseTwo() {
    if (state.killed || state.decided) return;

    await broadcastMessage(2, state.x as Value, state.k as number);

    await waitForMessages(2);

    processPhaseTwoMessages();
  }

  async function broadcastMessage(phase: number, value: Value, k: number) {
    const promises = [];
    
    for (let i = 0; i < N; i++) {
      if (i !== node_id) {
        promises.push(
          fetch(`http://localhost:${BASE_NODE_PORT + i}/message`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              phase,
              value,
              k,
              from: node_id,
            }),
          }).catch(() => {
          })
        );
      }
    }
    
    await Promise.all(promises);
  }

  async function waitForMessages(phase: number) {
    const maxWaitTime = 200; // ms
    const startTime = Date.now();
    
    const messages = phase === 1 ? phaseOneMessages : phaseTwoMessages;
    const currentK = state.k as number;
    
    while (
      Date.now() - startTime < maxWaitTime &&
      (!messages[currentK] || messages[currentK].length < N - F - 1)
    ) {
      await delay(10);
      if (state.killed) return;
    }
  }

  function processPhaseOneMessages() {
    if (state.killed) return;
    
    const currentK = state.k as number;
    if (!phaseOneMessages[currentK]) {
      phaseOneMessages[currentK] = [];
    }

    const messages = phaseOneMessages[currentK];
    const counts: Record<string, number> = { "0": 0, "1": 0 };

    if (state.x === 0 || state.x === 1) {
      counts[state.x.toString()]++;
    }

    for (const msg of messages) {
      if (msg.value === 0 || msg.value === 1) {
        counts[msg.value.toString()]++;
      }
    }

    const majority = Math.floor(N / 2) + 1;
    
    if (counts["0"] >= majority) {
      state.x = 0;
    } else if (counts["1"] >= majority) {
      state.x = 1;
    } else {
      state.x = "?";
    }
  }

  function processPhaseTwoMessages() {
    if (state.killed) return;
    
    const currentK = state.k as number;
    if (!phaseTwoMessages[currentK]) {
      phaseTwoMessages[currentK] = [];
    }

    const messages = phaseTwoMessages[currentK];
    const counts: Record<string, number> = { "0": 0, "1": 0, "?": 0 };

    if (state.x !== null) {
      counts[state.x.toString()]++;
    }

    for (const msg of messages) {
      if (msg.value === 0 || msg.value === 1 || msg.value === "?") {
        counts[msg.value.toString()]++;
      }
    }

    const nonFaultyNodes = N - F;
    
    const decision_threshold = Math.floor(nonFaultyNodes / 2) + 1;
    const adoption_threshold = Math.floor(nonFaultyNodes / 3) + 1;

    if (counts["0"] >= decision_threshold && state.x === 0) {
      state.decided = true;
    } else if (counts["1"] >= decision_threshold && state.x === 1) {
      state.decided = true;
    }
    else if (counts["0"] >= adoption_threshold) {
      state.x = 0;
    } else if (counts["1"] >= adoption_threshold) {
      state.x = 1;
    } else {
      state.x = Math.random() < 0.5 ? 0 : 1;
    }
  }

  const server = node.listen(BASE_NODE_PORT + node_id, async () => {
    console.log(
      `Node ${node_id} is listening on port ${BASE_NODE_PORT + node_id}`
    );

    setNodeIsReady(node_id);
  });

  return server;
}