import express from "express";
import bodyParser from "body-parser";
import { BASE_NODE_PORT } from "../config";
import { Value } from "../types";
import axios from "axios";

type NodeState = {
  killed: boolean;
  x: 0 | 1 | '?' | null;
  decided: boolean | null;
  k: number | null;
  consensusRunning: boolean;
  phase1Messages: Array<{ sender: number, value: 0 | 1 | '?', step: number }>;
  phase2Messages: Array<{ sender: number, value: 0 | 1 | '?', step: number }>;
};

export async function node(
  nodeId: number, 
  N: number, 
  F: number, 
  initialValue: Value, 
  isFaulty: boolean, 
  nodesAreReady: () => boolean, 
  setNodeIsReady: (index: number) => void 
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  let nodeState: NodeState = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : false,
    k: isFaulty ? null : 0,
    consensusRunning: false,
    phase1Messages: [],
    phase2Messages: []
  };

  node.get('/status', (req, res) => {
    return res.status(isFaulty ? 500 : 200).send(isFaulty ? 'faulty' : 'live');
  });

  node.get('/getState', (req, res) => {
    return res.status(200).json({
      killed: nodeState.killed,
      x: nodeState.x,
      decided: nodeState.decided,
      k: nodeState.k
    });
  });

  const flipCoin = (): 0 | 1 => (Math.random() < 0.5 ? 0 : 1);

  const broadcastMessage = async (phase: number, value: 0 | 1 | '?', step: number) => {
    if (nodeState.killed || isFaulty) return;
    await Promise.all(
      Array.from({ length: N }, (_, i) =>
        i !== nodeId ? axios.post(`http://localhost:${BASE_NODE_PORT + i}/message`, { sender: nodeId, phase, value, step }).catch(() => {}) : null
      ).filter(Boolean)
    );
  };

  node.post('/message', (req, res) => {
    if (nodeState.killed || isFaulty) return res.status(500).send('Node is faulty or killed');
    const { sender, phase, value, step } = req.body;
    if (typeof sender !== 'number' || typeof phase !== 'number' || typeof value === 'undefined' || typeof step !== 'number') {
      return res.status(400).send('Invalid message format');
    }
    if (step === nodeState.k) {
      if (phase === 1) nodeState.phase1Messages.push({ sender, value, step });
      if (phase === 2) nodeState.phase2Messages.push({ sender, value, step });
    }
    return res.status(200).send('Message received');
  });

  const runStep = async () => {
    if (nodeState.killed || isFaulty || nodeState.decided) return false;

    // Special case: N=1 decides immediately on initial value
    if (N === 1 && nodeState.k === 0) {
      nodeState.decided = true;
      nodeState.k = (nodeState.k as number) + 1;
      return false;
    }

    nodeState.phase1Messages = [];
    nodeState.phase2Messages = [];
    await broadcastMessage(1, nodeState.x as 0 | 1 | '?', nodeState.k as number);
    nodeState.phase1Messages.push({ sender: nodeId, value: nodeState.x as 0 | 1 | '?', step: nodeState.k as number });
    await new Promise(resolve => setTimeout(resolve, 10));

    const phase1Counts = { 0: 0, 1: 0, '?': 0 };
    nodeState.phase1Messages.forEach(msg => phase1Counts[msg.value]++);
    const healthyThreshold = Math.floor((N - F) / 2) + 1; // Majority of healthy nodes
    let phase2Value: 0 | 1 | '?' = '?';
    if (phase1Counts[0] >= healthyThreshold) phase2Value = 0;
    else if (phase1Counts[1] >= healthyThreshold) phase2Value = 1;

    await broadcastMessage(2, phase2Value, nodeState.k as number);
    nodeState.phase2Messages.push({ sender: nodeId, value: phase2Value, step: nodeState.k as number });
    await new Promise(resolve => setTimeout(resolve, 10));

    const phase2Counts = { 0: 0, 1: 0, '?': 0 };
    nodeState.phase2Messages.forEach(msg => phase2Counts[msg.value]++);
    if (phase2Counts[0] >= healthyThreshold) {
      nodeState.x = 0;
      nodeState.decided = true;
    } else if (phase2Counts[1] >= healthyThreshold) {
      nodeState.x = 1;
      nodeState.decided = true;
    } else if (phase2Counts[0] > phase2Counts[1] && phase2Counts[0] > 0) {
      nodeState.x = 0; // Prefer the more common value if any
      nodeState.decided = false;
    } else if (phase2Counts[1] > phase2Counts[0] && phase2Counts[1] > 0) {
      nodeState.x = 1;
      nodeState.decided = false;
    } else {
      nodeState.x = flipCoin();
      nodeState.decided = false;
    }
    nodeState.k = (nodeState.k as number) + 1;
    return !nodeState.decided;
  };

  node.get('/start', async (req, res) => {
    if (nodeState.killed || isFaulty) return res.status(500).send('Node is faulty or killed');
    if (nodeState.consensusRunning) return res.status(200).send('Consensus already running');
    nodeState.k = 0;
    nodeState.decided = false;
    nodeState.consensusRunning = true;
    (async () => {
      while (await runStep() && !nodeState.killed) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      nodeState.consensusRunning = false;
    })();
    return res.status(200).send('Consensus started');
  });

  node.get('/stop', async (req, res) => {
    nodeState.killed = true;
    nodeState.consensusRunning = false;
    nodeState.x = null;
    nodeState.decided = null;
    nodeState.k = null;
    return res.status(200).send('Consensus stopped');
  });

  const server = node.listen(BASE_NODE_PORT + nodeId, () => setNodeIsReady(nodeId));
  return server;
}