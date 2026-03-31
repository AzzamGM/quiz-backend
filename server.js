const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: 'http://10.172.217.92:3000', // React frontend URL
    methods: ['GET', 'POST'],
  },
});

app.use(cors());
// --------------- Quiz Data ---------------
const QUESTIONS = [
  {
    question: 'ما هي عاصمة فرنسا؟',
    options: ['برلين', 'مدريد', 'باريس', 'روما'],
    correctIndex: 2,
    timeLimit: 15,
  },{
    question: 'ما هي عاصمة فرنسا؟',
    options: ['برلين', 'مدريد', 'باريس', 'روما'],
    correctIndex: 2,
    timeLimit: 15,
  },{
    question: 'ما هي عاصمة فرنسا؟',
    options: ['برلين', 'مدريد', 'باريس', 'روما'],
    correctIndex: 2,
    timeLimit: 15,
  },{
    question: 'ما هي عاصمة فرنسا؟',
    options: ['برلين', 'مدريد', 'باريس', 'روما'],
    correctIndex: 2,
    timeLimit: 15,
  }
];

// --------------- Game State ---------------
let state = {
  phase: 'lobby', // 'lobby' | 'question' | 'between' | 'ended'
  currentQuestion: -1,
  hostId: null,
  players: new Map(), // socketId -> { name, score, answer, pointsEarned, lastPoints }
  ghost: new Map(),   // name.toLowerCase() -> { ...playerData, wasHost, disconnectTimer }
  timerInterval: null,
  secondsLeft: 0,
};

function getPlayers() {
  return Array.from(state.players.entries()).map(([id, p]) => ({
    id,
    name: p.name,
    score: p.score,
    lastPoints: p.lastPoints || 0,
  }));
}

function clearTimer() {
  if (state.timerInterval) {
    clearInterval(state.timerInterval);
    state.timerInterval = null;
  }
}

function endQuestion() {
  clearTimer();
  const q = QUESTIONS[state.currentQuestion];

  // Award speed-based points for correct answer (host skipped — answer stays null)
  state.players.forEach((player) => {
    if (player.answer === q.correctIndex) {
      player.score += player.pointsEarned || 0;
      player.lastPoints = player.pointsEarned || 0;
    } else {
      player.lastPoints = 0;
    }
  });

  state.phase = 'between';
  io.emit('question-result', {
    correctIndex: q.correctIndex,
    players: getPlayers(),
  });
}

function startQuestion(index) {
  state.currentQuestion = index;
  state.phase = 'question';
  state.secondsLeft = QUESTIONS[index].timeLimit;

  // Clear answers from previous round
  state.players.forEach((p) => { p.answer = null; p.pointsEarned = 0; });

  const q = QUESTIONS[index];
  io.emit('new-question', {
    questionIndex: index,
    totalQuestions: QUESTIONS.length,
    question: q.question,
    options: q.options,
    timeLimit: q.timeLimit,
    secondsLeft: state.secondsLeft,
  });

  clearTimer();
  state.timerInterval = setInterval(() => {
    state.secondsLeft -= 1;
    io.emit('timer-tick', state.secondsLeft);
    if (state.secondsLeft <= 0) {
      endQuestion();
    }
  }, 1000);
}

function resetGame() {
  clearTimer();
  state.phase = 'lobby';
  state.currentQuestion = -1;
  state.ghost.forEach((g) => clearTimeout(g.disconnectTimer));
  state.ghost.clear();
  state.players.forEach((p) => {
    p.score = 0;
    p.answer = null;
    p.pointsEarned = 0;
    p.lastPoints = 0;
  });
}

// --------------- Socket Events ---------------
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('join', ({ name }) => {
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      socket.emit('join-error', 'الاسم غير صالح');
      return;
    }
    const trimmedName = name.trim().slice(0, 20);
    const key = trimmedName.toLowerCase();

    // ---- Reconnect: restore ghost player ----
    if (state.ghost.has(key)) {
      const ghost = state.ghost.get(key);
      clearTimeout(ghost.disconnectTimer);
      state.ghost.delete(key);

      state.players.set(socket.id, {
        name: ghost.name,
        score: ghost.score,
        answer: ghost.answer,
        pointsEarned: ghost.pointsEarned,
        lastPoints: ghost.lastPoints,
      });

      if (ghost.wasHost || !state.hostId) {
        state.hostId = socket.id;
      }

      socket.emit('joined', {
        playerId: socket.id,
        hostId: state.hostId,
        players: getPlayers(),
        phase: state.phase,
        // Send current question state so client can restore mid-game
        currentQuestion: state.currentQuestion >= 0 ? {
          questionIndex: state.currentQuestion,
          totalQuestions: QUESTIONS.length,
          question: QUESTIONS[state.currentQuestion].question,
          options: QUESTIONS[state.currentQuestion].options,
          timeLimit: QUESTIONS[state.currentQuestion].timeLimit,
          secondsLeft: state.secondsLeft,
        } : null,
      });

      io.emit('player-joined', { hostId: state.hostId, players: getPlayers() });
      console.log('Player reconnected:', ghost.name);
      return;
    }

    // ---- Normal join ----
    // Reject duplicate names
    let isDuplicate = false;
    state.players.forEach((p) => {
      if (p.name.toLowerCase() === key) isDuplicate = true;
    });
    if (isDuplicate) {
      socket.emit('join-error', 'الاسم مستخدم');
      return;
    }

    if (state.phase !== 'lobby') {
      socket.emit('join-error', 'اللعبة جارية');
      return;
    }

    state.players.set(socket.id, { name: trimmedName, score: 0, answer: null, pointsEarned: 0, lastPoints: 0 });

    // First player becomes host automatically
    if (!state.hostId) {
      state.hostId = socket.id;
    }

    socket.emit('joined', {
      playerId: socket.id,
      hostId: state.hostId,
      players: getPlayers(),
      phase: state.phase,
    });

    socket.broadcast.emit('player-joined', {
      hostId: state.hostId,
      players: getPlayers(),
    });
  });

  socket.on('start-quiz', () => {
    if (socket.id !== state.hostId) return;
    if (state.phase !== 'lobby') return;
    if (state.players.size < 1) return;
    state.phase = 'countdown';
    io.emit('game-countdown');
    setTimeout(() => startQuestion(0), 4000);
  });

  socket.on('submit-answer', ({ questionIndex, answerIndex }) => {
    const player = state.players.get(socket.id);
    if (!player) return;
    if (state.phase !== 'question') return;
    if (questionIndex !== state.currentQuestion) return;
    if (typeof answerIndex !== 'number' || answerIndex < 0 || answerIndex > 3) return;
    if (player.answer !== null) return; // already answered

    player.answer = answerIndex;
    // Points scale with remaining time: max 1000 at instant answer, min 100 at last second
    const q = QUESTIONS[state.currentQuestion];
    player.pointsEarned = Math.max(100, Math.round(1000 * (state.secondsLeft / q.timeLimit)));
    socket.emit('answer-received', { answerIndex, pointsEarned: player.pointsEarned });

    // End early if every non-host player has answered
    let allAnswered = true;
    state.players.forEach((p, id) => {
      if (id !== state.hostId && p.answer === null) allAnswered = false;
    });
    if (allAnswered) endQuestion();
  });

  socket.on('next-question', () => {
    if (socket.id !== state.hostId) return;
    if (state.phase !== 'between') return;
    const next = state.currentQuestion + 1;
    if (next >= QUESTIONS.length) {
      state.phase = 'ended';
      clearTimer();
      io.emit('game-over', { players: getPlayers() });
    } else {
      startQuestion(next);
    }
  });

  socket.on('restart-game', () => {
    if (socket.id !== state.hostId) return;
    resetGame();
    io.emit('game-reset', { players: getPlayers(), hostId: state.hostId });
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    const player = state.players.get(socket.id);
    if (!player) return;

    const wasHost = state.hostId === socket.id;

    // Move to ghost map — keep state alive for 45 seconds to allow reconnect
    const timer = setTimeout(() => {
      state.ghost.delete(player.name.toLowerCase());
      console.log('Ghost expired for:', player.name);

      // If they were host, reassign now
      if (wasHost) {
        const nextId = state.players.keys().next().value;
        state.hostId = nextId || null;
        if (state.players.size > 0) {
          io.emit('player-left', { hostId: state.hostId, players: getPlayers() });
        }
      }

      if (state.players.size === 0 && state.ghost.size === 0) {
        clearTimer();
        state.phase = 'lobby';
        state.currentQuestion = -1;
        state.hostId = null;
      }

      // Check all-answered after ghost expires
      if (state.phase === 'question') {
        let allAnswered = true;
        state.players.forEach((p, id) => {
          if (id !== state.hostId && p.answer === null) allAnswered = false;
        });
        if (allAnswered && state.players.size > 0) endQuestion();
      }
    }, 150000);

    state.ghost.set(player.name.toLowerCase(), {
      ...player,
      wasHost,
      disconnectTimer: timer,
    });

    state.players.delete(socket.id);

    // Temporarily reassign host so others see a host
    if (wasHost) {
      const nextId = state.players.keys().next().value;
      state.hostId = nextId || null;
    }

    if (state.players.size === 0) {
      // Everyone disconnected — pause but keep ghost timers running
      return;
    }

    io.emit('player-left', {
      hostId: state.hostId,
      players: getPlayers(),
    });

    // Check if all remaining active players have answered
    if (state.phase === 'question') {
      let allAnswered = true;
      state.players.forEach((p, id) => {
        if (id !== state.hostId && p.answer === null) allAnswered = false;
      });
      if (allAnswered) endQuestion();
    }
  });
});
server.listen(4000, () => {
  console.log('Server is running on port 4000');
});
