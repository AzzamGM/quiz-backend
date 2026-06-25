const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: true, // reflect the request origin — accept any LAN client
    methods: ['GET', 'POST'],
  },
});

app.use(cors());

// --------------- Rooms ---------------
// Each room is fully self-contained: its own questions, players, host and timers.
// roomCode (trimmed, as the host typed it) -> room
const rooms = new Map();

const GHOST_TTL = 150000; // keep a disconnected player's slot alive this long for reconnects
const STEAL_AMOUNT = 500; // points transferred by the "steal" power-up

// Power-up effects queued during the between-questions phase, applied to the NEXT question.
function freshPending() {
  return { remove2: false, double: false, halve: false, freeze: false };
}
// Effects active for the CURRENT question (derived from pending at question start).
function freshActive() {
  return { remove2: false, double: false, halve: false, frozen: false, removedOptions: [] };
}
function freshPlayer(name) {
  return {
    name,
    score: 0,
    answer: null,
    pointsEarned: 0,
    lastPoints: 0,
    pending: freshPending(),
    active: freshActive(),
    shield: 0, // >0 means protected from offensive power-ups (countdown over rounds)
    usedPowerups: new Set(), // power-up types used in the current between-round (once each)
    incoming: [],            // offensive power-ups used against this player, shown next round start
  };
}

// Pick up to 2 wrong option indices to hide (always leaving the correct answer + at least one wrong).
function pickRemovedOptions(q) {
  const wrong = q.options.map((_, i) => i).filter((i) => i !== q.correctIndex);
  for (let i = wrong.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [wrong[i], wrong[j]] = [wrong[j], wrong[i]];
  }
  const removeCount = Math.min(2, Math.max(0, wrong.length - 1));
  return wrong.slice(0, removeCount);
}

function createRoomState(code, displayCode, questions, limitNavigation) {
  return {
    code,                 // normalized (lowercased) key — used for lookups & socket.io rooms
    displayCode,          // original casing the host typed — shown to users / in share links
    questions,            // [{ question, options, correctIndex, timeLimit }]
    limitNavigation: limitNavigation !== false, // default true; players can't leave when on
    phase: 'lobby',       // 'lobby' | 'countdown' | 'question' | 'between' | 'ended'
    currentQuestion: -1,
    hostId: null,
    players: new Map(),   // socketId -> { name, score, answer, pointsEarned, lastPoints }
    ghost: new Map(),     // name.toLowerCase() -> { ...playerData, wasHost, disconnectTimer }
    timerInterval: null,
    secondsLeft: 0,
    resultTimer: null,
  };
}

function roomOf(socket) {
  const code = socket.data.roomCode;
  return code ? rooms.get(code) || null : null;
}

function getPlayers(room) {
  return Array.from(room.players.entries()).map(([id, p]) => ({
    id,
    name: p.name,
    score: p.score,
    lastPoints: p.lastPoints || 0,
    frozen: !!p.active.frozen,
  }));
}

function effectsFor(player) {
  return {
    frozen: player.active.frozen,
    removedOptions: player.active.removedOptions,
    double: player.active.double,
    halve: player.active.halve,
    attacks: (player.incoming || []).slice(), // offensive power-ups used against this player
  };
}

function questionPayload(room, index) {
  const q = room.questions[index];
  return {
    questionIndex: index,
    totalQuestions: room.questions.length,
    question: q.question,
    options: q.options,
    timeLimit: q.timeLimit,
    secondsLeft: room.secondsLeft,
  };
}

// Validate/normalize a host-supplied questions array. Returns a clean array or null.
function sanitizeQuestions(raw) {
  if (!Array.isArray(raw)) return null;
  const out = [];
  for (const q of raw) {
    if (!q || typeof q !== 'object') continue;
    const question = typeof q.question === 'string' ? q.question.trim() : '';
    const options = Array.isArray(q.options)
      ? q.options.map((o) => String(o).trim()).filter((o) => o.length > 0)
      : [];
    if (!question || options.length < 2) continue;

    let correctIndex = Number(q.correctIndex);
    if (!Number.isInteger(correctIndex) || correctIndex < 0 || correctIndex >= options.length) {
      correctIndex = 0;
    }

    let timeLimit = Number(q.timeLimit);
    if (!Number.isFinite(timeLimit) || timeLimit < 3) timeLimit = 15;
    timeLimit = Math.min(300, Math.round(timeLimit));

    out.push({ question, options, correctIndex, timeLimit });
  }
  return out;
}

function clearTimer(room) {
  if (room.timerInterval) {
    clearInterval(room.timerInterval);
    room.timerInterval = null;
  }
}

function clearResultTimer(room) {
  if (room.resultTimer) {
    clearInterval(room.resultTimer);
    room.resultTimer = null;
  }
}

function deleteRoom(room) {
  clearTimer(room);
  clearResultTimer(room);
  room.ghost.forEach((g) => clearTimeout(g.disconnectTimer));
  rooms.delete(room.code);
  console.log('Room deleted:', room.code);
}

// Remove a player immediately (intentional leave — no ghost/reconnect window).
function removePlayer(socket, room) {
  const player = room.players.get(socket.id);
  if (!player) return;
  const wasHost = room.hostId === socket.id;
  room.players.delete(socket.id);
  socket.leave(room.code);
  socket.data.roomCode = null;

  if (wasHost) {
    const nextId = room.players.keys().next().value;
    room.hostId = nextId || null;
  }
  if (room.players.size === 0 && room.ghost.size === 0) {
    deleteRoom(room);
    return;
  }
  io.to(room.code).emit('player-left', { hostId: room.hostId, players: getPlayers(room) });
  maybeEndOnAllAnswered(room);
}

function advanceToNext(room) {
  clearResultTimer(room);
  const next = room.currentQuestion + 1;
  if (next >= room.questions.length) {
    room.phase = 'ended';
    clearTimer(room);
    io.to(room.code).emit('game-over', { players: getPlayers(room) });
  } else {
    startQuestion(room, next);
  }
}

function endQuestion(room) {
  clearTimer(room);
  const q = room.questions[room.currentQuestion];

  // Award speed-based points for correct answer (host skipped — answer stays null).
  // Frozen players couldn't answer; double/halve power-ups adjust the gained points.
  room.players.forEach((player) => {
    if (player.answer === q.correctIndex && !player.active.frozen) {
      let pts = player.pointsEarned || 0;
      if (player.active.double) pts *= 2;
      if (player.active.halve) pts = Math.round(pts / 2);
      player.score += pts;
      player.lastPoints = pts;
    } else {
      player.lastPoints = 0;
    }
    // Clear this round's active effects (freeze marker, double/halve, removed options)
    player.active = freshActive();
    // A fresh between-round opens — every power-up is available once again.
    player.usedPowerups = new Set();
  });

  room.phase = 'between';
  io.to(room.code).emit('question-result', {
    correctIndex: q.correctIndex,
    players: getPlayers(room),
  });

  // Auto-advance after 20 seconds
  const AUTO_ADVANCE_SECS = 20;
  let resultSecsLeft = AUTO_ADVANCE_SECS;
  io.to(room.code).emit('result-tick', resultSecsLeft);
  room.resultTimer = setInterval(() => {
    resultSecsLeft -= 1;
    io.to(room.code).emit('result-tick', resultSecsLeft);
    if (resultSecsLeft <= 0) advanceToNext(room);
  }, 1000);
}

function startQuestion(room, index) {
  room.currentQuestion = index;
  room.phase = 'question';
  room.secondsLeft = room.questions[index].timeLimit;

  const q = room.questions[index];

  // Clear answers from previous round and turn pending power-ups into active effects.
  room.players.forEach((p) => {
    p.answer = null;
    p.pointsEarned = 0;
    p.active = freshActive();
    p.active.double = p.pending.double;
    p.active.halve = p.pending.halve;
    p.active.frozen = p.pending.freeze;
    p.active.remove2 = p.pending.remove2;
    if (p.pending.remove2) p.active.removedOptions = pickRemovedOptions(q);
    p.pending = freshPending();
    // Shield is a small countdown (set to 2 on use) so it survives one question and still
    // guards the NEXT round's attack window, then expires.
    if (p.shield > 0) p.shield -= 1;
  });

  io.to(room.code).emit('new-question', questionPayload(room, index));

  // Tell each affected player about the effects on their own screen, including a
  // summary of any offensive power-ups used against them since last round.
  room.players.forEach((p, id) => {
    const hasIncoming = p.incoming && p.incoming.length > 0;
    if (p.active.frozen || p.active.remove2 || p.active.double || p.active.halve || hasIncoming) {
      io.to(id).emit('your-effects', effectsFor(p));
    }
    p.incoming = []; // consumed — only shown once, at the start of the affected round
  });
  // Refresh scoreboards so the host sees freeze markers (❄️) on frozen players.
  io.to(room.code).emit('powerup-update', { players: getPlayers(room) });

  clearTimer(room);
  room.timerInterval = setInterval(() => {
    room.secondsLeft -= 1;
    io.to(room.code).emit('timer-tick', room.secondsLeft);
    if (room.secondsLeft <= 0) {
      endQuestion(room);
    }
  }, 1000);
}

function resetGame(room) {
  clearTimer(room);
  clearResultTimer(room);
  room.phase = 'lobby';
  room.currentQuestion = -1;
  room.ghost.forEach((g) => clearTimeout(g.disconnectTimer));
  room.ghost.clear();
  room.players.forEach((p) => {
    p.score = 0;
    p.answer = null;
    p.pointsEarned = 0;
    p.lastPoints = 0;
    p.pending = freshPending();
    p.active = freshActive();
    p.shield = 0;
    p.usedPowerups = new Set();
    p.incoming = [];
  });
}

// Everyone (active + ghosts) checked-out after all non-host players answered?
function maybeEndOnAllAnswered(room) {
  if (room.phase !== 'question') return;
  let allAnswered = true;
  room.players.forEach((p, id) => {
    // Frozen players can't answer, so they don't block the round from ending.
    if (id !== room.hostId && !p.active.frozen && p.answer === null) allAnswered = false;
  });
  if (allAnswered && room.players.size > 0) endQuestion(room);
}

// Shared logic for both creating and joining a room.
function doJoin(socket, room, name) {
  if (!name || typeof name !== 'string' || name.trim().length === 0) {
    socket.emit('join-error', 'الاسم غير صالح');
    return;
  }
  const trimmedName = name.trim().slice(0, 20);
  const key = trimmedName.toLowerCase();

  // ---- Reconnect: restore ghost player ----
  if (room.ghost.has(key)) {
    const ghost = room.ghost.get(key);
    clearTimeout(ghost.disconnectTimer);
    room.ghost.delete(key);

    const restored = {
      name: ghost.name,
      score: ghost.score,
      answer: ghost.answer,
      pointsEarned: ghost.pointsEarned,
      lastPoints: ghost.lastPoints,
      pending: ghost.pending || freshPending(),
      active: ghost.active || freshActive(),
      shield: ghost.shield || 0,
      usedPowerups: ghost.usedPowerups || new Set(),
      incoming: ghost.incoming || [],
    };
    room.players.set(socket.id, restored);
    socket.data.roomCode = room.code;
    socket.join(room.code);

    if (ghost.wasHost || !room.hostId) {
      room.hostId = socket.id;
    }

    socket.emit('joined', {
      roomCode: room.displayCode,
      playerId: socket.id,
      hostId: room.hostId,
      players: getPlayers(room),
      phase: room.phase,
      totalQuestions: room.questions.length,
      limitNavigation: room.limitNavigation,
      currentQuestion: room.currentQuestion >= 0 ? questionPayload(room, room.currentQuestion) : null,
      effects: room.phase === 'question' ? effectsFor(restored) : null,
    });

    io.to(room.code).emit('player-joined', { hostId: room.hostId, players: getPlayers(room) });
    console.log('Player reconnected:', ghost.name, 'room', room.code);
    return;
  }

  // ---- Normal join ----
  let isDuplicate = false;
  room.players.forEach((p) => {
    if (p.name.toLowerCase() === key) isDuplicate = true;
  });
  if (isDuplicate) {
    socket.emit('join-error', 'الاسم مستخدم');
    return;
  }

  if (room.phase !== 'lobby') {
    socket.emit('join-error', 'اللعبة جارية');
    return;
  }

  room.players.set(socket.id, freshPlayer(trimmedName));
  socket.data.roomCode = room.code;
  socket.join(room.code);

  // First player in the room becomes host automatically
  if (!room.hostId) {
    room.hostId = socket.id;
  }

  socket.emit('joined', {
    roomCode: room.displayCode,
    playerId: socket.id,
    hostId: room.hostId,
    players: getPlayers(room),
    phase: room.phase,
    totalQuestions: room.questions.length,
    limitNavigation: room.limitNavigation,
  });

  socket.broadcast.to(room.code).emit('player-joined', {
    hostId: room.hostId,
    players: getPlayers(room),
  });
}

// --------------- Socket Events ---------------
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('create-room', ({ roomCode, name, questions, limitNavigation }) => {
    const displayCode = typeof roomCode === 'string' ? roomCode.trim() : '';
    const code = displayCode.toLowerCase(); // case-insensitive key
    if (!code) {
      socket.emit('join-error', 'كود الغرفة غير صالح');
      return;
    }
    if (rooms.has(code)) {
      socket.emit('join-error', 'كود الغرفة مستخدم');
      return;
    }
    const sanitized = sanitizeQuestions(questions);
    if (!sanitized || sanitized.length === 0) {
      socket.emit('join-error', 'يجب إضافة سؤال واحد صالح على الأقل');
      return;
    }

    const room = createRoomState(code, displayCode, sanitized, limitNavigation);
    rooms.set(code, room);
    console.log('Room created:', displayCode, 'with', sanitized.length, 'questions', '| limitNavigation:', room.limitNavigation);
    doJoin(socket, room, name); // creator joins as the first player → becomes host
  });

  socket.on('join', ({ roomCode, name }) => {
    const code = typeof roomCode === 'string' ? roomCode.trim().toLowerCase() : '';
    const room = rooms.get(code);
    if (!room) {
      socket.emit('room-not-found');
      return;
    }
    doJoin(socket, room, name);
  });

  socket.on('start-quiz', () => {
    const room = roomOf(socket);
    if (!room) return;
    if (socket.id !== room.hostId) return;
    if (room.phase !== 'lobby') return;
    if (room.players.size < 1) return;
    room.phase = 'countdown';
    io.to(room.code).emit('game-countdown');
    setTimeout(() => {
      // Room may have been torn down during the countdown
      if (rooms.get(room.code) === room) startQuestion(room, 0);
    }, 4000);
  });

  socket.on('submit-answer', ({ questionIndex, answerIndex }) => {
    const room = roomOf(socket);
    if (!room) return;
    const player = room.players.get(socket.id);
    if (!player) return;
    if (room.phase !== 'question') return;
    if (questionIndex !== room.currentQuestion) return;
    const q = room.questions[room.currentQuestion];
    if (typeof answerIndex !== 'number' || answerIndex < 0 || answerIndex >= q.options.length) return;
    if (player.active.frozen) return; // frozen this round — can't answer
    if (player.answer !== null) return; // already answered

    player.answer = answerIndex;
    // Points scale with remaining time: max 1000 at instant answer, min 100 at last second
    player.pointsEarned = Math.max(100, Math.round(1000 * (room.secondsLeft / q.timeLimit)));
    socket.emit('answer-received', { answerIndex, pointsEarned: player.pointsEarned });

    maybeEndOnAllAnswered(room);
  });

  // Players use power-ups during the between-questions phase.
  socket.on('use-powerup', ({ type, targetId }) => {
    const room = roomOf(socket);
    if (!room) return;
    if (room.phase !== 'between') return; // only between questions
    if (socket.id === room.hostId) return; // the host doesn't play
    const user = room.players.get(socket.id);
    if (!user) return;

    const VALID = ['remove2', 'double', 'shield', 'halve', 'freeze', 'steal'];
    if (!VALID.includes(type)) return;

    const notice = (sid, message) => io.to(sid).emit('powerup-notice', { message });
    const plog = (msg) => console.log(`[powerup][room ${room.code}] ${user.name}: ${msg}`);

    // Each power-up can be used only once per between-round.
    if (user.usedPowerups.has(type)) {
      plog(`${type} REJECTED (already used this round)`);
      notice(socket.id, 'استخدمت هذه القدرة في هذه الجولة');
      return;
    }

    const isOffensive = type === 'halve' || type === 'freeze' || type === 'steal';
    let target = null;
    if (isOffensive) {
      if (!targetId || targetId === socket.id || targetId === room.hostId) return;
      target = room.players.get(targetId);
      if (!target) return;
    }

    // The attempt is consumed for this round (even if a shield later blocks it).
    user.usedPowerups.add(type);

    switch (type) {
      // ---- Self power-ups (affect your own next question) ----
      case 'remove2':
        user.pending.remove2 = true;
        plog('remove2 (self)');
        notice(socket.id, '✂️ ستُحذف إجابتان من سؤالك القادم');
        break;
      case 'double':
        user.pending.double = true;
        plog('double (self)');
        notice(socket.id, '⚡ نقاط سؤالك القادم مضاعفة');
        break;
      case 'shield':
        user.shield = 2; // guards the rest of this window and the next round's attack window
        plog('shield (self)');
        notice(socket.id, '🛡️ الدرع مُفعّل — أنت محمي من الهجمات هذه الجولة والقادمة');
        break;

      // ---- Offensive power-ups (target another player) ----
      case 'halve':
      case 'freeze':
      case 'steal': {
        if (target.shield) {
          plog(`${type} on ${target.name} → BLOCKED by shield`);
          notice(socket.id, `🛡️ ${target.name} محمي بالدرع — فشل الهجوم`);
          notice(targetId, `🛡️ صدّ درعُك هجوم ${user.name}`);
          // Still record it so the target sees what was attempted against them (blocked).
          target.incoming.push({ type, from: user.name, blocked: true });
          return;
        }

        if (type === 'halve') {
          target.pending.halve = true;
          target.incoming.push({ type: 'halve', from: user.name });
          plog(`halve → ${target.name}`);
          notice(socket.id, `½ ستُنصّف نقاط ${target.name} القادمة`);
          notice(targetId, `⚠️ ${user.name} سينصّف نقاطك في السؤال القادم`);
        } else if (type === 'freeze') {
          target.pending.freeze = true;
          target.incoming.push({ type: 'freeze', from: user.name });
          plog(`freeze → ${target.name}`);
          notice(socket.id, `❄️ تم تجميد ${target.name} للجولة القادمة`);
          notice(targetId, `❄️ ${user.name} جمّدك — لن تستطيع الإجابة في الجولة القادمة`);
        } else if (type === 'steal') {
          const amount = Math.min(STEAL_AMOUNT, target.score);
          target.score -= amount;
          user.score += amount;
          target.incoming.push({ type: 'steal', from: user.name, amount });
          plog(`steal ${amount} ← ${target.name}`);
          notice(socket.id, `💰 سرقت ${amount.toLocaleString()} نقطة من ${target.name}`);
          notice(targetId, `💰 ${user.name} سرق ${amount.toLocaleString()} نقطة منك`);
          io.to(room.code).emit('powerup-update', { players: getPlayers(room) });
        }
        break;
      }
    }
  });

  // A player intentionally leaves the room (only relevant when navigation isn't limited).
  socket.on('leave-room', () => {
    const room = roomOf(socket);
    if (!room) return;
    console.log('Player left room:', room.players.get(socket.id)?.name, 'room', room.code);
    removePlayer(socket, room);
  });

  // Host closes the whole room for everyone.
  socket.on('close-room', () => {
    const room = roomOf(socket);
    if (!room) return;
    if (socket.id !== room.hostId) return;
    console.log('Host closed room:', room.code);
    io.to(room.code).emit('room-closed');
    deleteRoom(room);
  });

  socket.on('next-question', () => {
    const room = roomOf(socket);
    if (!room) return;
    if (socket.id !== room.hostId) return;
    if (room.phase !== 'between') return;
    advanceToNext(room);
  });

  socket.on('restart-game', () => {
    const room = roomOf(socket);
    if (!room) return;
    if (socket.id !== room.hostId) return;
    resetGame(room);
    io.to(room.code).emit('game-reset', { players: getPlayers(room), hostId: room.hostId });
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    const room = roomOf(socket);
    if (!room) return;
    const player = room.players.get(socket.id);
    if (!player) return;

    const wasHost = room.hostId === socket.id;

    // Move to ghost map — keep state alive for a while to allow reconnect
    const timer = setTimeout(() => {
      room.ghost.delete(player.name.toLowerCase());
      console.log('Session expired for:', player.name, 'room', room.code);

      // If they were host, reassign now
      if (wasHost) {
        const nextId = room.players.keys().next().value;
        room.hostId = nextId || null;
        if (room.players.size > 0) {
          io.to(room.code).emit('player-left', { hostId: room.hostId, players: getPlayers(room) });
        }
      }

      if (room.players.size === 0 && room.ghost.size === 0) {
        deleteRoom(room);
        return;
      }

      maybeEndOnAllAnswered(room);
    }, GHOST_TTL);

    room.ghost.set(player.name.toLowerCase(), {
      ...player,
      wasHost,
      disconnectTimer: timer,
    });

    room.players.delete(socket.id);

    // Temporarily reassign host so others see a host
    if (wasHost) {
      const nextId = room.players.keys().next().value;
      room.hostId = nextId || null;
    }

    if (room.players.size === 0) {
      // Everyone disconnected — pause but keep ghost timers running
      return;
    }

    io.to(room.code).emit('player-left', {
      hostId: room.hostId,
      players: getPlayers(room),
    });

    maybeEndOnAllAnswered(room);
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
