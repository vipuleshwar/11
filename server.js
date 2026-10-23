const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e7
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const rooms = {};

function getDistanceMeters(lat1, lon1, lat2, lon2) {
  if (!lat1 || !lon1 || !lat2 || !lon2) return 999999;
  const R = 6371e3;
  const p1 = lat1 * Math.PI / 180;
  const p2 = lat2 * Math.PI / 180;
  const dp = (lat2 - lat1) * Math.PI / 180;
  const dl = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dp/2)**2 + Math.cos(p1) * Math.cos(p2) * Math.sin(dl/2)**2;
  return Math.round(R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

io.on('connection', (socket) => {
  let currentRoom = null;

  // 1. Create Room (Host)
  socket.on('create_room', (data) => {
    const roomCode = data.roomCode || `CAMPUS-${Math.floor(1000 + Math.random() * 9000)}`;
    currentRoom = roomCode;
    socket.join(roomCode);

    rooms[roomCode] = {
      code: roomCode,
      hostId: socket.id,
      started: false,
      paused: false,
      center: null, // Initialized on game start from host GPS
      settings: data.settings || {},
      maxPlayers: 20,
      minPlayersToStart: 5,
      taskPhotos: [],
      eliminationPhotos: [],
      rematchVotes: {}, // { socketId: true }
      players: {
        [socket.id]: {
          id: socket.id,
          callsign: data.callsign || 'HOST_LEADER',
          role: data.role || 'hider',
          isProtector: false,
          isImmuneUntil: 0,
          lat: data.lat || 0,
          lng: data.lng || 0,
          isCaught: false,
          catches: 0,
          tasksCompleted: 0,
          outOfBoundsSince: null
        }
      },
      pulseActive: false
    };

    socket.emit('room_created', { 
      roomCode, 
      player: rooms[roomCode].players[socket.id], 
      settings: rooms[roomCode].settings 
    });
    io.to(roomCode).emit('lobby_update', Object.values(rooms[roomCode].players));
  });

  // 2. Join Room (Operative)
  socket.on('join_room', ({ roomCode, callsign, preferredRole }) => {
    const room = rooms[roomCode];
    if (!room) return socket.emit('error_msg', 'Room not found. Check code.');
    if (room.started) return socket.emit('error_msg', 'Match already in progress!');
    if (Object.keys(room.players).length >= room.maxPlayers) {
      return socket.emit('error_msg', 'Room is full (Max 20 operatives).');
    }

    currentRoom = roomCode;
    socket.join(roomCode);

    let assignedRole = preferredRole || 'hider';
    if (room.settings.autoAssignRoles) {
      const existing = Object.values(room.players);
      const seekerCount = existing.filter(p => p.role === 'seeker').length;
      assignedRole = (seekerCount < (room.settings.seekerQuota || 2)) ? 'seeker' : 'hider';
    }

    room.players[socket.id] = {
      id: socket.id,
      callsign: callsign || `AGENT_${socket.id.slice(0, 4)}`,
      role: assignedRole,
      isProtector: false,
      isImmuneUntil: 0,
      lat: 0,
      lng: 0,
      isCaught: false,
      catches: 0,
      tasksCompleted: 0,
      outOfBoundsSince: null
    };

    socket.emit('joined_successfully', {
      roomCode,
      player: room.players[socket.id],
      settings: room.settings
    });

    io.to(roomCode).emit('lobby_update', Object.values(room.players));
  });

  // 3. Start Game (Enforce >= 5 players rule)
  socket.on('start_game', ({ hostLat, hostLng }) => {
    const room = rooms[currentRoom];
    if (!room || room.hostId !== socket.id) return;

    const count = Object.keys(room.players).length;
    if (count < 5) {
      return socket.emit('error_msg', `Cannot start: Minimum 5 players required to launch session! (Currently: ${count}/5)`);
    }

    room.started = true;
    room.paused = false;
    room.taskPhotos = [];
    room.eliminationPhotos = [];
    room.rematchVotes = {};

    // Center detection: If host GPS provided, use host location, otherwise fallback
    if (hostLat && hostLng) {
      room.center = { lat: hostLat, lng: hostLng };
    } else {
      const hostP = room.players[socket.id];
      if (hostP && hostP.lat) {
        room.center = { lat: hostP.lat, lng: hostP.lng };
      }
    }

    const hiders = Object.values(room.players).filter(p => p.role === 'hider');
    if (room.settings.enableGuardian) {
      hiders.forEach((h, idx) => {
        h.isProtector = (idx < 2);
      });
    }

    Object.values(room.players).forEach(p => {
      p.isCaught = false;
      p.catches = 0;
      p.tasksCompleted = 0;
      p.isImmuneUntil = 0;
      p.outOfBoundsSince = null;
    });

    io.to(currentRoom).emit('game_started', {
      settings: room.settings,
      center: room.center,
      players: Object.values(room.players)
    });
  });

  // 4. Pause & Resume Game Controls
  socket.on('toggle_pause_game', () => {
    const room = rooms[currentRoom];
    if (!room || room.hostId !== socket.id || !room.started) return;

    room.paused = !room.paused;
    io.to(currentRoom).emit('game_pause_state', {
      paused: room.paused,
      byHost: room.players[socket.id].callsign
    });

    io.to(currentRoom).emit('chat_message', {
      sender: 'SATELLITE',
      role: 'system',
      text: room.paused ? '⏸️ MATCH PAUSED BY HOST! Movement & eliminations frozen.' : '▶️ MATCH RESUMED BY HOST! Mission back live.',
      time: new Date().toLocaleTimeString().slice(3, 8)
    });
  });

  // 5. GPS Stream, Range Tracking & Out-of-Zone Enforcement
  socket.on('send_gps', ({ lat, lng }) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id] || room.paused) return;

    const me = room.players[socket.id];
    me.lat = lat;
    me.lng = lng;

    const elimRadius = room.settings.catchRadius || 10;
    const now = Date.now();

    // Check Seeker lock on active Hiders
    if (me.role === 'seeker') {
      let targetInScope = null;
      let targetDist = 9999;

      Object.values(room.players).forEach(other => {
        if (other.role === 'hider' && !other.isCaught) {
          const d = getDistanceMeters(me.lat, me.lng, other.lat, other.lng);
          if (d <= elimRadius && d < targetDist) {
            const isImmune = other.isImmuneUntil && other.isImmuneUntil > now;
            targetDist = d;
            targetInScope = {
              id: other.id,
              name: other.callsign,
              distance: d,
              isImmune: isImmune,
              immuneRemaining: isImmune ? Math.ceil((other.isImmuneUntil - now) / 1000) : 0
            };
          }
        }
      });

      socket.emit('target_scope_update', targetInScope);
    }

    // Proximity klaxon alert (<50m)
    if (me.role === 'hider' && !me.isCaught) {
      let minSeekerDist = 99999;
      Object.values(room.players).forEach(other => {
        if (other.role === 'seeker') {
          const d = getDistanceMeters(me.lat, me.lng, other.lat, other.lng);
          if (d < minSeekerDist) minSeekerDist = d;
        }
      });

      if (minSeekerDist <= 50) {
        socket.emit('proximity_alert', { distance: minSeekerDist });
      }
    }

    // Mandatory Out-of-Zone Boundary Check (CANNOT BE OFF)
    if (room.center && room.center.lat && !me.isCaught) {
      const distFromCenter = getDistanceMeters(me.lat, me.lng, room.center.lat, room.center.lng);
      const zoneLimit = room.settings.zoneRadius || 500;
      const graceSeconds = room.settings.gracePeriodSec || 10;

      if (distFromCenter > zoneLimit) {
        if (!me.outOfBoundsSince) me.outOfBoundsSince = now;
        const secondsOut = Math.floor((now - me.outOfBoundsSince) / 1000);
        const rem = Math.max(0, graceSeconds - secondsOut);
        socket.emit('zone_warning', { secondsRemaining: rem, isOutOfBounds: true });

        if (secondsOut >= graceSeconds) {
          executeElimination(room, me, `Eliminated: Stayed outside play boundary for ${graceSeconds} seconds!`, null);
        }
      } else {
        if (me.outOfBoundsSince) {
          me.outOfBoundsSince = null;
          socket.emit('zone_warning', { isOutOfBounds: false });
        }
      }
    }

    broadcastTacticalTelemetry(room);
  });

  // 6. Direct Elimination (Instant 1-Tap)
  socket.on('direct_eliminate_target', ({ targetHiderId }) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id] || room.paused) return;

    const seeker = room.players[socket.id];
    const target = room.players[targetHiderId];

    if (!seeker || seeker.role !== 'seeker' || !target || target.role !== 'hider' || target.isCaught) return;

    if (target.isImmuneUntil && target.isImmuneUntil > Date.now()) {
      return socket.emit('error_msg', `CANNOT ELIMINATE! ${target.callsign} is shielded by a Guardian Angel!`);
    }

    const allowed = (room.settings.catchRadius || 10) + 4;
    const dist = getDistanceMeters(seeker.lat, seeker.lng, target.lat, target.lng);
    if (dist > allowed) {
      return socket.emit('error_msg', `Target escaped! Distance is ${dist}m (Limit: ${room.settings.catchRadius}m)`);
    }

    executeElimination(room, target, `Directly tagged by ${seeker.callsign}!`, seeker);
  });

  // 7. Photo Elimination
  socket.on('confirm_photo_elimination', ({ targetHiderId, photoData }) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id] || room.paused) return;

    const seeker = room.players[socket.id];
    const target = room.players[targetHiderId];

    if (!seeker || seeker.role !== 'seeker' || !target || target.role !== 'hider' || target.isCaught) return;

    if (target.isImmuneUntil && target.isImmuneUntil > Date.now()) {
      return socket.emit('error_msg', `CANNOT ELIMINATE! ${target.callsign} is shielded by a Guardian Angel!`);
    }

    const allowed = (room.settings.catchRadius || 10) + 4;
    const dist = getDistanceMeters(seeker.lat, seeker.lng, target.lat, target.lng);
    if (dist > allowed) {
      return socket.emit('error_msg', `Target escaped! Distance is ${dist}m (Limit: ${room.settings.catchRadius}m)`);
    }

    const elimRecord = {
      seeker: seeker.callsign,
      target: target.callsign,
      photoData: photoData,
      time: new Date().toLocaleTimeString().slice(3, 8)
    };
    room.eliminationPhotos.push(elimRecord);

    executeElimination(room, target, `Eliminated with verified photo proof by ${seeker.callsign}!`, seeker, photoData);
  });

  function executeElimination(room, target, reason, seeker = null, photoData = null) {
    if (target.isCaught) return;
    target.isCaught = true;

    if (seeker) {
      seeker.catches = (seeker.catches || 0) + 1;
    }

    io.to(target.id).emit('you_are_eliminated', {
      reason: reason,
      bySeeker: seeker ? seeker.callsign : 'SYSTEM BOUNDARY',
      photoData: photoData,
      canSpectate: true,
      canBeProtector: true
    });

    let convertedToSeeker = false;
    if (room.settings.enableInfected && seeker) {
      const activeSeekers = Object.values(room.players).filter(p => p.role === 'seeker').length;
      if (activeSeekers < 2) {
        target.role = 'seeker';
        target.isCaught = false;
        convertedToSeeker = true;
        io.to(target.id).emit('converted_to_seeker', {
          msg: "☣️ INFECTED MODE: You are now a Seeker! Hunt the remaining runners!"
        });
      }
    }

    io.to(room.code).emit('player_officially_eliminated', {
      seekerName: seeker ? seeker.callsign : 'OUT OF BOUNDS',
      targetName: target.callsign,
      targetId: target.id,
      totalCatches: seeker ? seeker.catches : 0,
      convertedToSeeker: convertedToSeeker,
      reason: reason
    });

    io.to(room.code).emit('chat_message', {
      sender: 'SATELLITE',
      role: 'system',
      text: `🚨 ${target.callsign} was eliminated! (${reason})`,
      time: new Date().toLocaleTimeString().slice(3, 8)
    });

    const remainingHiders = Object.values(room.players).filter(p => p.role === 'hider' && !p.isCaught);
    if (remainingHiders.length === 0) {
      io.to(room.code).emit('game_over', {
        winner: 'SEEKERS',
        reason: 'All hiders have been eliminated!',
        standings: Object.values(room.players),
        photos: room.taskPhotos,
        eliminations: room.eliminationPhotos
      });
    }
  }

  // 8. Guardian Angel Shield
  socket.on('activate_guardian_shield', ({ targetHiderId }) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id] || room.paused) return;

    const guardian = room.players[socket.id];
    const target = room.players[targetHiderId];
    if (!target || target.isCaught) return;

    target.isImmuneUntil = Date.now() + 60000;

    io.to(currentRoom).emit('guardian_shield_activated', {
      guardianName: guardian.callsign,
      targetName: target.callsign,
      targetId: target.id,
      duration: 60
    });

    io.to(currentRoom).emit('chat_message', {
      sender: 'ANGEL',
      role: 'guardian',
      text: `👼 GUARDIAN REVIVE! ${guardian.callsign} shielded ${target.callsign}! Seekers cannot eliminate them for 60 seconds!`,
      time: new Date().toLocaleTimeString().slice(3, 8)
    });
  });

  // 9. Radar Pulse
  socket.on('trigger_pulse', () => {
    const room = rooms[currentRoom];
    if (!room || room.paused) return;
    room.pulseActive = true;
    io.to(currentRoom).emit('pulse_state', { active: true, duration: 10 });
    broadcastTacticalTelemetry(room);

    setTimeout(() => {
      if (rooms[currentRoom]) {
        rooms[currentRoom].pulseActive = false;
        io.to(currentRoom).emit('pulse_state', { active: false });
        broadcastTacticalTelemetry(rooms[currentRoom]);
      }
    }, 10000);
  });

  // 10. Photo Tasks
  socket.on('submit_task_photo', ({ taskId, taskTitle, photoData }) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id] || room.paused) return;

    const me = room.players[socket.id];
    me.tasksCompleted = (me.tasksCompleted || 0) + 1;

    const evidence = {
      taskId,
      taskTitle,
      photoData,
      uploader: me.callsign,
      time: new Date().toLocaleTimeString().slice(3, 8)
    };
    room.taskPhotos.push(evidence);

    io.to(currentRoom).emit('chat_message', {
      sender: 'INTEL',
      role: 'system',
      text: `📸 ${me.callsign} captured verified landmark proof: ${taskTitle}!`,
      time: evidence.time
    });

    const totalHiders = Object.values(room.players).filter(p => p.role === 'hider');
    const totalRequired = (room.settings.taskCount || 2) * (totalHiders.length || 1);
    const totalDone = totalHiders.reduce((acc, h) => acc + (h.tasksCompleted || 0), 0);

    if (totalDone >= totalRequired && totalRequired > 0) {
      io.to(currentRoom).emit('game_over', {
        winner: 'HIDERS',
        reason: 'Extraction quota complete! All photo tasks verified!',
        standings: Object.values(room.players),
        photos: room.taskPhotos,
        eliminations: room.eliminationPhotos
      });
    }
  });

  // 11. Rematch Roster & Voting Mechanism
  socket.on('vote_rematch', () => {
    const room = rooms[currentRoom];
    if (!room) return;

    room.rematchVotes[socket.id] = true;
    const votedNames = Object.keys(room.rematchVotes).map(id => room.players[id]?.callsign).filter(Boolean);
    const totalConnected = Object.keys(room.players).length;

    io.to(currentRoom).emit('rematch_roster_update', {
      votedList: votedNames,
      totalVoted: votedNames.length,
      totalNeeded: totalConnected
    });
  });

  socket.on('request_rematch', () => {
    const room = rooms[currentRoom];
    if (!room) return;
    room.started = false;
    room.paused = false;
    room.taskPhotos = [];
    room.eliminationPhotos = [];
    room.rematchVotes = {};

    Object.values(room.players).forEach(p => {
      p.isCaught = false;
      p.catches = 0;
      p.tasksCompleted = 0;
      p.isImmuneUntil = 0;
      p.outOfBoundsSince = null;
    });

    io.to(currentRoom).emit('return_to_lobby', {
      roomCode: room.code,
      players: Object.values(room.players),
      settings: room.settings
    });
  });

  // 12. Voice & Chat
  socket.on('voice_audio_chunk', (chunk) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id]) return;
    const myRole = room.players[socket.id].role;
    Object.values(room.players).forEach(p => {
      if (p.id !== socket.id && p.role === myRole) {
        io.to(p.id).emit('receive_voice_chunk', { sender: room.players[socket.id].callsign, audio: chunk });
      }
    });
  });

  socket.on('send_chat', (text) => {
    const room = rooms[currentRoom];
    if (!room || !room.players[socket.id]) return;
    io.to(currentRoom).emit('chat_message', {
      sender: room.players[socket.id].callsign,
      role: room.players[socket.id].role,
      text: text,
      time: new Date().toLocaleTimeString().slice(3, 8)
    });
  });

  socket.on('disconnect', () => {
    if (currentRoom && rooms[currentRoom]) {
      delete rooms[currentRoom].players[socket.id];
      delete rooms[currentRoom].rematchVotes[socket.id];
      io.to(currentRoom).emit('lobby_update', Object.values(rooms[currentRoom].players));
      if (Object.keys(rooms[currentRoom].players).length === 0) delete rooms[currentRoom];
    }
  });
});

function broadcastTacticalTelemetry(room) {
  const players = Object.values(room.players);

  players.forEach(client => {
    let visible = [];

    if (client.role === 'hider') {
      visible = players
        .filter(p => p.role === 'hider')
        .map(p => ({
          id: p.id,
          name: p.callsign,
          role: p.role,
          lat: p.lat,
          lng: p.lng,
          isCaught: p.isCaught,
          isProtector: p.isProtector,
          isImmune: p.isImmuneUntil > Date.now()
        }));
    } else if (client.role === 'seeker') {
      visible = players
        .filter(p => p.role === 'seeker' || (p.role === 'hider' && room.pulseActive && !p.isCaught))
        .map(p => ({
          id: p.id,
          name: p.callsign,
          role: p.role,
          lat: p.lat,
          lng: p.lng,
          isCaught: p.isCaught,
          isProtector: p.isProtector,
          isImmune: p.isImmuneUntil > Date.now()
        }));
    }

    io.to(client.id).emit('radar_sync', visible);
  });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`MAN-HUNT Tactical Server live on port ${PORT}`);
});
