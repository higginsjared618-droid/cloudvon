const { ytmp3, tiktok, facebook, instagram, twitter, ytmp4 } = require('sadaslk-dlcore');
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const ytdl = require('ytdl-core');
const yts = require('yt-search');
const FileType = require('file-type');
const AdmZip = require('adm-zip');
const mongoose = require('mongoose');

if (fs.existsSync('2nd_dev_config.env')) require('dotenv').config({ path: './2nd_dev_config.env' });

const { sms } = require("./msg");

const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    makeCacheableSignalKeyStore,
    Browsers,
    jidNormalizedUser,
    proto,
    prepareWAMessageMedia,
    downloadContentFromMessage,
    getContentType,
    generateWAMessageFromContent,
    DisconnectReason,
    fetchLatestBaileysVersion,
    getAggregateVotesInPollMessage
} = require('@whiskeysockets/baileys');

// FIX: Add makeInMemoryStore function
function makeInMemoryStore() {
    const store = {
        chats: new Map(),
        contacts: new Map(),
        messages: new Map(),
        groupMetadata: new Map(),
        state: { connection: 'close' },
        
        toJSON: () => ({
            chats: Array.from(store.chats.entries()),
            contacts: Array.from(store.contacts.entries()),
            messages: Array.from(store.messages.entries()),
            groupMetadata: Array.from(store.groupMetadata.entries())
        }),
        
        fromJSON: (json) => {
            if (json.chats) store.chats = new Map(json.chats);
            if (json.contacts) store.contacts = new Map(json.contacts);
            if (json.messages) store.messages = new Map(json.messages);
            if (json.groupMetadata) store.groupMetadata = new Map(json.groupMetadata);
        },
        
        writeToFile: (filePath) => {
            fs.writeFileSync(filePath, JSON.stringify(store.toJSON(), null, 2));
        },
        
        readFromFile: (filePath) => {
            if (fs.existsSync(filePath)) {
                const json = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                store.fromJSON(json);
            }
        },
        
        bind: (ev) => {
            ev.on('contacts.set', ({ contacts }) => {
                contacts.forEach(contact => {
                    store.contacts.set(contact.id, contact);
                });
            });
            
            ev.on('contacts.update', (updates) => {
                updates.forEach(update => {
                    const contact = store.contacts.get(update.id);
                    if (contact) {
                        Object.assign(contact, update);
                    }
                });
            });
            
            ev.on('chats.set', ({ chats }) => {
                chats.forEach(chat => {
                    store.chats.set(chat.id, chat);
                });
            });
            
            ev.on('chats.update', (updates) => {
                updates.forEach(update => {
                    const chat = store.chats.get(update.id);
                    if (chat) {
                        Object.assign(chat, update);
                    }
                });
            });
            
            ev.on('messages.upsert', ({ messages }) => {
                messages.forEach(msg => {
                    store.messages.set(msg.key.id, msg);
                });
            });
            
            ev.on('groups.update', (updates) => {
                updates.forEach(update => {
                    const metadata = store.groupMetadata.get(update.id);
                    if (metadata) {
                        Object.assign(metadata, update);
                    }
                });
            });
            
            ev.on('connection.update', (update) => {
                if (update.connection) {
                    store.state.connection = update.connection;
                }
            });
        }
    };
    return store;
}

// MongoDB Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://ellyongiro8:QwXDXE6tyrGpUTNb@cluster0.tyxcmm9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

process.env.NODE_ENV = 'production';
process.env.PM2_NAME = 'breshyb';

console.log('🚀 Auto Session Manager initialized with MongoDB Atlas');

const config = {
    AUTO_VIEW_STATUS: 'true',
    AUTO_LIKE_STATUS: 'true',
    AUTO_RECORDING: 'true',
    AUTO_LIKE_EMOJI: ['💗', '🩵', '🥺', '🫶', '😶'],
    AUTO_REACT_NEWSLETTERS: 'true',
    NEWSLETTER_JIDS: ['120363299029326322@newsletter','120363401297349965@newsletter','120363339980514201@newsletter','120363420947784745@newsletter','120363296314610373@newsletter'],
    NEWSLETTER_REACT_EMOJIS: ['🐥', '🤭', '♥️', '🙂', '☺️', '🩵', '🫶'],
    AUTO_SAVE_INTERVAL: 300000,
    AUTO_CLEANUP_INTERVAL: 900000,
    AUTO_RECONNECT_INTERVAL: 300000,
    AUTO_RESTORE_INTERVAL: 1800000,
    MONGODB_SYNC_INTERVAL: 600000,
    MAX_SESSION_AGE: 604800000,
    DISCONNECTED_CLEANUP_TIME: 300000,
    MAX_FAILED_ATTEMPTS: 3,
    INITIAL_RESTORE_DELAY: 10000,
    IMMEDIATE_DELETE_DELAY: 60000,
    PREFIX: '.',
    MAX_RETRIES: 3,
    GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JXaWiMrpjWyJ6Kd2G9FAAq?mode=ems_copy_t',
    NEWSLETTER_JID: '120363299029326322@newsletter',
    NEWSLETTER_MESSAGE_ID: '291',
    CHANNEL_LINK: 'https://whatsapp.com/channel/0029Vb6V5Xl6LwHgkapiAI0V',
    ADMIN_LIST_PATH: './admin.json',
    IMAGE_PATH: 'https://i.ibb.co/zhm2RF8j/vision-v.jpg',
    NUMBER_LIST_PATH: './numbers.json',
    SESSION_STATUS_PATH: './session_status.json',
    SESSION_BASE_PATH: './session',
    OTP_EXPIRY: 300000,
    NEWS_JSON_URL: 'https://raw.githubusercontent.com/boychalana9-max/mage/refs/heads/main/main.json?token=GHSAT0AAAAAADJU6UDFFZ67CUOLUQAAWL322F3RI2Q',
    OWNER_NUMBER: '254740007567',
    TRANSFER_OWNER_NUMBER: '254740007567',
};

const activeSockets = new Map();
const socketCreationTime = new Map();
const disconnectionTime = new Map();
const sessionHealth = new Map();
const reconnectionAttempts = new Map();
const lastBackupTime = new Map();
const otpStore = new Map();
const pendingSaves = new Map();
const restoringNumbers = new Set();
const sessionConnectionStatus = new Map();
const stores = new Map();
const followedNewsletters = new Map();

let autoSaveInterval;
let autoCleanupInterval;
let autoReconnectInterval;
let autoRestoreInterval;
let mongoSyncInterval;
let mongoConnected = false;

const sessionSchema = new mongoose.Schema({
    number: { type: String, required: true, unique: true, index: true },
    sessionData: { type: Object, required: true },
    status: { type: String, default: 'active', index: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    lastActive: { type: Date, default: Date.now },
    health: { type: String, default: 'active' }
});

const userConfigSchema = new mongoose.Schema({
    number: { type: String, required: true, unique: true, index: true },
    config: { type: Object, required: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
});

const Session = mongoose.model('Session', sessionSchema);
const UserConfig = mongoose.model('UserConfig', userConfigSchema);

async function initializeMongoDB() {
    try {
        if (mongoConnected) return true;
        await mongoose.connect(MONGODB_URI, {
            serverSelectionTimeoutMS: 30000,
            socketTimeoutMS: 45000,
            maxPoolSize: 10,
            minPoolSize: 5
        });
        mongoConnected = true;
        console.log('✅ MongoDB Atlas connected successfully');
        await Session.createIndexes();
        await UserConfig.createIndexes();
        return true;
    } catch (error) {
        console.error('❌ MongoDB connection error:', error.message);
        mongoConnected = false;
        setTimeout(() => initializeMongoDB(), 5000);
        return false;
    }
}

async function saveSessionToMongoDB(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Not saving inactive session: ${sanitizedNumber}`);
            return false;
        }
        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid session data: ${sanitizedNumber}`);
            return false;
        }
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            {
                sessionData: sessionData,
                status: 'active',
                updatedAt: new Date(),
                lastActive: new Date(),
                health: sessionHealth.get(sanitizedNumber) || 'active'
            },
            { upsert: true, new: true }
        );
        console.log(`✅ Session saved to MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB save failed: ${error.message}`);
        pendingSaves.set(number, { data: sessionData, timestamp: Date.now() });
        return false;
    }
}

async function loadSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const session = await Session.findOne({ 
            number: sanitizedNumber,
            status: { $ne: 'deleted' }
        });
        if (session) {
            console.log(`✅ Session loaded: ${sanitizedNumber}`);
            return session.sessionData;
        }
        return null;
    } catch (error) {
        console.error(`❌ MongoDB load failed: ${error.message}`);
        return null;
    }
}

async function deleteSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.deleteOne({ number: sanitizedNumber });
        await UserConfig.deleteOne({ number: sanitizedNumber });
        console.log(`🗑️ Session deleted: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB delete failed: ${error.message}`);
        return false;
    }
}

async function getAllActiveSessionsFromMongoDB() {
    try {
        const sessions = await Session.find({ 
            status: 'active',
            health: { $ne: 'invalid' }
        });
        console.log(`📊 Found ${sessions.length} active sessions`);
        return sessions;
    } catch (error) {
        console.error('❌ Failed to get sessions:', error.message);
        return [];
    }
}

async function updateSessionStatusInMongoDB(number, status, health = null) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const updateData = { status: status, updatedAt: new Date() };
        if (health) updateData.health = health;
        if (status === 'active') updateData.lastActive = new Date();
        await Session.findOneAndUpdate({ number: sanitizedNumber }, updateData);
        console.log(`📝 Status updated: ${sanitizedNumber} -> ${status}`);
        return true;
    } catch (error) {
        console.error(`❌ Status update failed: ${error.message}`);
        return false;
    }
}

async function cleanupInactiveSessionsFromMongoDB() {
    try {
        const result = await Session.deleteMany({
            $or: [
                { status: 'disconnected' },
                { status: 'invalid' },
                { status: 'failed' },
                { status: 'bad_mac_cleared' },
                { health: 'invalid' },
                { health: 'disconnected' },
                { health: 'bad_mac_cleared' }
            ]
        });
        console.log(`🧹 Cleaned ${result.deletedCount} inactive sessions`);
        return result.deletedCount;
    } catch (error) {
        console.error('❌ Cleanup failed:', error.message);
        return 0;
    }
}

async function getMongoSessionCount() {
    try {
        const count = await Session.countDocuments({ status: 'active' });
        return count;
    } catch (error) {
        console.error('❌ Failed to count sessions:', error.message);
        return 0;
    }
}

async function saveUserConfigToMongoDB(number, configData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await UserConfig.findOneAndUpdate(
            { number: sanitizedNumber },
            { config: configData, updatedAt: new Date() },
            { upsert: true, new: true }
        );
        console.log(`✅ Config saved: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Config save failed: ${error.message}`);
        return false;
    }
}

async function loadUserConfigFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const userConfig = await UserConfig.findOne({ number: sanitizedNumber });
        if (userConfig) {
            console.log(`✅ Config loaded: ${sanitizedNumber}`);
            return userConfig.config;
        }
        return null;
    } catch (error) {
        console.error(`❌ Config load failed: ${error.message}`);
        return null;
    }
}

function initializeDirectories() {
    const dirs = [config.SESSION_BASE_PATH, './temp'];
    dirs.forEach(dir => {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`📁 Created directory: ${dir}`);
        }
    });
}
initializeDirectories();

async function validateSessionData(sessionData) {
    try {
        if (!sessionData || typeof sessionData !== 'object') return false;
        if (!sessionData.me || !sessionData.myAppStateKeyId) return false;
        const requiredFields = ['noiseKey', 'signedIdentityKey', 'signedPreKey', 'registrationId'];
        for (const field of requiredFields) {
            if (!sessionData[field]) {
                console.warn(`⚠️ Missing field: ${field}`);
                return false;
            }
        }
        return true;
    } catch (error) {
        console.error('❌ Validation error:', error);
        return false;
    }
}

async function handleBadMacError(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    console.log(`🔧 Handling Bad MAC: ${sanitizedNumber}`);
    try {
        if (activeSockets.has(sanitizedNumber)) {
            const socket = activeSockets.get(sanitizedNumber);
            try {
                if (socket?.ws) socket.ws.close();
                else if (socket?.end) socket.end();
                else if (socket?.logout) await socket.logout();
            } catch (e) {}
            activeSockets.delete(sanitizedNumber);
        }
        if (stores.has(sanitizedNumber)) stores.delete(sanitizedNumber);
        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            console.log(`🗑️ Removing files: ${sanitizedNumber}`);
            await fs.remove(sessionPath);
        }
        await deleteSessionFromMongoDB(sanitizedNumber);
        sessionHealth.set(sanitizedNumber, 'bad_mac_cleared');
        reconnectionAttempts.delete(sanitizedNumber);
        disconnectionTime.delete(sanitizedNumber);
        sessionConnectionStatus.delete(sanitizedNumber);
        pendingSaves.delete(sanitizedNumber);
        lastBackupTime.delete(sanitizedNumber);
        restoringNumbers.delete(sanitizedNumber);
        followedNewsletters.delete(sanitizedNumber);
        await updateSessionStatus(sanitizedNumber, 'bad_mac_cleared');
        console.log(`✅ Cleared Bad MAC: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed: ${error}`);
        return false;
    }
}

async function downloadAndSaveMedia(message, mediaType) {
    try {
        const stream = await downloadContentFromMessage(message, mediaType);
        let buffer = Buffer.from([]);
        for await (const chunk of stream) {
            buffer = Buffer.concat([buffer, chunk]);
        }
        return buffer;
    } catch (error) {
        console.error('Download Media Error:', error);
        throw error;
    }
}

function isOwner(sender) {
    const senderNumber = sender.replace('@s.whatsapp.net', '').replace(/[^0-9]/g, '');
    const ownerNumber = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    return senderNumber === ownerNumber;
}

function isSessionActive(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const health = sessionHealth.get(sanitizedNumber);
    const connectionStatus = sessionConnectionStatus.get(sanitizedNumber);
    const socket = activeSockets.get(sanitizedNumber);
    return (
        connectionStatus === 'open' &&
        health === 'active' &&
        socket &&
        socket.user &&
        !disconnectionTime.has(sanitizedNumber)
    );
}

function isSocketReady(socket) {
    if (!socket) return false;
    return socket.ws && socket.ws.readyState === socket.ws.OPEN;
}

async function saveSessionLocally(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Skipping save: ${sanitizedNumber}`);
            return false;
        }
        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid data: ${sanitizedNumber}`);
            return false;
        }
        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        await fs.ensureDir(sessionPath);
        await fs.writeFile(
            path.join(sessionPath, 'creds.json'),
            JSON.stringify(sessionData, null, 2)
        );
        console.log(`💾 Saved locally: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Local save failed: ${error}`);
        return false;
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const sessionData = await loadSessionFromMongoDB(sanitizedNumber);
        if (sessionData) {
            if (!validateSessionData(sessionData)) {
                console.warn(`⚠️ Invalid data: ${sanitizedNumber}`);
                await handleBadMacError(sanitizedNumber);
                return null;
            }
            await saveSessionLocally(sanitizedNumber, sessionData);
            console.log(`✅ Restored: ${sanitizedNumber}`);
            return sessionData;
        }
        return null;
    } catch (error) {
        console.error(`❌ Restore failed: ${error.message}`);
        if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
            await handleBadMacError(number);
        }
        return null;
    }
}

async function deleteSessionImmediately(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    console.log(`🗑️ Deleting: ${sanitizedNumber}`);
    if (activeSockets.has(sanitizedNumber)) {
        const socket = activeSockets.get(sanitizedNumber);
        try {
            if (socket?.ws) socket.ws.close();
            else if (socket?.end) socket.end();
            else if (socket?.logout) await socket.logout();
        } catch (e) {}
    }
    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
    if (fs.existsSync(sessionPath)) await fs.remove(sessionPath);
    await deleteSessionFromMongoDB(sanitizedNumber);
    activeSockets.delete(sanitizedNumber);
    stores.delete(sanitizedNumber);
    socketCreationTime.delete(sanitizedNumber);
    disconnectionTime.delete(sanitizedNumber);
    sessionHealth.delete(sanitizedNumber);
    reconnectionAttempts.delete(sanitizedNumber);
    lastBackupTime.delete(sanitizedNumber);
    sessionConnectionStatus.delete(sanitizedNumber);
    pendingSaves.delete(sanitizedNumber);
    restoringNumbers.delete(sanitizedNumber);
    followedNewsletters.delete(sanitizedNumber);
    console.log(`✅ Deleted: ${sanitizedNumber}`);
    return true;
}

async function updateSessionStatus(number, status, health = null) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    if (health) sessionHealth.set(sanitizedNumber, health);
    await updateSessionStatusInMongoDB(sanitizedNumber, status, health);
    const statusData = {};
    try {
        if (fs.existsSync(config.SESSION_STATUS_PATH)) {
            const existingData = await fs.readJSON(config.SESSION_STATUS_PATH);
            Object.assign(statusData, existingData);
        }
    } catch (e) {}
    statusData[sanitizedNumber] = {
        status,
        health: health || sessionHealth.get(sanitizedNumber) || 'unknown',
        timestamp: new Date().toISOString()
    };
    await fs.writeJSON(config.SESSION_STATUS_PATH, statusData, { spaces: 2 });
    console.log(`📝 Status updated: ${sanitizedNumber} -> ${status}`);
}

async function EmpirePair(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        console.log(`🔗 Pairing: ${sanitizedNumber}`);

        const store = makeInMemoryStore();
        stores.set(sanitizedNumber, store);

        const { state, saveCreds } = await useMultiFileAuthState(
            path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`)
        );

        const { version } = await fetchLatestBaileysVersion();
        const sock = makeWASocket({
            version,
            logger: pino({ level: 'silent' }),
            printQRInTerminal: true,
            browser: Browsers.macOS('Desktop'),
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' })),
            },
            generateHighQualityLinkPreview: true,
            syncFullHistory: true,
        });

        store.bind(sock.ev);
        activeSockets.set(sanitizedNumber, sock);
        socketCreationTime.set(sanitizedNumber, Date.now());
        sessionConnectionStatus.set(sanitizedNumber, 'connecting');

        sock.ev.on('creds.update', async () => {
            try {
                if (isSessionActive(sanitizedNumber)) {
                    await saveCreds();
                    await saveSessionLocally(sanitizedNumber, state.creds);
                    await saveSessionToMongoDB(sanitizedNumber, state.creds);
                    lastBackupTime.set(sanitizedNumber, Date.now());
                }
            } catch (error) {
                console.error(`❌ Creds update failed: ${error.message}`);
            }
        });

        sock.ev.on('connection.update', async (update) => {
            try {
                const { connection, lastDisconnect, qr } = update;
                if (qr) console.log(`📱 QR Code: ${sanitizedNumber}`);
                if (connection === 'open') {
                    console.log(`✅ Connected: ${sanitizedNumber}`);
                    sessionConnectionStatus.set(sanitizedNumber, 'open');
                    sessionHealth.set(sanitizedNumber, 'active');
                    await updateSessionStatus(sanitizedNumber, 'active', 'active');
                    disconnectionTime.delete(sanitizedNumber);
                    reconnectionAttempts.set(sanitizedNumber, 0);
                    const welcomeMessage = `✅ *BOT CONNECTED*\n\n📱 *Number:* ${sanitizedNumber}\n⏰ *Time:* ${moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss')}\n🔧 *Status:* Active\n\n*Use:* ${config.PREFIX}help`;
                    await sock.sendMessage(sock.user.id, { text: welcomeMessage });
                }
                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    console.log(`❌ Disconnected: ${sanitizedNumber}`, statusCode || lastDisconnect?.error);
                    sessionConnectionStatus.set(sanitizedNumber, 'close');
                    disconnectionTime.set(sanitizedNumber, Date.now());
                    if (statusCode === DisconnectReason.badSession) {
                        console.log(`⚠️ Bad session: ${sanitizedNumber}`);
                        await handleBadMacError(sanitizedNumber);
                        return;
                    }
                    if (statusCode === DisconnectReason.connectionLost || statusCode === DisconnectReason.connectionClosed) {
                        const attempts = reconnectionAttempts.get(sanitizedNumber) || 0;
                        if (attempts < config.MAX_FAILED_ATTEMPTS) {
                            reconnectionAttempts.set(sanitizedNumber, attempts + 1);
                            console.log(`🔄 Reconnect attempt ${attempts + 1}: ${sanitizedNumber}`);
                            setTimeout(() => EmpirePair(sanitizedNumber).catch(console.error), 5000);
                        } else {
                            console.log(`❌ Max attempts: ${sanitizedNumber}`);
                            await updateSessionStatus(sanitizedNumber, 'disconnected', 'failed');
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ Connection error: ${error}`);
            }
        });

        sock.ev.on('messages.upsert', async (m) => {
            try {
                const msg = m.messages[0];
                if (!msg.message || msg.key.fromMe) return;
                const sender = msg.key.remoteJid;
                const messageText = msg.message.conversation || 
                                   msg.message.extendedTextMessage?.text || 
                                   msg.message.imageMessage?.caption || '';
                const isOwnerMsg = isOwner(sender);

                if (messageText.startsWith(config.PREFIX)) {
                    const command = messageText.slice(config.PREFIX.length).trim().split(' ')[0].toLowerCase();
                    const args = messageText.slice(config.PREFIX.length + command.length).trim();
                    console.log(`📨 Command: ${command} ${args}`);

                    if (command === 'help' || command === 'menu') {
                        const helpMenu = `╭─━━━━━━━━━━━━━━━━━━━─╮
│   *BOT COMMANDS MENU*   │
╰─━━━━━━━━━━━━━━━━━━━─╯

*📱 BASIC*
${config.PREFIX}help - Show menu
${config.PREFIX}ping - Check speed
${config.PREFIX}owner - Contact owner
${config.PREFIX}status - Bot status

*🔧 SESSION*
${config.PREFIX}pair [num] - Pair session
${config.PREFIX}list - List sessions
${config.PREFIX}restart - Restart
${config.PREFIX}logout - Logout

*🎵 MEDIA*
${config.PREFIX}ytmp3 [url] - YouTube audio
${config.PREFIX}ytmp4 [url] - YouTube video
${config.PREFIX}tiktok [url] - TikTok
${config.PREFIX}fb [url] - Facebook
${config.PREFIX}ig [url] - Instagram

*👑 OWNER*
${config.PREFIX}bc [msg] - Broadcast
${config.PREFIX}eval [code] - Execute code
${config.PREFIX}delete [num] - Delete session

*Owner:* ${config.OWNER_NUMBER}
*Prefix:* ${config.PREFIX}`;
                        await sock.sendMessage(sender, { text: helpMenu });
                    }

                    else if (command === 'ping') {
                        const start = Date.now();
                        await sock.sendMessage(sender, { text: '🏓 Pinging...' });
                        const latency = Date.now() - start;
                        const status = {
                            latency: `${latency}ms`,
                            number: sanitizedNumber,
                            status: isSessionActive(sanitizedNumber) ? '✅ Active' : '❌ Inactive',
                            uptime: socketCreationTime.has(sanitizedNumber) ? 
                                Math.floor((Date.now() - socketCreationTime.get(sanitizedNumber)) / 1000) + 's' : 'N/A'
                        };
                        await sock.sendMessage(sender, { 
                            text: `🏓 *PONG!*\n📱 *Bot:* ${status.number}\n⚡ *Latency:* ${status.latency}\n🔧 *Status:* ${status.status}\n⏰ *Uptime:* ${status.uptime}` 
                        });
                    }

                    else if (command === 'owner' || command === 'dev') {
                        await sock.sendMessage(sender, { 
                            text: `👑 *OWNER*\n📱 *Number:* ${config.OWNER_NUMBER}\n💬 *Contact:* wa.me/${config.OWNER_NUMBER}` 
                        });
                    }

                    else if (command === 'status' || command === 'info') {
                        const activeSessionsCount = Array.from(activeSockets.keys())
                            .filter(num => isSessionActive(num)).length;
                        const statusMessage = `🤖 *BOT STATUS*\n📱 *Bot:* ${sanitizedNumber}\n🔧 *Status:* ${isSessionActive(sanitizedNumber) ? '✅ Active' : '❌ Inactive'}\n👥 *Sessions:* ${activeSessionsCount}\n⏰ *Time:* ${moment().tz('Africa/Nairobi').format('HH:mm:ss')}\n💾 *MongoDB:* ${mongoConnected ? '✅' : '❌'}`;
                        await sock.sendMessage(sender, { text: statusMessage });
                    }

                    else if (command === 'pair') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        const targetNumber = args.trim();
                        if (!targetNumber) {
                            await sock.sendMessage(sender, { text: '❌ Provide number!\nExample: .pair 254712345678' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: `🔗 Pairing: ${targetNumber}` });
                        try {
                            await EmpirePair(targetNumber);
                            await sock.sendMessage(sender, { text: `✅ Pairing started!\nCheck console for QR code.` });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Failed: ${error.message}` });
                        }
                    }

                    else if (command === 'list' || command === 'sessions') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        const activeNumbers = Array.from(activeSockets.keys()).filter(num => isSessionActive(num));
                        let listMessage = `📋 *ACTIVE SESSIONS*\nTotal: ${activeNumbers.length}\n\n`;
                        if (activeNumbers.length > 0) {
                            activeNumbers.forEach((num, index) => {
                                const creationTime = socketCreationTime.get(num);
                                const uptime = creationTime ? Math.floor((Date.now() - creationTime) / 1000) + 's' : 'N/A';
                                listMessage += `${index + 1}. *${num}* (${uptime})\n`;
                            });
                        } else listMessage += '*No active sessions*';
                        listMessage += `\n💾 *MongoDB:* ${await getMongoSessionCount()} sessions`;
                        await sock.sendMessage(sender, { text: listMessage });
                    }

                    else if (command === 'restart') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '🔄 Restarting...' });
                        if (sock.ws) sock.ws.close();
                        setTimeout(async () => {
                            try {
                                await EmpirePair(sanitizedNumber);
                                await sock.sendMessage(sender, { text: '✅ Restarted!' });
                            } catch (error) {
                                await sock.sendMessage(sender, { text: `❌ Failed: ${error.message}` });
                            }
                        }, 3000);
                    }

                    else if (command === 'logout' || command === 'stop') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '🚪 Logging out...' });
                        try {
                            await deleteSessionImmediately(sanitizedNumber);
                            await sock.sendMessage(sender, { text: '✅ Logged out!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Failed: ${error.message}` });
                        }
                    }

                    else if (command === 'bc' || command === 'broadcast') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        const broadcastMessage = args;
                        if (!broadcastMessage) {
                            await sock.sendMessage(sender, { text: '❌ Provide message!' });
                            return;
                        }
                        const activeNumbers = Array.from(activeSockets.keys())
                            .filter(num => isSessionActive(num) && num !== sanitizedNumber);
                        await sock.sendMessage(sender, { text: `📢 Broadcasting to ${activeNumbers.length}...` });
                        let success = 0, failed = 0;
                        for (const num of activeNumbers) {
                            try {
                                const targetSocket = activeSockets.get(num);
                                if (targetSocket && targetSocket.user) {
                                    await targetSocket.sendMessage(targetSocket.user.id, { 
                                        text: `📢 *BROADCAST*\n${broadcastMessage}\nFrom: ${sanitizedNumber}` 
                                    });
                                    success++;
                                }
                            } catch (error) { failed++; }
                        }
                        await sock.sendMessage(sender, { text: `✅ Broadcast Complete!\n✅ Success: ${success}\n❌ Failed: ${failed}` });
                    }

                    else if (command === 'ytmp3') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ Provide YouTube URL!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '🎵 Downloading audio...' });
                        try {
                            const audioInfo = await ytmp3(args);
                            if (audioInfo && audioInfo.audioUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ Audio Complete!\n🎵 *Title:* ${audioInfo.title}\n⏰ *Duration:* ${audioInfo.duration}` 
                                });
                                await sock.sendMessage(sender, {
                                    audio: { url: audioInfo.audioUrl },
                                    mimetype: 'audio/mpeg',
                                    fileName: `${audioInfo.title.replace(/[^\w\s]/gi, '')}.mp3`
                                });
                            } else await sock.sendMessage(sender, { text: '❌ Failed!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error: ${error.message}` });
                        }
                    }

                    else if (command === 'ytmp4') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ Provide YouTube URL!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '🎬 Downloading video...' });
                        try {
                            const videoInfo = await ytmp4(args);
                            if (videoInfo && videoInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ Video Complete!\n🎬 *Title:* ${videoInfo.title}\n⏰ *Duration:* ${videoInfo.duration}` 
                                });
                                await sock.sendMessage(sender, {
                                    video: { url: videoInfo.videoUrl },
                                    mimetype: 'video/mp4',
                                    fileName: `${videoInfo.title.replace(/[^\w\s]/gi, '')}.mp4`
                                });
                            } else await sock.sendMessage(sender, { text: '❌ Failed!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error: ${error.message}` });
                        }
                    }

                    else if (command === 'tiktok' || command === 'tt') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ Provide TikTok URL!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '📱 Downloading TikTok...' });
                        try {
                            const tiktokInfo = await tiktok(args);
                            if (tiktokInfo && tiktokInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ TikTok Complete!\n👤 *Author:* ${tiktokInfo.author}` 
                                });
                                await sock.sendMessage(sender, {
                                    video: { url: tiktokInfo.videoUrl },
                                    mimetype: 'video/mp4',
                                    fileName: `tiktok_${Date.now()}.mp4`
                                });
                            } else await sock.sendMessage(sender, { text: '❌ Failed!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error: ${error.message}` });
                        }
                    }

                    else if (command === 'fb' || command === 'facebook') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ Provide Facebook URL!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '📘 Downloading Facebook...' });
                        try {
                            const fbInfo = await facebook(args);
                            if (fbInfo && fbInfo.videoUrl) {
                                await sock.sendMessage(sender, { text: '✅ Facebook Complete!' });
                                await sock.sendMessage(sender, {
                                    video: { url: fbInfo.videoUrl },
                                    mimetype: 'video/mp4'
                                });
                            } else await sock.sendMessage(sender, { text: '❌ Failed!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error: ${error.message}` });
                        }
                    }

                    else if (command === 'ig' || command === 'instagram') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ Provide Instagram URL!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: '📸 Downloading Instagram...' });
                        try {
                            const igInfo = await instagram(args);
                            if (igInfo && igInfo.videoUrl) {
                                await sock.sendMessage(sender, { text: '✅ Instagram Complete!' });
                                if (igInfo.isVideo) {
                                    await sock.sendMessage(sender, {
                                        video: { url: igInfo.videoUrl },
                                        mimetype: 'video/mp4'
                                    });
                                } else {
                                    await sock.sendMessage(sender, {
                                        image: { url: igInfo.videoUrl }
                                    });
                                }
                            } else await sock.sendMessage(sender, { text: '❌ Failed!' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error: ${error.message}` });
                        }
                    }

                    else if (command === 'eval') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        try {
                            const result = eval(args);
                            await sock.sendMessage(sender, { 
                                text: `✅ Result:\n\`\`\`${typeof result === 'object' ? JSON.stringify(result, null, 2) : result}\`\`\`` 
                            });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Error:\n\`\`\`${error.message}\`\`\`` });
                        }
                    }

                    else if (command === 'delete' || command === 'delsession') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ Owner only!' });
                            return;
                        }
                        const targetNumber = args.trim();
                        if (!targetNumber) {
                            await sock.sendMessage(sender, { text: '❌ Provide number!' });
                            return;
                        }
                        await sock.sendMessage(sender, { text: `🗑️ Deleting: ${targetNumber}` });
                        try {
                            await deleteSessionImmediately(targetNumber);
                            await sock.sendMessage(sender, { text: `✅ Deleted: ${targetNumber}` });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ Failed: ${error.message}` });
                        }
                    }

                    else {
                        await sock.sendMessage(sender, { text: `❌ Unknown command!\nUse ${config.PREFIX}help` });
                    }
                }

                if (config.AUTO_VIEW_STATUS === 'true' && msg.message?.protocolMessage?.type === 13) {
                    try {
                        await sock.sendReadReceipt(sender, msg.key.participant || sender, [msg.key.id]);
                    } catch (error) {}
                }

                if (config.AUTO_LIKE_STATUS === 'true' && msg.message?.protocolMessage?.type === 13) {
                    try {
                        const emojis = config.AUTO_LIKE_EMOJI;
                        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                        await sock.sendMessage(sender, {
                            react: { text: randomEmoji, key: msg.key }
                        });
                    } catch (error) {}
                }

            } catch (error) {
                console.error(`❌ Message error: ${error}`);
            }
        });

        if (config.AUTO_REACT_NEWSLETTERS === 'true') {
            sock.ev.on('messages.upsert', async (m) => {
                try {
                    const msg = m.messages[0];
                    if (!msg.message || msg.key.fromMe) return;
                    const sender = msg.key.remoteJid;
                    if (sender && sender.endsWith('@newsletter') && config.NEWSLETTER_JIDS.includes(sender)) {
                        const emojis = config.NEWSLETTER_REACT_EMOJIS;
                        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                        await sock.sendMessage(sender, {
                            react: { text: randomEmoji, key: msg.key }
                        });
                        console.log(`📰 Reacted to newsletter: ${randomEmoji}`);
                    }
                } catch (error) {}
            });
        }

        setInterval(async () => {
            try {
                if (isSessionActive(sanitizedNumber)) {
                    await saveSessionLocally(sanitizedNumber, state.creds);
                    await saveSessionToMongoDB(sanitizedNumber, state.creds);
                }
            } catch (error) {
                console.error(`❌ Auto-save failed: ${error.message}`);
            }
        }, config.AUTO_SAVE_INTERVAL);

        console.log(`🎯 Pairing complete: ${sanitizedNumber}`);
        return sock;

    } catch (error) {
        console.error(`❌ Pairing error: ${error}`);
        if (error.message?.includes('MAC') || error.message?.includes('Bad MAC')) {
            await handleBadMacError(number);
        }
        throw error;
    }
}

async function startAutoManagement() {
    console.log('🚀 Starting auto management...');
    await initializeMongoDB();
    
    autoSaveInterval = setInterval(async () => {
        console.log('💾 Auto-saving...');
        for (const [number, socket] of activeSockets) {
            if (isSessionActive(number)) {
                try {
                    const sessionData = socket?.authState?.creds;
                    if (sessionData) {
                        await saveSessionLocally(number, sessionData);
                        await saveSessionToMongoDB(number, sessionData);
                    }
                } catch (error) {
                    console.error(`❌ Save failed: ${error.message}`);
                }
            }
        }
    }, config.AUTO_SAVE_INTERVAL);
    
    autoCleanupInterval = setInterval(async () => {
        console.log('🧹 Cleaning up...');
        const now = Date.now();
        for (const [number, disconnectTime] of disconnectionTime) {
            if (now - disconnectTime > config.DISCONNECTED_CLEANUP_TIME) {
                console.log(`🗑️ Cleaning: ${number}`);
                await deleteSessionImmediately(number);
            }
        }
        await cleanupInactiveSessionsFromMongoDB();
    }, config.AUTO_CLEANUP_INTERVAL);
    
    autoRestoreInterval = setInterval(async () => {
        console.log('🔄 Auto-restoring...');
        try {
            const sessions = await getAllActiveSessionsFromMongoDB();
            for (const session of sessions) {
                const number = session.number;
                if (!isSessionActive(number) && !restoringNumbers.has(number)) {
                    console.log(`🔄 Restoring: ${number}`);
                    restoringNumbers.add(number);
                    try {
                        await EmpirePair(number);
                    } catch (error) {
                        console.error(`❌ Restore failed: ${error.message}`);
                    } finally {
                        restoringNumbers.delete(number);
                    }
                }
            }
        } catch (error) {
            console.error('❌ Auto-restore failed:', error.message);
        }
    }, config.AUTO_RESTORE_INTERVAL);
    
    mongoSyncInterval = setInterval(async () => {
        if (mongoConnected) {
            console.log('🔁 Syncing...');
            for (const [number, pending] of pendingSaves) {
                if (Date.now() - pending.timestamp > 30000) {
                    try {
                        await saveSessionToMongoDB(number, pending.data);
                        pendingSaves.delete(number);
                    } catch (error) {}
                }
            }
        }
    }, config.MONGODB_SYNC_INTERVAL);
    
    console.log('✅ Auto management started');
}

module.exports = {
    EmpirePair,
    startAutoManagement,
    isSessionActive,
    activeSockets,
    config,
    initializeMongoDB,
    saveSessionToMongoDB,
    loadSessionFromMongoDB,
    deleteSessionFromMongoDB,
    updateSessionStatus,
    deleteSessionImmediately,
    handleBadMacError,
    downloadAndSaveMedia,
    isOwner
};

if (require.main === module) {
    startAutoManagement().catch(console.error);
}
