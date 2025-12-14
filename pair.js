const { ytmp3, tiktok, facebook, instagram, twitter, ytmp4 } = require('sadaslk-dlcore');
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const router = express.Router();
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

// FIX: Add makeInMemoryStore function since it's not exported in newer versions
function makeInMemoryStore() {
    const store = {
        chats: new Map(),
        contacts: new Map(),
        messages: new Map(),
        groupMetadata: new Map(),
        state: { connection: 'close' },
        
        // Basic methods
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
        
        writeToFile: (path) => {
            const fs = require('fs-extra');
            fs.writeFileSync(path, JSON.stringify(store.toJSON(), null, 2));
        },
        
        readFromFile: (path) => {
            const fs = require('fs-extra');
            if (fs.existsSync(path)) {
                const json = JSON.parse(fs.readFileSync(path, 'utf8'));
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
    // General Bot Settings
    AUTO_VIEW_STATUS: 'true',
    AUTO_LIKE_STATUS: 'true',
    AUTO_RECORDING: 'true',
    AUTO_LIKE_EMOJI: ['💗', '🩵', '🥺', '🫶', '😶'],

    // Newsletter Auto-React Settings
    AUTO_REACT_NEWSLETTERS: 'true',
    NEWSLETTER_JIDS: ['120363299029326322@newsletter','120363401297349965@newsletter','120363339980514201@newsletter','120363420947784745@newsletter','120363296314610373@newsletter'],
    NEWSLETTER_REACT_EMOJIS: ['🐥', '🤭', '♥️', '🙂', '☺️', '🩵', '🫶'],
    
    // OPTIMIZED Auto Session Management for Heroku Dynos
    AUTO_SAVE_INTERVAL: 300000,        // Auto-save every 5 minutes
    AUTO_CLEANUP_INTERVAL: 900000,     // Cleanup every 15 minutes
    AUTO_RECONNECT_INTERVAL: 300000,   // Reconnect every 5 minutes
    AUTO_RESTORE_INTERVAL: 1800000,    // Auto-restore every 30 minutes
    MONGODB_SYNC_INTERVAL: 600000,     // Sync with MongoDB every 10 minutes
    MAX_SESSION_AGE: 604800000,        // 7 days in milliseconds
    DISCONNECTED_CLEANUP_TIME: 300000, // 5 minutes cleanup for disconnected sessions
    MAX_FAILED_ATTEMPTS: 3,            // Allow 3 failed attempts before giving up
    INITIAL_RESTORE_DELAY: 10000,      // Wait 10 seconds before first restore
    IMMEDIATE_DELETE_DELAY: 60000,     // Delete invalid sessions after 1 minute

    // Command Settings
    PREFIX: '.',
    MAX_RETRIES: 3,

    // Group & Channel Settings
    GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JXaWiMrpjWyJ6Kd2G9FAAq?mode=ems_copy_t',
    NEWSLETTER_JID: '120363299029326322@newsletter',
    NEWSLETTER_MESSAGE_ID: '291',
    CHANNEL_LINK: 'https://whatsapp.com/channel/0029Vb6V5Xl6LwHgkapiAI0V',

    // File Paths
    ADMIN_LIST_PATH: './admin.json',
    IMAGE_PATH: 'https://i.ibb.co/zhm2RF8j/vision-v.jpg',
    NUMBER_LIST_PATH: './numbers.json',
    SESSION_STATUS_PATH: './session_status.json',
    SESSION_BASE_PATH: './session',

    // Security & OTP
    OTP_EXPIRY: 300000,

    // News Feed
    NEWS_JSON_URL: 'https://raw.githubusercontent.com/boychalana9-max/mage/refs/heads/main/main.json?token=GHSAT0AAAAAADJU6UDFFZ67CUOLUQAAWL322F3RI2Q',

    // Owner Details
    OWNER_NUMBER: '254740007567',
    TRANSFER_OWNER_NUMBER: '254740007567',
};

// Session Management Maps
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

// Auto-management intervals
let autoSaveInterval;
let autoCleanupInterval;
let autoReconnectInterval;
let autoRestoreInterval;
let mongoSyncInterval;

// MongoDB Connection
let mongoConnected = false;

// MongoDB Schemas
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

// Initialize MongoDB Connection
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

        await Session.createIndexes().catch(err => console.error('Index creation error:', err));
        await UserConfig.createIndexes().catch(err => console.error('Index creation error:', err));

        return true;
    } catch (error) {
        console.error('❌ MongoDB connection error:', error.message);
        mongoConnected = false;

        setTimeout(() => {
            initializeMongoDB();
        }, 5000);

        return false;
    }
}

// MongoDB Session Management Functions
async function saveSessionToMongoDB(number, sessionData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        if (!isSessionActive(sanitizedNumber)) {
            console.log(`⏭️ Not saving inactive session to MongoDB: ${sanitizedNumber}`);
            return false;
        }

        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid session data, not saving to MongoDB: ${sanitizedNumber}`);
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
        console.error(`❌ MongoDB save failed for ${number}:`, error.message);
        pendingSaves.set(number, {
            data: sessionData,
            timestamp: Date.now()
        });
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
            console.log(`✅ Session loaded from MongoDB: ${sanitizedNumber}`);
            return session.sessionData;
        }

        return null;
    } catch (error) {
        console.error(`❌ MongoDB load failed for ${number}:`, error.message);
        return null;
    }
}

async function deleteSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        await Session.deleteOne({ number: sanitizedNumber });
        await UserConfig.deleteOne({ number: sanitizedNumber });

        console.log(`🗑️ Session deleted from MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB delete failed for ${number}:`, error.message);
        return false;
    }
}

async function getAllActiveSessionsFromMongoDB() {
    try {
        const sessions = await Session.find({ 
            status: 'active',
            health: { $ne: 'invalid' }
        });

        console.log(`📊 Found ${sessions.length} active sessions in MongoDB`);
        return sessions;
    } catch (error) {
        console.error('❌ Failed to get sessions from MongoDB:', error.message);
        return [];
    }
}

async function updateSessionStatusInMongoDB(number, status, health = null) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const updateData = {
            status: status,
            updatedAt: new Date()
        };

        if (health) {
            updateData.health = health;
        }

        if (status === 'active') {
            updateData.lastActive = new Date();
        }

        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            updateData,
            { upsert: false }
        );

        console.log(`📝 Session status updated in MongoDB: ${sanitizedNumber} -> ${status}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB status update failed for ${number}:`, error.message);
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

        console.log(`🧹 Cleaned ${result.deletedCount} inactive sessions from MongoDB`);
        return result.deletedCount;
    } catch (error) {
        console.error('❌ MongoDB cleanup failed:', error.message);
        return 0;
    }
}

async function getMongoSessionCount() {
    try {
        const count = await Session.countDocuments({ status: 'active' });
        return count;
    } catch (error) {
        console.error('❌ Failed to count MongoDB sessions:', error.message);
        return 0;
    }
}

// User Config MongoDB Functions
async function saveUserConfigToMongoDB(number, configData) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        await UserConfig.findOneAndUpdate(
            { number: sanitizedNumber },
            {
                config: configData,
                updatedAt: new Date()
            },
            { upsert: true, new: true }
        );

        console.log(`✅ User config saved to MongoDB: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ MongoDB config save failed for ${number}:`, error.message);
        return false;
    }
}

async function loadUserConfigFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const userConfig = await UserConfig.findOne({ number: sanitizedNumber });

        if (userConfig) {
            console.log(`✅ User config loaded from MongoDB: ${sanitizedNumber}`);
            return userConfig.config;
        }

        return null;
    } catch (error) {
        console.error(`❌ MongoDB config load failed for ${number}:`, error.message);
        return null;
    }
}

// Create necessary directories
function initializeDirectories() {
    const dirs = [
        config.SESSION_BASE_PATH,
        './temp'
    ];

    dirs.forEach(dir => {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`📁 Created directory: ${dir}`);
        }
    });
}

initializeDirectories();

// **HELPER FUNCTIONS**
async function validateSessionData(sessionData) {
    try {
        if (!sessionData || typeof sessionData !== 'object') {
            return false;
        }

        if (!sessionData.me || !sessionData.myAppStateKeyId) {
            return false;
        }

        const requiredFields = ['noiseKey', 'signedIdentityKey', 'signedPreKey', 'registrationId'];
        for (const field of requiredFields) {
            if (!sessionData[field]) {
                console.warn(`⚠️ Missing required field: ${field}`);
                return false;
            }
        }

        return true;
    } catch (error) {
        console.error('❌ Session validation error:', error);
        return false;
    }
}

async function handleBadMacError(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    console.log(`🔧 Handling Bad MAC error for ${sanitizedNumber}`);

    try {
        if (activeSockets.has(sanitizedNumber)) {
            const socket = activeSockets.get(sanitizedNumber);
            try {
                if (socket?.ws) {
                    socket.ws.close();
                } else if (socket?.end) {
                    socket.end();
                } else if (socket?.logout) {
                    await socket.logout();
                }
            } catch (e) {
                console.error('Error closing socket:', e.message);
            }
            activeSockets.delete(sanitizedNumber);
        }

        if (stores.has(sanitizedNumber)) {
            stores.delete(sanitizedNumber);
        }

        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            console.log(`🗑️ Removing corrupted session files for ${sanitizedNumber}`);
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

        await updateSessionStatus(sanitizedNumber, 'bad_mac_cleared', new Date().toISOString());

        console.log(`✅ Cleared Bad MAC session for ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to handle Bad MAC for ${sanitizedNumber}:`, error);
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

// **SESSION MANAGEMENT**
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
            console.log(`⏭️ Skipping local save for inactive session: ${sanitizedNumber}`);
            return false;
        }

        if (!validateSessionData(sessionData)) {
            console.warn(`⚠️ Invalid session data, not saving locally: ${sanitizedNumber}`);
            return false;
        }

        const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        await fs.ensureDir(sessionPath);

        await fs.writeFile(
            path.join(sessionPath, 'creds.json'),
            JSON.stringify(sessionData, null, 2)
        );

        console.log(`💾 Active session saved locally: ${sanitizedNumber}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to save session locally for ${number}:`, error);
        return false;
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');

        const sessionData = await loadSessionFromMongoDB(sanitizedNumber);

        if (sessionData) {
            if (!validateSessionData(sessionData)) {
                console.warn(`⚠️ Invalid session data for ${sanitizedNumber}, clearing...`);
                await handleBadMacError(sanitizedNumber);
                return null;
            }

            await saveSessionLocally(sanitizedNumber, sessionData);
            console.log(`✅ Restored valid session from MongoDB: ${sanitizedNumber}`);
            return sessionData;
        }

        return null;
    } catch (error) {
        console.error(`❌ Session restore failed for ${number}:`, error.message);

        if (error.message?.includes('MAC') || error.message?.includes('decrypt')) {
            await handleBadMacError(number);
        }

        return null;
    }
}

async function deleteSessionImmediately(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    console.log(`🗑️ Immediately deleting inactive/invalid session: ${sanitizedNumber}`);

    if (activeSockets.has(sanitizedNumber)) {
        const socket = activeSockets.get(sanitizedNumber);
        try {
            if (socket?.ws) {
                socket.ws.close();
            } else if (socket?.end) {
                socket.end();
            } else if (socket?.logout) {
                await socket.logout();
            }
        } catch (e) {
            console.error('Error closing socket:', e.message);
        }
    }

    const sessionPath = path.join(config.SESSION_BASE_PATH, `session_${sanitizedNumber}`);
    if (fs.existsSync(sessionPath)) {
        await fs.remove(sessionPath);
    }

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

    console.log(`✅ Session completely deleted: ${sanitizedNumber}`);
    return true;
}

async function updateSessionStatus(number, status, health = null) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    if (health) {
        sessionHealth.set(sanitizedNumber, health);
    }

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
    console.log(`📝 Session status updated: ${sanitizedNumber} -> ${status}`);
}

// **COMPLETE EMPIRE PAIR FUNCTION WITH ALL COMMANDS**
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

        // Save credentials periodically
        sock.ev.on('creds.update', async () => {
            try {
                if (isSessionActive(sanitizedNumber)) {
                    await saveCreds();
                    await saveSessionLocally(sanitizedNumber, state.creds);
                    await saveSessionToMongoDB(sanitizedNumber, state.creds);
                    lastBackupTime.set(sanitizedNumber, Date.now());
                }
            } catch (error) {
                console.error(`❌ Creds update failed for ${sanitizedNumber}:`, error.message);
            }
        });

        // Handle connection updates
        sock.ev.on('connection.update', async (update) => {
            try {
                const { connection, lastDisconnect, qr } = update;

                if (qr) {
                    console.log(`📱 QR Code generated for ${sanitizedNumber}`);
                }

                if (connection === 'open') {
                    console.log(`✅ Connected: ${sanitizedNumber}`);
                    sessionConnectionStatus.set(sanitizedNumber, 'open');
                    sessionHealth.set(sanitizedNumber, 'active');
                    await updateSessionStatus(sanitizedNumber, 'active', 'active');
                    
                    disconnectionTime.delete(sanitizedNumber);
                    reconnectionAttempts.set(sanitizedNumber, 0);
                    
                    // Send welcome message
                    const welcomeMessage = `✅ *BOT CONNECTED*\n\n📱 *Number:* ${sanitizedNumber}\n⏰ *Time:* ${moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss')}\n🔧 *Status:* Active\n\n*Use commands with prefix:* ${config.PREFIX}`;
                    await sock.sendMessage(sock.user.id, { text: welcomeMessage });
                }

                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    console.log(`❌ Disconnected: ${sanitizedNumber}`, statusCode || lastDisconnect?.error);

                    sessionConnectionStatus.set(sanitizedNumber, 'close');
                    disconnectionTime.set(sanitizedNumber, Date.now());

                    if (statusCode === DisconnectReason.badSession) {
                        console.log(`⚠️ Bad session detected for ${sanitizedNumber}, clearing...`);
                        await handleBadMacError(sanitizedNumber);
                        return;
                    }

                    if (statusCode === DisconnectReason.connectionLost || 
                        statusCode === DisconnectReason.connectionClosed) {
                        const attempts = reconnectionAttempts.get(sanitizedNumber) || 0;
                        if (attempts < config.MAX_FAILED_ATTEMPTS) {
                            reconnectionAttempts.set(sanitizedNumber, attempts + 1);
                            console.log(`🔄 Reconnection attempt ${attempts + 1} for ${sanitizedNumber}`);
                            setTimeout(() => {
                                EmpirePair(sanitizedNumber).catch(console.error);
                            }, 5000);
                        } else {
                            console.log(`❌ Max reconnection attempts reached for ${sanitizedNumber}`);
                            await updateSessionStatus(sanitizedNumber, 'disconnected', 'failed');
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ Connection update error for ${sanitizedNumber}:`, error);
            }
        });

        // **BOT COMMANDS HANDLER**
        sock.ev.on('messages.upsert', async (m) => {
            try {
                const msg = m.messages[0];
                if (!msg.message || msg.key.fromMe) return;

                const sender = msg.key.remoteJid;
                const messageText = msg.message.conversation || 
                                   msg.message.extendedTextMessage?.text || 
                                   msg.message.imageMessage?.caption || '';
                
                const isGroup = sender.endsWith('@g.us');
                const isOwnerMsg = isOwner(sender);

                // Check if message starts with prefix
                if (messageText.startsWith(config.PREFIX)) {
                    const command = messageText.slice(config.PREFIX.length).trim().split(' ')[0].toLowerCase();
                    const args = messageText.slice(config.PREFIX.length + command.length).trim();
                    
                    console.log(`📨 Command from ${sender}: ${command} ${args}`);

                    // **HELP COMMAND**
                    if (command === 'help' || command === 'menu') {
                        const helpMenu = `╭─━━━━━━━━━━━━━━━━━━━─╮
│   *BOT COMMANDS MENU*   │
╰─━━━━━━━━━━━━━━━━━━━─╯

╭─━━━━━━━━━━━━━━━━━━━─╮
│ *📱 BASIC COMMANDS*      │
├─────────────────────┤
│ ${config.PREFIX}help - Show this menu
│ ${config.PREFIX}ping - Check bot speed
│ ${config.PREFIX}owner - Contact owner
│ ${config.PREFIX}status - Bot status
╰─━━━━━━━━━━━━━━━━━━━─╯

╭─━━━━━━━━━━━━━━━━━━━─╮
│ *🔧 SESSION COMMANDS*   │
├─────────────────────┤
│ ${config.PREFIX}pair - Pair new session
│ ${config.PREFIX}list - List active sessions
│ ${config.PREFIX}restart - Restart session
│ ${config.PREFIX}logout - Logout session
╰─━━━━━━━━━━━━━━━━━━━─╯

╭─━━━━━━━━━━━━━━━━━━━─╮
│ *🎵 MEDIA COMMANDS*     │
├─────────────────────┤
│ ${config.PREFIX}ytmp3 [url] - Download YT audio
│ ${config.PREFIX}ytmp4 [url] - Download YT video
│ ${config.PREFIX}tiktok [url] - TikTok download
│ ${config.PREFIX}fb [url] - Facebook download
│ ${config.PREFIX}ig [url] - Instagram download
╰─━━━━━━━━━━━━━━━━━━━─╯

╭─━━━━━━━━━━━━━━━━━━━─╮
│ *👑 OWNER COMMANDS*     │
├─────────────────────┤
│ ${config.PREFIX}bc [text] - Broadcast
│ ${config.PREFIX}eval [code] - Execute code
│ ${config.PREFIX}exec [cmd] - Execute shell
│ ${config.PREFIX}delete [num] - Delete session
╰─━━━━━━━━━━━━━━━━━━━─╯

╭─━━━━━━━━━━━━━━━━━━━─╮
│ *🔗 JOIN OUR GROUP*     │
├─────────────────────┤
│ ${config.GROUP_INVITE_LINK}
╰─━━━━━━━━━━━━━━━━━━━─╯

*Owner:* ${config.OWNER_NUMBER}
*Prefix:* ${config.PREFIX}`;

                        await sock.sendMessage(sender, { text: helpMenu });
                    }

                    // **PING COMMAND**
                    else if (command === 'ping') {
                        const start = Date.now();
                        const reply = await sock.sendMessage(sender, { text: '🏓 Pinging...' });
                        const latency = Date.now() - start;
                        
                        const status = {
                            latency: `${latency}ms`,
                            number: sanitizedNumber,
                            status: isSessionActive(sanitizedNumber) ? '✅ Active' : '❌ Inactive',
                            uptime: socketCreationTime.has(sanitizedNumber) ? 
                                Math.floor((Date.now() - socketCreationTime.get(sanitizedNumber)) / 1000) + 's' : 'N/A'
                        };
                        
                        await sock.sendMessage(sender, { 
                            text: `🏓 *PONG!*\n\n📱 *Bot Number:* ${status.number}\n⚡ *Latency:* ${status.latency}\n🔧 *Status:* ${status.status}\n⏰ *Uptime:* ${status.uptime}` 
                        });
                    }

                    // **OWNER COMMAND**
                    else if (command === 'owner' || command === 'dev') {
                        await sock.sendMessage(sender, { 
                            text: `👑 *OWNER INFORMATION*\n\n📱 *Number:* ${config.OWNER_NUMBER}\n💬 *Contact:* wa.me/${config.OWNER_NUMBER}\n\n*Need help? Contact the owner directly!*` 
                        });
                    }

                    // **STATUS COMMAND**
                    else if (command === 'status' || command === 'info') {
                        const activeSessionsCount = Array.from(activeSockets.keys())
                            .filter(num => isSessionActive(num)).length;
                        
                        const statusMessage = `🤖 *BOT STATUS*\n\n📱 *Bot Number:* ${sanitizedNumber}\n🔧 *Status:* ${isSessionActive(sanitizedNumber) ? '✅ Active' : '❌ Inactive'}\n👥 *Active Sessions:* ${activeSessionsCount}\n⏰ *Time:* ${moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss')}\n\n*MongoDB:* ${mongoConnected ? '✅ Connected' : '❌ Disconnected'}\n*Storage:* ${await getMongoSessionCount()} sessions saved`;
                        
                        await sock.sendMessage(sender, { text: statusMessage });
                    }

                    // **PAIR COMMAND**
                    else if (command === 'pair') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        const targetNumber = args.trim();
                        if (!targetNumber) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide a number!*\nExample: .pair 254712345678' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: `🔗 *Pairing new session...*\n\n📱 *Number:* ${targetNumber}\n⏳ Please wait...` });
                        
                        try {
                            await EmpirePair(targetNumber);
                            await sock.sendMessage(sender, { 
                                text: `✅ *Pairing initiated!*\n\n📱 *Number:* ${targetNumber}\n📱 Scan the QR code to connect.\n\n*Check the console/logs for QR code.*` 
                            });
                        } catch (error) {
                            await sock.sendMessage(sender, { 
                                text: `❌ *Pairing failed!*\n\n📱 *Number:* ${targetNumber}\n🔧 *Error:* ${error.message}` 
                            });
                        }
                    }

                    // **LIST COMMANDS**
                    else if (command === 'list' || command === 'sessions') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        const activeNumbers = Array.from(activeSockets.keys())
                            .filter(num => isSessionActive(num));
                        
                        let listMessage = `📋 *ACTIVE SESSIONS*\n\n*Total:* ${activeNumbers.length}\n\n`;
                        
                        if (activeNumbers.length > 0) {
                            activeNumbers.forEach((num, index) => {
                                const creationTime = socketCreationTime.get(num);
                                const uptime = creationTime ? 
                                    Math.floor((Date.now() - creationTime) / 1000) + 's' : 'N/A';
                                listMessage += `${index + 1}. *${num}* (${uptime})\n`;
                            });
                        } else {
                            listMessage += '*No active sessions*';
                        }
                        
                        listMessage += `\n\n*MongoDB Sessions:* ${await getMongoSessionCount()}`;
                        await sock.sendMessage(sender, { text: listMessage });
                    }

                    // **RESTART COMMAND**
                    else if (command === 'restart') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '🔄 *Restarting session...*' });
                        
                        if (sock.ws) sock.ws.close();
                        
                        setTimeout(async () => {
                            try {
                                await EmpirePair(sanitizedNumber);
                                await sock.sendMessage(sender, { text: '✅ *Session restarted successfully!*' });
                            } catch (error) {
                                await sock.sendMessage(sender, { text: `❌ *Restart failed:* ${error.message}` });
                            }
                        }, 3000);
                    }

                    // **LOGOUT COMMAND**
                    else if (command === 'logout' || command === 'stop') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '🚪 *Logging out session...*' });
                        
                        try {
                            await deleteSessionImmediately(sanitizedNumber);
                            await sock.sendMessage(sender, { text: '✅ *Session logged out successfully!*' });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Logout failed:* ${error.message}` });
                        }
                    }

                    // **BROADCAST COMMAND**
                    else if (command === 'bc' || command === 'broadcast') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        const broadcastMessage = args;
                        if (!broadcastMessage) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide message!*\nExample: .bc Hello everyone!' });
                            return;
                        }
                        
                        const activeNumbers = Array.from(activeSockets.keys())
                            .filter(num => isSessionActive(num) && num !== sanitizedNumber);
                        
                        await sock.sendMessage(sender, { 
                            text: `📢 *Broadcasting to ${activeNumbers.length} sessions...*` 
                        });
                        
                        let success = 0;
                        let failed = 0;
                        
                        for (const num of activeNumbers) {
                            try {
                                const targetSocket = activeSockets.get(num);
                                if (targetSocket && targetSocket.user) {
                                    await targetSocket.sendMessage(targetSocket.user.id, { 
                                        text: `📢 *BROADCAST MESSAGE*\n\n${broadcastMessage}\n\n*From:* ${sanitizedNumber}` 
                                    });
                                    success++;
                                }
                            } catch (error) {
                                failed++;
                            }
                        }
                        
                        await sock.sendMessage(sender, { 
                            text: `✅ *Broadcast Complete!*\n\n✅ Success: ${success}\n❌ Failed: ${failed}\n📱 Total: ${activeNumbers.length}` 
                        });
                    }

                    // **YTMP3 COMMAND**
                    else if (command === 'ytmp3') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide YouTube URL!*\nExample: .ytmp3 https://youtube.com/watch?v=...' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '🎵 *Downloading YouTube audio...*\n\n⏳ Please wait...' });
                        
                        try {
                            const audioInfo = await ytmp3(args);
                            
                            if (audioInfo && audioInfo.audioUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ *Audio Download Complete!*\n\n🎵 *Title:* ${audioInfo.title}\n⏰ *Duration:* ${audioInfo.duration}\n📁 *Size:* ${audioInfo.size || 'N/A'}` 
                                });
                                
                                await sock.sendMessage(sender, {
                                    audio: { url: audioInfo.audioUrl },
                                    mimetype: 'audio/mpeg',
                                    fileName: `${audioInfo.title.replace(/[^\w\s]/gi, '')}.mp3`
                                });
                            } else {
                                await sock.sendMessage(sender, { text: '❌ *Failed to download audio!*' });
                            }
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Error:* ${error.message}` });
                        }
                    }

                    // **YTMP4 COMMAND**
                    else if (command === 'ytmp4') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide YouTube URL!*\nExample: .ytmp4 https://youtube.com/watch?v=...' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '🎬 *Downloading YouTube video...*\n\n⏳ Please wait...' });
                        
                        try {
                            const videoInfo = await ytmp4(args);
                            
                            if (videoInfo && videoInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ *Video Download Complete!*\n\n🎬 *Title:* ${videoInfo.title}\n⏰ *Duration:* ${videoInfo.duration}\n📁 *Size:* ${videoInfo.size || 'N/A'}` 
                                });
                                
                                await sock.sendMessage(sender, {
                                    video: { url: videoInfo.videoUrl },
                                    mimetype: 'video/mp4',
                                    caption: videoInfo.title,
                                    fileName: `${videoInfo.title.replace(/[^\w\s]/gi, '')}.mp4`
                                });
                            } else {
                                await sock.sendMessage(sender, { text: '❌ *Failed to download video!*' });
                            }
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Error:* ${error.message}` });
                        }
                    }

                    // **TIKTOK COMMAND**
                    else if (command === 'tiktok' || command === 'tt') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide TikTok URL!*\nExample: .tiktok https://tiktok.com/@...' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '📱 *Downloading TikTok video...*\n\n⏳ Please wait...' });
                        
                        try {
                            const tiktokInfo = await tiktok(args);
                            
                            if (tiktokInfo && tiktokInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ *TikTok Download Complete!*\n\n👤 *Author:* ${tiktokInfo.author}\n❤️ *Likes:* ${tiktokInfo.likes || 'N/A'}\n💬 *Comments:* ${tiktokInfo.comments || 'N/A'}` 
                                });
                                
                                await sock.sendMessage(sender, {
                                    video: { url: tiktokInfo.videoUrl },
                                    mimetype: 'video/mp4',
                                    caption: tiktokInfo.description || 'TikTok Video',
                                    fileName: `tiktok_${Date.now()}.mp4`
                                });
                            } else {
                                await sock.sendMessage(sender, { text: '❌ *Failed to download TikTok video!*' });
                            }
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Error:* ${error.message}` });
                        }
                    }

                    // **FACEBOOK COMMAND**
                    else if (command === 'fb' || command === 'facebook') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide Facebook URL!*\nExample: .fb https://facebook.com/...' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '📘 *Downloading Facebook video...*\n\n⏳ Please wait...' });
                        
                        try {
                            const fbInfo = await facebook(args);
                            
                            if (fbInfo && fbInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ *Facebook Download Complete!*\n\n📘 *Title:* ${fbInfo.title || 'Facebook Video'}\n⏰ *Duration:* ${fbInfo.duration || 'N/A'}` 
                                });
                                
                                await sock.sendMessage(sender, {
                                    video: { url: fbInfo.videoUrl },
                                    mimetype: 'video/mp4',
                                    fileName: `facebook_${Date.now()}.mp4`
                                });
                            } else {
                                await sock.sendMessage(sender, { text: '❌ *Failed to download Facebook video!*' });
                            }
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Error:* ${error.message}` });
                        }
                    }

                    // **INSTAGRAM COMMAND**
                    else if (command === 'ig' || command === 'instagram') {
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide Instagram URL!*\nExample: .ig https://instagram.com/p/...' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: '📸 *Downloading Instagram media...*\n\n⏳ Please wait...' });
                        
                        try {
                            const igInfo = await instagram(args);
                            
                            if (igInfo && igInfo.videoUrl) {
                                await sock.sendMessage(sender, { 
                                    text: `✅ *Instagram Download Complete!*\n\n📸 *Username:* ${igInfo.username || 'N/A'}\n💬 *Caption:* ${igInfo.caption || 'No caption'}` 
                                });
                                
                                if (igInfo.isVideo) {
                                    await sock.sendMessage(sender, {
                                        video: { url: igInfo.videoUrl },
                                        mimetype: 'video/mp4',
                                        fileName: `instagram_${Date.now()}.mp4`
                                    });
                                } else {
                                    await sock.sendMessage(sender, {
                                        image: { url: igInfo.videoUrl },
                                        caption: igInfo.caption || 'Instagram Photo',
                                        fileName: `instagram_${Date.now()}.jpg`
                                    });
                                }
                            } else {
                                await sock.sendMessage(sender, { text: '❌ *Failed to download Instagram media!*' });
                            }
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Error:* ${error.message}` });
                        }
                    }

                    // **EVAL COMMAND**
                    else if (command === 'eval') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        try {
                            const result = eval(args);
                            await sock.sendMessage(sender, { 
                                text: `✅ *Eval Result:*\n\n\`\`\`${typeof result === 'object' ? JSON.stringify(result, null, 2) : result}\`\`\`` 
                            });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Eval Error:*\n\n\`\`\`${error.message}\`\`\`` });
                        }
                    }

                    // **EXEC COMMAND**
                    else if (command === 'exec' || command === 'shell') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        if (!args) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide command!*\nExample: .exec ls -la' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: `💻 *Executing:* \`${args}\`\n\n⏳ Please wait...` });
                        
                        exec(args, async (error, stdout, stderr) => {
                            if (error) {
                                await sock.sendMessage(sender, { text: `❌ *Execution Error:*\n\n\`\`\`${error.message}\`\`\`` });
                                return;
                            }
                            
                            const output = stdout || stderr;
                            const truncatedOutput = output.length > 1900 ? output.substring(0, 1900) + '...' : output;
                            
                            await sock.sendMessage(sender, { text: `✅ *Execution Result:*\n\n\`\`\`${truncatedOutput}\`\`\`` });
                        });
                    }

                    // **DELETE COMMAND**
                    else if (command === 'delete' || command === 'delsession') {
                        if (!isOwnerMsg) {
                            await sock.sendMessage(sender, { text: '❌ *Owner only command!*' });
                            return;
                        }
                        
                        const targetNumber = args.trim();
                        if (!targetNumber) {
                            await sock.sendMessage(sender, { text: '❌ *Please provide a number!*\nExample: .delete 254712345678' });
                            return;
                        }
                        
                        await sock.sendMessage(sender, { text: `🗑️ *Deleting session...*\n\n📱 *Number:* ${targetNumber}` });
                        
                        try {
                            await deleteSessionImmediately(targetNumber);
                            await sock.sendMessage(sender, { text: `✅ *Session deleted successfully!*\n\n📱 *Number:* ${targetNumber}` });
                        } catch (error) {
                            await sock.sendMessage(sender, { text: `❌ *Delete failed:* ${error.message}` });
                        }
                    }

                    // **UNKNOWN COMMAND**
                    else {
                        await sock.sendMessage(sender, { 
                            text: `❌ *Unknown command!*\n\nType *${config.PREFIX}help* to see available commands.` 
                        });
                    }
                }

                // **AUTO-REACT TO STATUS UPDATES**
                if (config.AUTO_VIEW_STATUS === 'true' && msg.message?.protocolMessage?.type === 13) {
                    try {
                        await sock.sendReadReceipt(sender, msg.key.participant || sender, [msg.key.id]);
                    } catch (error) {}
                }

                // **AUTO-LIKE STATUS UPDATES**
                if (config.AUTO_LIKE_STATUS === 'true' && msg.message?.protocolMessage?.type === 13) {
                    try {
                        const emojis = config.AUTO_LIKE_EMOJI;
                        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                        await sock.sendMessage(sender, {
                            react: {
                                text: randomEmoji,
                                key: msg.key
                            }
                        });
                    } catch (error) {}
                }

            } catch (error) {
                console.error(`❌ Message handling error for ${sanitizedNumber}:`, error);
            }
        });

        // **AUTO-REACT TO NEWSLETTERS**
        if (config.AUTO_REACT_NEWSLETTERS === 'true') {
            sock.ev.on('messages.upsert', async (m) => {
                try {
                    const msg = m.messages[0];
                    if (!msg.message || msg.key.fromMe) return;

                    const sender = msg.key.remoteJid;
                    
                    if (sender && sender.endsWith('@newsletter')) {
                        const isTargetNewsletter = config.NEWSLETTER_JIDS.includes(sender);
                        
                        if (isTargetNewsletter) {
                            const emojis = config.NEWSLETTER_REACT_EMOJIS;
                            const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                            
                            await sock.sendMessage(sender, {
                                react: {
                                    text: randomEmoji,
                                    key: msg.key
                                }
                            });
                            
                            console.log(`📰 Auto-reacted to newsletter: ${sender} with ${randomEmoji}`);
                        }
                    }
                } catch (error) {}
            });
        }

        // Auto-save session periodically
        setInterval(async () => {
            try {
                if (isSessionActive(sanitizedNumber)) {
                    await saveSessionLocally(sanitizedNumber, state.creds);
                    await saveSessionToMongoDB(sanitizedNumber, state.creds);
                }
            } catch (error) {
                console.error(`❌ Auto-save failed for ${sanitizedNumber}:`, error.message);
            }
        }, config.AUTO_SAVE_INTERVAL);

        console.log(`🎯 Pairing setup complete for ${sanitizedNumber}`);
        return sock;

    } catch (error) {
        console.error(`❌ Pairing error for ${number}:`, error);
        
        if (error.message?.includes('MAC') || error.message?.includes('Bad MAC')) {
            await handleBadMacError(number);
        }
        
        throw error;
    }
}

// **MAIN FUNCTIONS**
async function startAutoManagement() {
    console.log('🚀 Starting auto session management...');
    
    await initializeMongoDB();
    
    autoSaveInterval = setInterval(async () => {
        console.log('💾 Auto-saving active sessions...');
        for (const [number, socket] of activeSockets) {
            if (isSessionActive(number)) {
                try {
                    const sessionData = socket?.authState?.creds;
                    if (sessionData) {
                        await saveSessionLocally(number, sessionData);
                        await saveSessionToMongoDB(number, sessionData);
                    }
                } catch (error) {
                    console.error(`❌ Auto-save failed for ${number}:`, error.message);
                }
            }
        }
    }, config.AUTO_SAVE_INTERVAL);
    
    autoCleanupInterval = setInterval(async () => {
        console.log('🧹 Cleaning up inactive sessions...');
        const now = Date.now();
        
        for (const [number, disconnectTime] of disconnectionTime) {
            if (now - disconnectTime > config.DISCONNECTED_CLEANUP_TIME) {
                console.log(`🗑️ Cleaning up disconnected session: ${number}`);
                await deleteSessionImmediately(number);
            }
        }
        
        await cleanupInactiveSessionsFromMongoDB();
    }, config.AUTO_CLEANUP_INTERVAL);
    
    autoRestoreInterval = setInterval(async () => {
        console.log('🔄 Auto-restoring sessions from MongoDB...');
        try {
            const sessions = await getAllActiveSessionsFromMongoDB();
            for (const session of sessions) {
                const number = session.number;
                if (!isSessionActive(number) && !restoringNumbers.has(number)) {
                    console.log(`🔄 Restoring session: ${number}`);
                    restoringNumbers.add(number);
                    try {
                        await EmpirePair(number);
                    } catch (error) {
                        console.error(`❌ Failed to restore ${number}:`, error.message);
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
            console.log('🔁 Syncing with MongoDB...');
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
    
    console.log('✅ Auto session management started');
}

// Export functions
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

// Start auto management if this is the main module
if (require.main === module) {
    startAutoManagement().catch(console.error);
}
