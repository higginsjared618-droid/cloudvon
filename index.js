const express = require('express');
const app = express();
__path = process.cwd()
const bodyParser = require("body-parser");
const PORT = process.env.PORT || 8000;
const { EmpirePair, startAutoManagement } = require('./pair');

require('events').EventEmitter.defaultMaxListeners = 500;

// FIXED: Create proper routes instead of trying to use the object as middleware
app.get('/pair', async (req, res, next) => {
    res.sendFile(__path + '/pair.html');
});

app.get('/pair/:number', async (req, res) => {
    try {
        const { number } = req.params;
        if (!number) {
            return res.status(400).json({ error: 'Number required' });
        }
        
        await EmpirePair(number);
        res.json({ 
            success: true, 
            message: `Pairing initiated for ${number}. Check console/logs for QR code.` 
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/', async (req, res, next) => {
    res.sendFile(__path + '/main.html');
});

app.get('/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Static files (if you have any)
app.use(express.static('public'));

// Start bot management when server starts
startAutoManagement().then(() => {
    console.log('✅ WhatsApp Bot auto-management started');
}).catch(err => {
    console.error('❌ Failed to start bot:', err);
});

// ✅ Changed here to bind on 0.0.0.0
app.listen(PORT, '0.0.0.0', () => {
    console.log(`
Don't Forget To Give Star ‼️

𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝐁𝚈 HASHAN-𝐌𝙳

Server running on http://0.0.0.0:` + PORT);
});

module.exports = app;
