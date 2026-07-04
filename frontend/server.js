const express = require('express');
const path = require('path');
const axios = require('axios');
const session = require('express-session');
require('dotenv').config();

const app = express();
// Frontend murni berjalan di Port 3001 (Satu pintu untuk semua)
const PORT = process.env.PORT || 3001;

// ==========================================
// MAP PORT BACKEND WORKER (Sesuai Request Anda)
// ==========================================
const BACKEND_WORKERS = {
    'kasir1': 4001,
    'kasir2': 4002,
    'staff_gudang1': 5001,
    'staff_gudang2': 5002,
    'staff_gudang3': 5003,
    'staff_gudang4': 5004,
    'kurir1': 6001,
    'kurir2': 6002,
    'manager': 7001,
    'manager_gudang1': 8001,
    'manager_gudang2': 8002,
};

// Backend Utama (Gateway untuk Auth & Ambil Katalog Bibit)
const BACKEND_MAIN = 'http://localhost:3000/api';
const apiClient = axios.create({ baseURL: BACKEND_MAIN });

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Konfigurasi Session di Port 3001
app.use(session({
    secret: 'bioroots-super-secret-key',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 1000 * 60 * 60 * 8 }
}));

// Middleware Proteksi Halaman
const requireAuth = (allowedRole) => {
    return (req, res, next) => {
        if (!req.session.user) return res.redirect('/login');
        if (req.session.user.role !== allowedRole) return res.status(403).send('Akses Ditolak!');
        next();
    };
};

// ==========================================
// ROUTING PUBLIK & AUTH
// ==========================================

// 1. Form Pelanggan (Nembak ke Backend Utama 3000)
app.get('/', async (req, res) => {
    try {
        const response = await apiClient.get('/inventaris');
        res.render('customer', {
            title: 'BioRoots - B2B Order Form',
            katalogBibit: response.data,
            apiUrl: BACKEND_MAIN
        });
    } catch (error) {
        res.render('customer', { title: 'BioRoots - B2B Order Form', katalogBibit: [], apiUrl: BACKEND_MAIN });
    }
});

// 2. Tampilan Login
app.get('/login', (req, res) => {
    if (req.session.user) return res.redirect(`/${req.session.user.role}`);
    res.render('login', { title: 'Login Karyawan - BioRoots', error: null });
});

// 3. Proses Login (Validasi ke MongoDB via Backend 3000)
app.post('/login', async (req, res) => {
    const { username, password } = req.body;
    try {
        const response = await apiClient.post('/auth/login', { username, password });

        if (response.data.success) {
            req.session.user = response.data.user;
            req.session.save((err) => {
                if (err) return res.render('login', { title: 'Login', error: 'Gagal sesi' });
                res.redirect(`/${response.data.user.role}`);
            });
        }
    } catch (error) {
        let errorMsg = 'Username atau Password salah!';
        res.render('login', { title: 'Login Karyawan', error: errorMsg });
    }
});

// 4. Proses Logout
app.get('/logout', (req, res) => {
    req.session.destroy();
    res.redirect('/login');
});

// ==========================================
// ROUTING TERPROTEKSI (Dinamis Menunjuk Port Worker)
// ==========================================

app.get('/kasir', requireAuth('kasir'), (req, res) => {
    const username = req.session.user.username;
    // Ambil port target backend berdasarkan siapa yang login
    const workerPort = BACKEND_WORKERS[username] || 4001;

    res.render('kasir', {
        title: 'BioRoots - Kasir POS',
        user: req.session.user,
        apiUrl: `http://localhost:${workerPort}/api`,
        socketUrl: `http://localhost:3000`
    });
});

app.get('/staff_gudang', requireAuth('staff_gudang'), (req, res) => {
    const username = req.session.user.username;
    const workerPort = BACKEND_WORKERS[username] || 5001;

    res.render('staff_gudang', {
        title: 'BioRoots - Staff Gudang',
        user: req.session.user,
        apiUrl: `http://localhost:${workerPort}/api`,
        socketUrl: `http://localhost:3000`
    });
});

app.get('/manager_gudang', requireAuth('manager_gudang'), (req, res) => {
    const username = req.session.user.username;
    const workerPort = BACKEND_WORKERS[username] || 8001;

    res.render('manager_gudang', {
        title: 'BioRoots - Manager Gudang',
        user: req.session.user,
        apiUrl: `http://localhost:${workerPort}/api`,
        socketUrl: `http://localhost:3000`
    });
});

app.get('/kurir', requireAuth('kurir'), (req, res) => {
    const username = req.session.user.username;
    const workerPort = BACKEND_WORKERS[username] || 6001;

    res.render('kurir', {
        title: 'BioRoots - Kurir',
        user: req.session.user,
        apiUrl: `http://localhost:${workerPort}/api`,
        socketUrl: `http://localhost:3000`
    });
});

app.get('/manager', requireAuth('manager'), (req, res) => {
    const workerPort = BACKEND_WORKERS['manager'] || 7001;

    res.render('manager', {
        title: 'BioRoots - Manager Dashboard',
        user: req.session.user,
        apiUrl: `http://localhost:${workerPort}/api`,
        socketUrl: `http://localhost:3000`
    });
});

app.listen(PORT, () => {
    console.log(`🎨 Frontend Server BioRoots berjalan di http://localhost:${PORT}`);
});