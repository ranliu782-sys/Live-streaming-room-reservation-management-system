const os = require('os');
const express = require('express');
const path = require('path');
const mysql = require('mysql2/promise');
const cookieParser = require('cookie-parser');
const helmet = require('helmet');
const app = express();
const PORT = 8081;
// 直播结束后多少天仍未提交网盘链接，视为逾期（逾期后暂停该生预约，直到教师确认已提交）
const SUBMIT_GRACE_DAYS = 3;

const COOKIE_SECRET = 'live-booking-secret-key-2026-please-change';
const COOKIE_NAME = 'user_token';
const COOKIE_MAX_AGE = 7 * 24 * 3600 * 1000;

app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json());
app.use(cookieParser(COOKIE_SECRET));
app.use(express.static(path.join(__dirname, 'public'), {
    maxAge: '1d',
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache');
    }
}));

const pool = mysql.createPool({
    host: 'localhost', port: 3306, user: 'root', password: 'liurootA',
    database: 'live_booking', charset: 'utf8mb4', dateStrings: true,
    waitForConnections: true, connectionLimit: 10, queueLimit: 0
});

// 启动自检：补齐历史上多次引发 500 的缺失字段（数据库恢复旧备份后也能自动修好）
const REQUIRED_COLUMNS = [
    { table: 'group_scores', column: 'student_name', ddl: "ALTER TABLE group_scores ADD COLUMN student_name VARCHAR(64) DEFAULT NULL AFTER student_no" },
    { table: 'bookings', column: 'late_flag', ddl: "ALTER TABLE bookings ADD COLUMN late_flag TINYINT NOT NULL DEFAULT 0" },
    { table: 'score_change_requests', column: 'student_name', ddl: "ALTER TABLE score_change_requests ADD COLUMN student_name VARCHAR(64) DEFAULT NULL AFTER student_no" },
    { table: 'score_change_requests', column: 'new_comment', ddl: "ALTER TABLE score_change_requests ADD COLUMN new_comment VARCHAR(512) DEFAULT NULL AFTER new_score" },
    { table: 'score_change_requests', column: 'apply_teacher_id', ddl: "ALTER TABLE score_change_requests ADD COLUMN apply_teacher_id VARCHAR(64) DEFAULT NULL AFTER reason" },
    { table: 'password_change_requests', column: 'rollback_by', ddl: "ALTER TABLE password_change_requests ADD COLUMN rollback_by VARCHAR(64) DEFAULT NULL AFTER review_at" },
    { table: 'password_change_requests', column: 'rollback_at', ddl: "ALTER TABLE password_change_requests ADD COLUMN rollback_at DATETIME DEFAULT NULL AFTER rollback_by" },
    { table: 'admin_action_requests', column: 'used_at', ddl: "ALTER TABLE admin_action_requests ADD COLUMN used_at DATETIME DEFAULT NULL AFTER review_at" }
];

const REQUIRED_TABLES = [
    {
        name: 'admin_action_requests',
        ddl: `CREATE TABLE IF NOT EXISTS admin_action_requests (
                id INT AUTO_INCREMENT PRIMARY KEY,
                apply_admin_id VARCHAR(64) NOT NULL,
                apply_admin_name VARCHAR(64) DEFAULT NULL,
                action_type VARCHAR(32) NOT NULL,
                target_role VARCHAR(20) DEFAULT NULL,
                target_id VARCHAR(64) DEFAULT NULL,
                target_name VARCHAR(64) DEFAULT NULL,
                new_password VARCHAR(128) DEFAULT NULL,
                reason VARCHAR(512) DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                reviewer_id VARCHAR(64) DEFAULT NULL,
                review_comment VARCHAR(512) DEFAULT NULL,
                review_at DATETIME DEFAULT NULL,
                used_at DATETIME DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                KEY idx_status (status),
                KEY idx_apply (apply_admin_id)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    }
];

async function ensureSchema() {
    let fixed = 0;
    for (const t of REQUIRED_TABLES) {
        try {
            const [rows] = await pool.query('SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? LIMIT 1', [t.name]);
            if (rows.length === 0) { await pool.query(t.ddl); fixed++; console.log(`🛠 已自动创建数据表 ${t.name}`); }
        } catch (e) { console.error(`⚠️ 数据表自检失败 ${t.name}:`, e.message); }
    }
    for (const item of REQUIRED_COLUMNS) {
        try {
            const [rows] = await pool.query(
                'SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
                [item.table, item.column]
            );
            if (rows.length === 0) { await pool.query(item.ddl); fixed++; console.log(`🛠 已自动补齐字段 ${item.table}.${item.column}`); }
        } catch (e) { console.error(`⚠️ 字段自检失败 ${item.table}.${item.column}:`, e.message); }
    }
    console.log(fixed > 0 ? `✅ 数据结构自检完成，修复 ${fixed} 项` : '✅ 数据结构自检通过');
}

// 数据库错误转成看得懂的话，避免把 SQL 原文丢给老师
function dbErrorMessage(e) {
    if (!e) return '服务器开小差了，请稍后重试';
    if (e.code === 'ER_BAD_FIELD_ERROR') { console.error('❌ 数据结构缺字段：', e.sqlMessage); return '系统数据结构异常（缺少字段），请把这条提示发给管理员'; }
    if (e.code === 'ER_NO_SUCH_TABLE') { console.error('❌ 数据表不存在：', e.sqlMessage); return '系统数据结构异常（缺少数据表），请把这条提示发给管理员'; }
    return e.message;
}

(async () => {
    try { const conn = await pool.getConnection(); console.log('✅ MySQL 连接成功'); conn.release(); await ensureSchema(); }
    catch (e) { console.error('❌ MySQL 连接失败：', e.message); }
})();

const sseClients = [];
app.get('/api/quotes/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream'); res.setHeader('Cache-Control', 'no-cache'); res.setHeader('Connection', 'keep-alive');
    sseClients.push(res);
    req.on('close', () => { const index = sseClients.indexOf(res); if (index !== -1) sseClients.splice(index, 1); });
});
const notifyQuoteUpdate = () => sseClients.forEach(client => client.write(`data: refresh\n\n`));

const activeTokens = new Map(); const START_TIME = Date.now(); let requestCount = 0;
app.use((req, res, next) => {
    requestCount++; const token = req.signedCookies && req.signedCookies[COOKIE_NAME];
    if (token) { try { const d = JSON.parse(Buffer.from(token, 'base64').toString('utf8')); activeTokens.set(token, { role: d.role, id: d.id, lastActive: Date.now(), ip: req.ip || '' }); } catch (e) {} }
    next();
});
setInterval(() => { const now = Date.now(); for (const [k, v] of activeTokens.entries()) { if (now - v.lastActive > 10 * 60 * 1000) activeTokens.delete(k); } }, 60000);

const ROLES = {
    student: { table: 'students', idField: 'student_no' }, teacher: { table: 'teachers', idField: 'teacher_id' },
    director: { table: 'directors', idField: 'director_id' }, dean: { table: 'deans', idField: 'dean_id' }, admin: { table: 'admins', idField: 'admin_id' }
};

// 同一账号可能同时存在于多张角色表（例如既是教师又是三级院长）。
// 登录时按「账号+密码」逐表匹配，密码不一致就只能匹配到一个身份，选不出另一个身份；
// 所以改密码/重置密码时把该账号在所有角色表中的密码一起更新，保持多身份可切换。
async function syncPasswordAllRoles(id, pwd, mustChange) {
    const flag = mustChange === undefined ? null : (mustChange ? 1 : 0);
    for (const cfg of Object.values(ROLES)) {
        try {
            if (flag === null) await pool.query(`UPDATE ${cfg.table} SET password_hash=? WHERE ${cfg.idField}=?`, [pwd, id]);
            else await pool.query(`UPDATE ${cfg.table} SET password_hash=?, must_change_password=? WHERE ${cfg.idField}=?`, [pwd, flag, id]);
        } catch (e) { /* 单表异常不影响其它角色 */ }
    }
}

const getAdminIdFromReq = (req) => {
    const t = req.signedCookies && req.signedCookies[COOKIE_NAME];
    if (t) { try { const d = JSON.parse(Buffer.from(t, 'base64').toString('utf8')); if (d.role === 'admin') return d.id; } catch (e) {} }
    const b = req.signedCookies && req.signedCookies['admin_backup_token'];
    if (b) { try { const a = JSON.parse(Buffer.from(b, 'base64').toString('utf8')); if (a.role === 'admin') return a.id; } catch (e) {} }
    return null;
};

// 只有 admin 是超级管理员，可以直接改数据；其他管理员账号只能提交申请，由 admin 审批执行
const SUPER_ADMIN_ID = 'admin';
const isSuperAdmin = (adminId) => adminId === SUPER_ADMIN_ID;
// 非超级管理员调用直接改写数据的接口时，统一返回「请提交申请」
const denyNeedSuper = (res) => res.status(403).json({ code: 'NEED_SUPER', message: '该操作需要系统管理员 admin 审批，请先提交申请' });

// 管理员账号被锁定后，已登录的会话也必须立即失效（否则拿着旧 cookie 仍能调接口）
app.use('/api/admin', async (req, res, next) => {
    const adminId = getAdminIdFromReq(req);
    if (!adminId) return next();
    try {
        const [rows] = await pool.query('SELECT status FROM admins WHERE admin_id=? LIMIT 1', [adminId]);
        if (rows.length === 0 || rows[0].status !== 'active') {
            res.clearCookie(COOKIE_NAME);
            return res.status(403).json({ code: 'ADMIN_LOCKED', message: '该管理员账号已被锁定，请联系系统管理员 admin' });
        }
    } catch (e) { /* 数据库异常时放行，避免整套管理端不可用 */ }
    next();
});

async function addLog(req, action, detail = '') {
    try {
        let role = 'unknown', userId = 'unknown', userName = '系统';
        const t = req.signedCookies && req.signedCookies[COOKIE_NAME];
        if (t) { const d = JSON.parse(Buffer.from(t, 'base64').toString('utf8')); role = d.role; userId = d.id; const c = ROLES[role]; if (c) { const [r] = await pool.query(`SELECT name FROM ${c.table} WHERE ${c.idField}=?`, [userId]); if (r.length > 0) userName = r[0].name; } }
        const ip = req.ip || req.connection.remoteAddress || '';
        await pool.query('INSERT INTO operation_logs (user_role, user_id, user_name, action, detail, ip) VALUES (?,?,?,?,?,?)', [role, userId, userName, action, detail, ip]);
    } catch (e) {}
}
async function addErrorLog(req, title, err) {
    try {
        let role = 'unknown', userId = 'unknown', userName = '系统';
        if (req && req.signedCookies) { const t = req.signedCookies[COOKIE_NAME]; if (t) { const d = JSON.parse(Buffer.from(t, 'base64').toString('utf8')); role = d.role; userId = d.id; const c = ROLES[role]; if (c) { const [r] = await pool.query(`SELECT name FROM ${c.table} WHERE ${c.idField}=?`, [userId]); if (r.length > 0) userName = r[0].name; } } }
        const ip = req ? (req.ip || req.connection?.remoteAddress || '') : 'system', msg = err ? (err.stack || err.message || String(err)) : '未知错误';
        await pool.query('INSERT INTO operation_logs (user_role, user_id, user_name, action, detail, ip) VALUES (?,?,?,?,?,?)', [role, userId, userName, `【系统报错】${title}`, msg, ip]);
    } catch (e) {}
}
process.on('uncaughtException', async (err) => { await addErrorLog(null, '未捕获异常', err); });
process.on('unhandledRejection', async (reason) => { await addErrorLog(null, '未处理的Promise拒绝', reason); });

// [安全修复] 原 /api/reset 接口已被移除。
// 该接口无任何鉴权，任何访问者都可把全站账号密码重置为固定值，
// 存在严重越权风险。如需批量重置密码，请通过数据库或管理后台操作。

// 🌟 正式登录接口：带强清洗
app.post('/api/login', async (req, res) => {
    let userId = String(req.body.userId || '').trim().replace(/[\uFF01-\uFF5E]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0));
    let password = String(req.body.password || '').trim().replace(/[\uFF01-\uFF5E]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0));
    const forceRole = req.body.forceRole;

    console.log("🔥 收到登录请求，账号:", userId, "（密码已隐藏）");

    if (!userId || !password) return res.status(400).json({ code: 'BAD_REQUEST', message: '参数缺失' });

    try {
        let foundList = [];
        for (const [role, cfg] of Object.entries(ROLES)) {
            if (forceRole && role !== forceRole) continue;

            const [allUsers] = await pool.query(`SELECT * FROM ${cfg.table}`);
            
            const matchedUser = allUsers.find(u => {
                const dbId = String(u[cfg.idField] || '').trim();
                const dbPwd = String(u.password_hash || '').trim();
                return dbId === userId && dbPwd === password;
            });

            if (matchedUser) {
                if (matchedUser.status === 'disabled') return res.status(403).json({ code: 'DISABLED', message: `您的 ${role} 身份已被禁用` });
                if (matchedUser.status === 'locked') return res.status(403).json({ code: 'LOCKED', message: `您的 ${role} 身份已锁定` });
                
                foundList.push({ role, cfg, user: matchedUser });
            }
        }

        if (foundList.length === 0) {
            console.log("❌ 登录失败：账号或密码错误");
            return res.status(401).json({ code: 'WRONG_PASSWORD', message: '账号不存在或密码错误' });
        }

        if (foundList.length > 1 && !forceRole) {
            console.log("✅ 检测到多重身份，返回角色列表");
            return res.json({
                code: 'MULTI_ROLE',
                roles: foundList.map(f => ({ role: f.role, name: f.user.name }))
            });
        }

        const { role, cfg, user } = foundList[0];
        await pool.query(`UPDATE ${cfg.table} SET fail_count=0 WHERE ${cfg.idField}=?`, [user[cfg.idField]]);

        const token = Buffer.from(JSON.stringify({ role, id: user[cfg.idField] })).toString('base64');
        res.cookie(COOKIE_NAME, token, { signed: true, httpOnly: true, maxAge: COOKIE_MAX_AGE, sameSite: 'lax' });
        req.signedCookies[COOKIE_NAME] = token;
        await addLog(req, '登录系统', `角色: ${role}`);

        console.log(`🎉 登录成功！角色: ${role}`);
        res.json({ code: 'OK', user: { role, id: user[cfg.idField], name: user.name, college: user.college || '', className: user.class_name || '', mustChangePassword: user.must_change_password === 1, isSuper: role === 'admin' ? isSuperAdmin(user[cfg.idField]) : false } });

    } catch (e) {
        console.error("💥 登录异常:", e);
        await addErrorLog(req, '登录异常', e);
        res.status(500).json({ code: 'SERVER_ERROR', message: e.message });
    }
});

// ---------------- 以下全部保持原样 ----------------
app.get('/api/me', async (req, res) => {
    const token = req.signedCookies[COOKIE_NAME]; if (!token) return res.status(401).json({ code: 'NOT_LOGIN' });
    let data; try { data = JSON.parse(Buffer.from(token, 'base64').toString('utf8')); } catch (e) { return res.status(401).json({ code: 'BAD_TOKEN' }); }
    const { role, id } = data, cfg = ROLES[role]; if (!cfg) return res.status(401).json({ code: 'BAD_ROLE' });
    try {
        const [rows] = await pool.query(`SELECT * FROM ${cfg.table} WHERE ${cfg.idField}=? LIMIT 1`, [id]);
        if (rows.length === 0) { res.clearCookie(COOKIE_NAME); return res.status(401).json({ code: 'USER_NOT_FOUND' }); }
        const u = rows[0]; if (u.status !== 'active') { res.clearCookie(COOKIE_NAME); return res.status(401).json({ code: 'NOT_ACTIVE' }); }
        const ab = req.signedCookies && req.signedCookies['admin_backup_token']; let imp = null;
        if (ab) { try { const ad = JSON.parse(Buffer.from(ab, 'base64').toString('utf8')); if (ad.role === 'admin') imp = { byAdmin: ad.id }; } catch (e) {} }
        res.json({ code: 'OK', user: { role, id, name: u.name, college: u.college || '', className: u.class_name || '', mustChangePassword: u.must_change_password === 1, isSuper: role === 'admin' ? isSuperAdmin(id) : false }, impersonating: imp });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/logout', (req, res) => { addLog(req, '退出登录'); res.clearCookie(COOKIE_NAME); res.clearCookie('admin_backup_token'); res.json({ code: 'OK' }); });

app.post('/api/change-password', async (req, res) => {
    const token = req.signedCookies[COOKIE_NAME]; if (!token) return res.status(401).json({ code: 'NOT_LOGIN' });
    let data; try { data = JSON.parse(Buffer.from(token, 'base64').toString('utf8')); } catch (e) { return res.status(401).json({ code: 'BAD_TOKEN' }); }
    const { role, id } = data, cfg = ROLES[role], { oldPassword, newPassword } = req.body;
    if (!oldPassword || !newPassword || newPassword.length < 6) return res.status(400).json({ code: 'BAD_REQUEST', message: '新密码至少 6 位' });
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction(); const [rows] = await conn.query(`SELECT password_hash FROM ${cfg.table} WHERE ${cfg.idField}=? FOR UPDATE`, [id]);
        if (rows[0].password_hash !== oldPassword) { await conn.rollback(); return res.status(401).json({ code: 'WRONG_PASSWORD', message: '原密码错误' }); }
        await conn.query(`UPDATE ${cfg.table} SET password_hash=?, must_change_password=0 WHERE ${cfg.idField}=?`, [newPassword, id]);
        await conn.commit();
        await syncPasswordAllRoles(id, newPassword, false); // 多身份账号保持密码一致，才能切换角色
        addLog(req, '修改密码'); res.json({ code: 'OK', message: '密码修改成功' });
    } catch (e) { await conn.rollback(); res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } finally { conn.release(); }
});

// 修改密码已改为直接生效，不再需要管理员审批。
// 保留旧接口路径并转发到直接改密逻辑，兼容仍在调用旧接口的客户端。
app.post('/api/password-change-apply', async (req, res) => {
    const token = req.signedCookies[COOKIE_NAME]; if (!token) return res.status(401).json({ code: 'NOT_LOGIN' });
    let data; try { data = JSON.parse(Buffer.from(token, 'base64').toString('utf8')); } catch (e) { return res.status(401).json({ code: 'BAD_TOKEN' }); }
    const { role, id } = data, cfg = ROLES[role], { oldPassword, newPassword } = req.body;
    if (!oldPassword || !newPassword || newPassword.length < 6) return res.status(400).json({ code: 'BAD_REQUEST', message: '参数缺失或密码过短' });
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction(); const [rows] = await conn.query(`SELECT password_hash FROM ${cfg.table} WHERE ${cfg.idField}=? FOR UPDATE`, [id]);
        if (!rows.length) { await conn.rollback(); return res.status(404).json({ code: 'NOT_FOUND', message: '账号不存在' }); }
        if (rows[0].password_hash !== oldPassword) { await conn.rollback(); return res.status(401).json({ code: 'WRONG_PASSWORD', message: '原密码错误' }); }
        await conn.query(`UPDATE ${cfg.table} SET password_hash=?, must_change_password=0 WHERE ${cfg.idField}=?`, [newPassword, id]);
        await conn.commit();
        await syncPasswordAllRoles(id, newPassword, false);
        addLog(req, '修改密码'); res.json({ code: 'OK', message: '密码修改成功' });
    } catch (e) { await conn.rollback(); res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } finally { conn.release(); }
});

app.get('/api/admin/logs', async (req, res) => {
    if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    const { date, type } = req.query;
    try {
        let sql = 'SELECT * FROM operation_logs', c = [], p = [];
        if (date) { c.push('DATE(created_at)=?'); p.push(date); }
        if (type === 'error') c.push("action LIKE '【系统报错】%'"); else if (type === 'action') c.push("action NOT LIKE '【系统报错】%'");
        if (c.length > 0) sql += ' WHERE ' + c.join(' AND ');
        sql += ' ORDER BY created_at DESC LIMIT 2000';
        const [rows] = await pool.query(sql, p); res.json({ code: 'OK', list: rows });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 密码审批功能已移除：改密直接生效，以下旧接口仅返回提示，兼容旧客户端调用。
app.get('/api/admin/password-requests', async (req, res) => {
    if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    res.json({ code: 'OK', list: [], disabled: true, message: '密码审批功能已移除，改密直接生效' });
});

app.post('/api/admin/review-password', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    res.status(410).json({ code: 'DISABLED', message: '密码审批功能已移除，无需审批' });
});

app.post('/api/admin/rollback-password', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    res.status(410).json({ code: 'DISABLED', message: '密码审批功能已移除，无需撤销' });
});

// 锁定 / 解锁账号（仅 admin）。锁定管理员账号后，对方无法登录且已登录会话立即失效
app.post('/api/admin/set-account-status', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    if (!isSuperAdmin(adminId)) return denyNeedSuper(res);
    const { role, targetId, status } = req.body || {};
    const cfg = ROLES[role];
    if (!cfg || !targetId) return res.status(400).json({ code: 'BAD_REQUEST', message: '请选择要操作的账号' });
    if (status !== 'locked' && status !== 'active') return res.status(400).json({ code: 'BAD_REQUEST', message: '状态参数无效' });
    if (role === 'admin' && isSuperAdmin(targetId)) return res.status(400).json({ code: 'BAD_REQUEST', message: '系统管理员 admin 账号不能被锁定' });
    try {
        const [rows] = await pool.query(`SELECT name FROM ${cfg.table} WHERE ${cfg.idField}=?`, [targetId]);
        if (rows.length === 0) return res.status(404).json({ code: 'NOT_FOUND', message: '账号不存在' });
        await pool.query(`UPDATE ${cfg.table} SET status=?, fail_count=0 WHERE ${cfg.idField}=?`, [status, targetId]);
        addLog(req, status === 'locked' ? '锁定账号' : '解锁账号', `${role}:${targetId} ${rows[0].name}`);
        res.json({ code: 'OK', message: status === 'locked' ? `已锁定 ${rows[0].name}，该账号无法登录` : `已解锁 ${rows[0].name}` });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) }); }
});

app.get('/api/admin/server-status', async (req, res) => {
    if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    let dbOk = false, dbLat = 0;
    try { const t0 = Date.now(); const c = await pool.getConnection(); await c.query('SELECT 1'); c.release(); dbLat = Date.now() - t0; dbOk = true; } catch (e) {}
    const mem = process.memoryUsage(), tot = os.totalmem(), fre = os.freemem(), on = { student: 0, teacher: 0, director: 0, dean: 0, admin: 0 };
    activeTokens.forEach(v => { if (on[v.role] !== undefined) on[v.role]++; });
    let stats = { bookings: 0, activeBookings: 0, pendingConfirm: 0, pendingScore: 0, pendingPwdReq: 0 };
    try {
        const [[b1]] = await pool.query('SELECT COUNT(*) AS c FROM bookings'), [[b2]] = await pool.query('SELECT COUNT(*) AS c FROM bookings WHERE booking_status="booked"');
        const [[b3]] = await pool.query('SELECT COUNT(*) AS c FROM bookings WHERE upload_status="submitted" AND confirm_status="pending_confirm"');
        const [[b4]] = await pool.query('SELECT COUNT(*) AS c FROM bookings WHERE confirm_status="confirmed" AND review_status="pending"');
        stats = { bookings: b1.c, activeBookings: b2.c, pendingConfirm: b3.c, pendingScore: b4.c, pendingPwdReq: 0 };
    } catch (e) {}
    res.json({ code: 'OK', server: { uptimeSec: Math.floor(process.uptime()), startedAt: new Date(START_TIME).toISOString(), nodeVersion: process.version, platform: os.platform(), hostname: os.hostname(), requestCount }, memory: { processRss: mem.rss, processHeapTotal: mem.heapTotal, processHeapUsed: mem.heapUsed, systemTotal: tot, systemFree: fre, systemUsedPercent: ((tot-fre)/tot*100).toFixed(1) }, cpu: { model: os.cpus()[0]?.model || 'unknown', cores: os.cpus().length, loadAvg: os.loadavg() }, database: { status: dbOk ? 'connected' : 'error', latencyMs: dbLat }, online: on, stats });
});

app.get('/api/admin/all-users', async (req, res) => {
    if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    try {
        const [students] = await pool.query('SELECT student_no AS id, name, college, class_name, status FROM students');
        const [teachers] = await pool.query('SELECT teacher_id AS id, name, college, NULL AS class_name, status FROM teachers');
        const [directors] = await pool.query('SELECT director_id AS id, name, college, NULL AS class_name, status FROM directors');
        const [deans] = await pool.query('SELECT dean_id AS id, name, college, NULL AS class_name, status FROM deans');
        const [admins] = await pool.query('SELECT admin_id AS id, name, NULL AS college, NULL AS class_name, status FROM admins');
        res.json({ code: 'OK', users: { students, teachers, directors, deans, admins } });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/admin/reset-password', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    if (!isSuperAdmin(adminId)) return denyNeedSuper(res);
    const { role, targetId, newPassword } = req.body, cfg = ROLES[role];
    if (!cfg || !newPassword || newPassword.length < 6) return res.status(400).json({ code: 'BAD_REQUEST', message: '参数缺失或密码过短' });
    try { await pool.query(`UPDATE ${cfg.table} SET password_hash=?, must_change_password=1, status='active', fail_count=0 WHERE ${cfg.idField}=?`, [newPassword, targetId]); await syncPasswordAllRoles(targetId, newPassword, true); addLog(req, '重置密码', targetId); res.json({ code: 'OK', message: '密码已重置' }); }
    catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// ---------------- 管理员操作申请：非 admin 账号只能申请，由 admin 审批执行 ----------------
app.post('/api/admin/apply-action', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    const { actionType, targetRole, targetId, newPassword, reason } = req.body || {};
    if (actionType !== 'reset_password' && actionType !== 'impersonate') return res.status(400).json({ code: 'BAD_ACTION', message: '目前支持「重置密码」和「测试身份切换」两类申请' });
    if (isSuperAdmin(adminId)) return res.status(400).json({ code: 'BAD_REQUEST', message: '系统管理员可直接操作，无需提交申请' });
    const cfg = ROLES[targetRole];
    if (!cfg || !targetId) return res.status(400).json({ code: 'BAD_REQUEST', message: '请选择要操作的账号' });
    if (targetRole === 'admin') return res.status(400).json({ code: 'BAD_REQUEST', message: '不能申请切换到管理员账号' });
    if (actionType === 'reset_password' && (!newPassword || String(newPassword).length < 6)) return res.status(400).json({ code: 'BAD_REQUEST', message: '新密码至少 6 位' });
    if (!reason || !String(reason).trim()) return res.status(400).json({ code: 'BAD_REQUEST', message: '请填写申请原因，便于 admin 判断' });
    try {
        const [t] = await pool.query(`SELECT name, status FROM ${cfg.table} WHERE ${cfg.idField}=?`, [targetId]);
        if (t.length === 0) return res.status(404).json({ code: 'NOT_FOUND', message: '账号不存在' });
        if (actionType === 'impersonate' && t[0].status !== 'active') return res.status(400).json({ code: 'BAD_REQUEST', message: '该账号当前不可用，无法切换' });
        const [dup] = await pool.query(`SELECT id FROM admin_action_requests WHERE apply_admin_id=? AND action_type=? AND target_role=? AND target_id=? AND status='pending' LIMIT 1`, [adminId, actionType, targetRole, targetId]);
        if (dup.length > 0) return res.status(400).json({ code: 'DUPLICATED', message: '该账号已有待审批的申请，请等待 admin 处理' });
        const [me] = await pool.query('SELECT name FROM admins WHERE admin_id=? LIMIT 1', [adminId]);
        const [ins] = await pool.query(
            `INSERT INTO admin_action_requests (apply_admin_id, apply_admin_name, action_type, target_role, target_id, target_name, new_password, reason) VALUES (?,?,?,?,?,?,?,?)`,
            [adminId, me.length ? me[0].name : adminId, actionType, targetRole, targetId, t[0].name, actionType === 'reset_password' ? String(newPassword) : null, String(reason).trim()]
        );
        addLog(req, '提交管理员申请', `${actionType === 'impersonate' ? '切换测试身份' : '重置密码'} → ${targetRole}:${targetId}`);
        res.json({ code: 'OK', message: actionType === 'impersonate' ? '切换申请已提交，等 admin 审批通过后即可进入' : '申请已提交，等待系统管理员 admin 审批', id: ins.insertId });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) }); }
});

app.get('/api/admin/action-requests', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    const filter = req.query.filter === 'history' ? 'history' : 'pending', superAdmin = isSuperAdmin(adminId);
    try {
        const c = [filter === 'pending' ? "status='pending'" : "status<>'pending'"], p = [];
        if (!superAdmin) { c.push('apply_admin_id=?'); p.push(adminId); }
        const [rows] = await pool.query(`SELECT * FROM admin_action_requests WHERE ${c.join(' AND ')} ORDER BY created_at DESC LIMIT 200`, p);
        rows.forEach(r => { if (!superAdmin) delete r.new_password; });
        res.json({ code: 'OK', isSuper: superAdmin, list: rows });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) }); }
});

app.post('/api/admin/review-action', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    if (!isSuperAdmin(adminId)) return denyNeedSuper(res);
    const { requestId, action, comment } = req.body || {};
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        const [rows] = await conn.query('SELECT * FROM admin_action_requests WHERE id=? AND status="pending" FOR UPDATE', [requestId]);
        if (rows.length === 0) { await conn.rollback(); return res.status(404).json({ code: 'NOT_FOUND', message: '申请不存在或已处理' }); }
        const r = rows[0];
        if (action === 'approve') {
            if (r.action_type === 'impersonate') {
                // 切换身份不需要改数据，审批通过即发放一次性授权（24 小时内有效，用后作废）
                if (!ROLES[r.target_role]) { await conn.rollback(); return res.status(400).json({ code: 'BAD_REQUEST', message: '目标角色无效' }); }
                await conn.query('UPDATE admin_action_requests SET status="approved", reviewer_id=?, review_comment=?, review_at=NOW() WHERE id=?', [adminId, comment || '', requestId]);
                await conn.commit();
                addLog(req, '审批管理员申请', `通过 · 切换测试身份 ${r.apply_admin_id} → ${r.target_role}:${r.target_id}`);
                return res.json({ code: 'OK', message: '已通过，申请人可一次性切换到该账号（24 小时内有效）' });
            }
            if (r.action_type !== 'reset_password') { await conn.rollback(); return res.status(400).json({ code: 'BAD_ACTION', message: '未知的申请类型' }); }
            const cfg = ROLES[r.target_role];
            if (!cfg) { await conn.rollback(); return res.status(400).json({ code: 'BAD_REQUEST', message: '目标角色无效' }); }
            await conn.query(`UPDATE ${cfg.table} SET password_hash=?, must_change_password=1, status='active', fail_count=0 WHERE ${cfg.idField}=?`, [r.new_password, r.target_id]);
            await conn.query('UPDATE admin_action_requests SET status="approved", reviewer_id=?, review_comment=?, review_at=NOW() WHERE id=?', [adminId, comment || '', requestId]);
            await conn.commit();
            await syncPasswordAllRoles(r.target_id, r.new_password, true);
            addLog(req, '审批管理员申请', `通过 · 重置密码 ${r.target_role}:${r.target_id}`);
            return res.json({ code: 'OK', message: '已通过，密码已重置' });
        }
        await conn.query('UPDATE admin_action_requests SET status="rejected", reviewer_id=?, review_comment=?, review_at=NOW() WHERE id=?', [adminId, comment || '', requestId]);
        await conn.commit();
        addLog(req, '审批管理员申请', `拒绝 · ${r.target_role}:${r.target_id}`);
        res.json({ code: 'OK', message: '已拒绝' });
    } catch (e) { await conn.rollback(); res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) }); } finally { conn.release(); }
});

app.get('/api/admin/test-accounts', async (req, res) => {
    const adminId = getAdminIdFromReq(req); if (!adminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    try {
        const [students] = await pool.query('SELECT student_no AS id, name, college, class_name, status FROM students');
        const [teachers] = await pool.query('SELECT teacher_id AS id, name, college, NULL AS class_name, status FROM teachers');
        const [directors] = await pool.query('SELECT director_id AS id, name, college, NULL AS class_name, status FROM directors');
        const [deans] = await pool.query('SELECT dean_id AS id, name, college, NULL AS class_name, status FROM deans');
        // 二级管理员：返回自己的切换申请，前端据此显示「待审批 / 已批准可进入 / 去申请」
        let myRequests = [];
        if (!isSuperAdmin(adminId)) {
            const [rs] = await pool.query(
                `SELECT id, target_role, target_id, status, used_at, review_at, created_at
                   FROM admin_action_requests
                  WHERE apply_admin_id=? AND action_type='impersonate' AND status IN ('pending','approved')
                  ORDER BY id DESC LIMIT 200`, [adminId]
            );
            myRequests = rs;
        }
        res.json({ code: 'OK', isSuper: isSuperAdmin(adminId), accounts: { students, teachers, directors, deans }, myRequests });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/admin/impersonate', async (req, res) => {
    const impAdminId = getAdminIdFromReq(req); if (!impAdminId) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' });
    const { targetRole, targetId, requestId } = req.body, cfg = ROLES[targetRole];
    if (!cfg || targetRole === 'admin') return res.status(400).json({ code: 'BAD_REQUEST', message: '角色无效' });
    try {
        // 二级管理员必须凭 admin 已批准、未使用、未过期的切换授权才能进入
        let authId = null;
        if (!isSuperAdmin(impAdminId)) {
            const params = [impAdminId, targetRole, targetId];
            let sql = `SELECT id, review_at FROM admin_action_requests
                        WHERE apply_admin_id=? AND action_type='impersonate' AND target_role=? AND target_id=?
                          AND status='approved' AND used_at IS NULL`;
            if (requestId) { sql += ' AND id=?'; params.push(requestId); }
            sql += ' ORDER BY id DESC LIMIT 1';
            const [rs] = await pool.query(sql, params);
            if (rs.length === 0) {
                return res.status(403).json({ code: 'NEED_APPLY', message: '切换测试身份需要先提交申请，等系统管理员 admin 审批通过后才能进入' });
            }
            const approvedAt = rs[0].review_at ? new Date(rs[0].review_at).getTime() : 0;
            if (!approvedAt || Date.now() - approvedAt > 24 * 3600 * 1000) {
                return res.status(400).json({ code: 'AUTH_EXPIRED', message: '该切换授权已超过 24 小时，请重新提交申请' });
            }
            authId = rs[0].id;
        }
        const [rows] = await pool.query(`SELECT * FROM ${cfg.table} WHERE ${cfg.idField}=?`, [targetId]);
        if (rows.length === 0 || rows[0].status !== 'active') return res.status(400).json({ code: 'BAD_REQUEST', message: '账户不可用' });
        if (authId) await pool.query('UPDATE admin_action_requests SET used_at=NOW() WHERE id=?', [authId]); // 一次性授权，用后作废
        const targetToken = Buffer.from(JSON.stringify({ role: targetRole, id: targetId })).toString('base64'), currentToken = req.signedCookies[COOKIE_NAME];
        res.cookie('admin_backup_token', currentToken, { signed: true, httpOnly: true, maxAge: COOKIE_MAX_AGE, sameSite: 'lax' });
        res.cookie(COOKIE_NAME, targetToken, { signed: true, httpOnly: true, maxAge: COOKIE_MAX_AGE, sameSite: 'lax' });
        addLog(req, '切换身份', targetId + (authId ? `（申请 #${authId} 授权）` : ''));
        res.json({ code: 'OK', message: `已切换为 ${rows[0].name}`, redirect: `/${targetRole}.html` });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/admin/restore', (req, res) => {
    const backup = req.signedCookies && req.signedCookies['admin_backup_token']; if (!backup) return res.status(400).json({ code: 'NO_BACKUP', message: '无备份' });
    res.cookie(COOKIE_NAME, backup, { signed: true, httpOnly: true, maxAge: COOKIE_MAX_AGE, sameSite: 'lax' }); res.clearCookie('admin_backup_token'); res.json({ code: 'OK', redirect: '/admin.html' });
});

const TIME_SLOTS = [{ start: '09:00', end: '09:40' }, { start: '09:50', end: '10:30' }, { start: '10:40', end: '11:20' }, { start: '12:00', end: '12:40' }, { start: '12:50', end: '13:30' }, { start: '14:00', end: '14:40' }, { start: '14:50', end: '15:30' }, { start: '15:40', end: '16:20' }, { start: '16:30', end: '17:10' }, { start: '17:20', end: '18:00' }, { start: '18:10', end: '18:50' }, { start: '19:00', end: '19:40' }];
const ROOMS = ['A', 'B', 'C'];
// 直播间对应的现实地址（看板列头标识用）
const ROOM_LOCATIONS = {
  A: '封龙书院计算机学院洽谈室',
  B: '风情小镇10号楼1楼网安优选直播间直播位1',
  C: '风情小镇10号楼1楼网安优选直播间直播位2'
};
// 直播间开放规则：A 晚上 19:00 及以后不开放；B、C 全天所有时段均可预约
const NIGHT_CLOSED_ROOMS = ['A'];
const NIGHT_CLOSED_FROM = '19:00';
const isSlotClosed = (room, start) => NIGHT_CLOSED_ROOMS.includes(room) && String(start) >= NIGHT_CLOSED_FROM;
// 预约日期范围：今天、明天、后天（共 3 天）
const BOOKING_DAYS_AHEAD = 2;
const dateKey = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const dateAfter = n => { const d = new Date(); d.setDate(d.getDate() + n); return dateKey(d); };
const allowedBookingDates = () => Array.from({ length: BOOKING_DAYS_AHEAD + 1 }, (_, i) => dateAfter(i));
const parseMembers = r => { if (!r) return []; try { return JSON.parse(r); } catch (e) { return []; } };

app.get('/api/courses', async (req, res) => { try { const [rows] = await pool.query("SELECT c.id, c.name, c.teacher_id, t.name AS teacher_name FROM courses c LEFT JOIN teachers t ON c.teacher_id = t.teacher_id ORDER BY c.id"); res.json({ code: 'OK', courses: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });
app.get('/api/student-lookup', async (req, res) => { try { const [rows] = await pool.query('SELECT student_no, name, college, class_name, status FROM students WHERE student_no=? LIMIT 1', [req.query.no]); if (rows.length === 0) return res.status(404).json({ code: 'NOT_FOUND', message: '学号不存在' }); res.json({ code: 'OK', student: rows[0] }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });

app.get('/api/board', async (req, res) => {
    const { college, date, teacherId } = req.query; if (!college || !date) return res.status(400).json({ code: 'BAD_REQUEST', message: '参数缺失' });
    try {
        let sql = `SELECT b.id, b.room, DATE_FORMAT(b.start_time,'%H:%i') AS st, b.student_no, b.student_name, s.class_name, b.course FROM bookings b LEFT JOIN students s ON b.student_no = s.student_no LEFT JOIN courses c ON b.course = c.name WHERE b.college=? AND b.date=? AND b.booking_status='booked'`;
        const params = [college, date];
        if (teacherId) { sql += ` AND c.teacher_id = ?`; params.push(teacherId); }
        const [bookings] = await pool.query(sql, params);
        const map = {}; bookings.forEach(b => { map[b.room + '|' + b.st] = b; });
        const now = new Date(), today = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0'), nowMin = now.getHours() * 60 + now.getMinutes();
        const board = ROOMS.map(room => ({ room, location: ROOM_LOCATIONS[room] || '', slots: TIME_SLOTS.map(s => {
                const b = map[room + '|' + s.start], [sh, sm] = s.start.split(':').map(Number), isPast = (date < today) || (date === today && (sh * 60 + sm) <= nowMin), closed = isSlotClosed(room, s.start);
                return { room, location: ROOM_LOCATIONS[room] || '', startTime: s.start, endTime: s.end, status: (isPast || closed) ? 'unavailable' : (b ? 'booked' : 'available'), isPast, nightClosed: closed, booking: b ? { id: b.id, studentNo: b.student_no, studentName: b.student_name, className: b.class_name, course: b.course } : null };
            }) }));
        res.json({ code: 'OK', date, college, board });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 提交义务限制：查询该生当前是否存在「未完成的提交义务」
// 命中条件（仅对本人作为组长的预约）：直播结束超过 3 天才提交（late_flag=1），且教师尚未确认。
// 说明：
//   1) 单纯「被驳回」不构成限制——驳回只要求重新提交，不影响继续预约；
//   2) 账号本身不再封禁（不写 is_banned），限制只针对预约动作，教师确认后立即解除。
async function getSubmitRestrictions(studentNo) {
    const [rows] = await pool.query(
        `SELECT id, room, date, DATE_FORMAT(start_time,'%H:%i') AS st, DATE_FORMAT(end_time,'%H:%i') AS et,
                course, netdisk_url, upload_status, confirm_status, late_flag,
                GREATEST(TIMESTAMPDIFF(DAY, CONCAT(date,' ',end_time), NOW()), 0) AS overdue_days
           FROM bookings
          WHERE student_no = ? AND booking_status = 'booked'
            AND late_flag = 1 AND confirm_status <> 'confirmed'
          ORDER BY date DESC, start_time DESC`,
        [studentNo]
    );
    return rows;
}

app.post('/api/booking/create', async (req, res) => {
    const { userId, room, date, startTime, course, members } = req.body;
    if (!userId || !room || !date || !startTime || !course || !members?.length) return res.status(400).json({ code: 'BAD_REQUEST', message: '请填写完整信息' });
    try {
        if (!ROOMS.includes(String(room))) return res.status(400).json({ code: 'BAD_ROOM', message: '直播间不存在' });
        if (!allowedBookingDates().includes(String(date).slice(0, 10))) return res.status(400).json({ code: 'BAD_DATE', message: `只能预约今天、明天、后天（${allowedBookingDates().join(' / ')}）` });
        if (isSlotClosed(room, startTime)) return res.status(400).json({ code: 'ROOM_CLOSED', message: `${room} 直播间 ${NIGHT_CLOSED_FROM} 以后不开放预约，请选择更早的时段或改约 ${ROOMS.filter(r => !NIGHT_CLOSED_ROOMS.includes(r)).join('、')} 直播间` });
        const [stu] = await pool.query('SELECT name, college FROM students WHERE student_no=? AND status="active"', [userId]);
        if (stu.length === 0) return res.status(403).json({ code: 'STUDENT_NOT_FOUND', message: '学生账号异常或不存在' });
        const blocks = await getSubmitRestrictions(userId);
        if (blocks.length > 0) {
            const msg = '您有超过 3 天才提交网盘链接的直播，预约功能已暂停，请等任课教师确认后再预约';
            return res.status(403).json({ code: 'SUBMIT_RESTRICTED', message: msg, items: blocks });
        }

        const cleanM = [];
        for (const m of members) { if (!m.studentNo || !m.name) continue; cleanM.push({ studentNo: m.studentNo.trim(), name: m.name.trim(), duty: (m.duty || '').trim() }); }

        const conn = await pool.getConnection();
        try {
            await conn.beginTransaction(); const slot = TIME_SLOTS.find(s => s.start === startTime);
            if (!slot) { await conn.rollback(); return res.status(400).json({ code: 'BAD_SLOT', message: '该时段不可预约，请刷新看板后重试' }); }
            const [ex] = await conn.query(`SELECT id FROM bookings WHERE college=? AND room=? AND date=? AND start_time=? AND booking_status='booked' FOR UPDATE`, [stu[0].college, room, date, startTime]);
            if (ex.length > 0) { await conn.rollback(); return res.status(409).json({ code: 'SLOT_TAKEN', message: '手慢了，该时段已被别人预约' }); }

            const [ins] = await conn.query(`INSERT INTO bookings (student_no, student_name, college, room, date, start_time, end_time, course, participants, upload_status) VALUES (?,?,?,?,?,?,?,?,?,'pending_live')`, [userId, stu[0].name, stu[0].college, room, date, startTime, slot.end, course, JSON.stringify(cleanM)]);
            await conn.commit(); addLog(req, '创建预约', `${room} ${date} ${startTime}`); res.json({ code: 'OK', bookingId: ins.insertId });
        } catch (e) { await conn.rollback(); res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } finally { conn.release(); }
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/booking/cancel', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM bookings WHERE id=? AND student_no=? AND booking_status="booked"', [req.body.bookingId, req.body.userId]);
        if (rows.length === 0) return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' });
        const b = rows[0], st = new Date(b.date + 'T' + b.start_time); if ((st - new Date()) / 60000 <= 120) return res.status(400).json({ code: 'TOO_LATE', message: '距开始不足2小时' });
        await pool.query('UPDATE bookings SET booking_status="cancelled" WHERE id=?', [b.id]); addLog(req, '取消预约', b.id); res.json({ code: 'OK', message: '已取消' });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/booking/upload', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM bookings WHERE id=? AND student_no=? AND booking_status="booked"', [req.body.bookingId, req.body.userId]);
        if (rows.length === 0) return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' });
        const wasLate = rows[0].late_flag === 1 || rows[0].upload_status === 'overdue';
        // 逾期标记一旦打上就保留，用于要求教师确认后才能恢复预约
        await pool.query(`UPDATE bookings SET late_flag=IF(upload_status='overdue' OR late_flag=1,1,0), netdisk_url=?, uploaded_at=NOW(), upload_status='submitted', confirm_status='pending_confirm' WHERE id=?`, [req.body.netdiskUrl, req.body.bookingId]);
        await pool.query(`UPDATE submit_reminders SET read_at=IFNULL(read_at,NOW()) WHERE booking_id=?`, [req.body.bookingId]);
        addLog(req, wasLate ? '逾期补交网盘' : '提交网盘', req.body.bookingId);
        res.json({ code: 'OK', message: wasLate ? '已提交，逾期提交需教师确认后才能继续预约' : '提交成功', late: wasLate });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.get('/api/booking/my', async (req, res) => {
    try {
        const sql = `
            SELECT b.id, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, DATE_FORMAT(b.end_time,'%H:%i') AS et,
                   b.course, b.participants, b.netdisk_url, b.uploaded_at, b.score, b.booking_status, b.upload_status,
                   b.review_status, b.confirm_status, b.late_flag, b.is_banned, b.student_no, b.student_name,
                   (SELECT status FROM pre_submit_requests WHERE booking_id = b.id ORDER BY id DESC LIMIT 1) AS pre_submit_status,
                   (SELECT review_comment FROM pre_submit_requests WHERE booking_id = b.id ORDER BY id DESC LIMIT 1) AS pre_submit_review_comment
            FROM bookings b
            WHERE b.student_no = ? OR JSON_CONTAINS(b.participants, JSON_OBJECT('studentNo', ?))
            ORDER BY b.date DESC, b.start_time DESC
        `;
        const [rows] = await pool.query(sql, [req.query.userId, req.query.userId]);
        for (const b of rows) {
            b.members = parseMembers(b.participants);
            const [gs] = await pool.query(`SELECT student_no, score, comment FROM group_scores WHERE booking_id=?`, [b.id]);
            const sm = {}; gs.forEach(g => sm[g.student_no] = g);
            b.members = b.members.map(m => ({ ...m, score: sm[m.studentNo]?.score || null, comment: sm[m.studentNo]?.comment || '' }));
        }
        res.json({ code: 'OK', bookings: rows });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 学生端：查询当前的预约限制与未读教师提醒
app.get('/api/student/restriction', async (req, res) => {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少学号' });
    try {
        const items = await getSubmitRestrictions(userId);
        const [reminders] = await pool.query(
            `SELECT r.id, r.booking_id, r.teacher_name, r.content, r.created_at, b.room, b.date, b.course
               FROM submit_reminders r LEFT JOIN bookings b ON r.booking_id = b.id
              WHERE r.student_no = ? AND r.read_at IS NULL
              ORDER BY r.id DESC LIMIT 20`, [userId]);
        res.json({ code: 'OK', restricted: items.length > 0, graceDays: SUBMIT_GRACE_DAYS, items, reminders });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 学生端：标记提醒为已读
app.post('/api/student/reminders/read', async (req, res) => {
    try {
        await pool.query(`UPDATE submit_reminders SET read_at=IFNULL(read_at,NOW()) WHERE student_no=?`, [req.body.userId]);
        res.json({ code: 'OK' });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.get('/api/booking/detail/:id', async (req, res) => {
    try { const [rows] = await pool.query(`SELECT *, DATE_FORMAT(start_time,'%H:%i') AS st, DATE_FORMAT(end_time,'%H:%i') AS et FROM bookings WHERE id=?`, [req.params.id]); if (!rows.length) return res.status(404).json({ code: 'NOT_FOUND', message: '不存在' }); res.json({ code: 'OK', booking: { ...rows[0], members: parseMembers(rows[0].participants) } }); }
    catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/booking/pre-submit-apply', async (req, res) => { try { await pool.query(`INSERT INTO pre_submit_requests (booking_id, student_no, reason) VALUES (?,?,?)`, [req.body.bookingId, req.body.userId, req.body.reason]); res.json({ code: 'OK', message: '申请已提交' }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });

app.get('/api/teacher/pre-submit-pending', async (req, res) => {
    if (!req.query.teacherId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少教师ID' });
    try { const [rows] = await pool.query(`SELECT r.*, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, b.course FROM pre_submit_requests r JOIN bookings b ON r.booking_id = b.id JOIN courses c ON b.course = c.name WHERE r.status='pending' AND c.teacher_id = ?`, [req.query.teacherId]); res.json({ code: 'OK', list: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/teacher/pre-submit-review', async (req, res) => { try { await pool.query(`UPDATE pre_submit_requests SET status=?, review_comment=?, review_at=NOW() WHERE id=?`, [req.body.action === 'approve' ? 'approved' : 'rejected', req.body.comment || '', req.body.requestId]); res.json({ code: 'OK', message: '审批完成' }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });

app.get('/api/teacher/pending-confirm', async (req, res) => {
    if (!req.query.teacherId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少教师ID' });
    try {
        const [rows] = await pool.query(`SELECT b.id, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, b.student_no, b.student_name, b.course, b.netdisk_url, b.uploaded_at, b.participants, b.upload_status, b.confirm_status, b.late_flag, GREATEST(TIMESTAMPDIFF(DAY, CONCAT(b.date,' ',b.end_time), NOW()), 0) AS overdue_days FROM bookings b JOIN courses c ON b.course = c.name WHERE c.teacher_id = ? AND b.booking_status='booked' AND ((b.upload_status='submitted' AND b.confirm_status='pending_confirm') OR (b.upload_status='overdue' AND b.confirm_status <> 'confirmed'))`, [req.query.teacherId]);
        rows.forEach(b => b.members = parseMembers(b.participants)); res.json({ code: 'OK', list: rows });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 教师端：催交提醒列表（直播结束超过 3 天仍未提交、且教师尚未确认的预约）
app.get('/api/teacher/remind-list', async (req, res) => {
    if (!req.query.teacherId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少教师ID' });
    try {
        const [rows] = await pool.query(
            `SELECT b.id, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, DATE_FORMAT(b.end_time,'%H:%i') AS et,
                    b.student_no, b.student_name, b.course, b.upload_status, b.confirm_status, b.late_flag, b.netdisk_url,
                    GREATEST(TIMESTAMPDIFF(DAY, CONCAT(b.date,' ',b.end_time), NOW()), 0) AS overdue_days,
                    (SELECT COUNT(*) FROM submit_reminders r WHERE r.booking_id = b.id) AS remind_times,
                    (SELECT MAX(r.created_at) FROM submit_reminders r WHERE r.booking_id = b.id) AS last_remind_at
               FROM bookings b JOIN courses c ON b.course = c.name
              WHERE c.teacher_id = ? AND b.booking_status = 'booked' AND b.confirm_status <> 'confirmed'
                AND (b.late_flag = 1 OR b.upload_status = 'overdue' OR b.confirm_status = 'rejected')
              ORDER BY b.date DESC, b.start_time DESC`, [req.query.teacherId]);
        res.json({ code: 'OK', list: rows });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 教师端：发送催交提醒（学生端下次进入会弹出提示）
app.post('/api/teacher/remind-submit', async (req, res) => {
    const { bookingId, teacherId, content } = req.body;
    if (!bookingId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少预约ID' });
    try {
        const [b] = await pool.query('SELECT id, student_no, room, date FROM bookings WHERE id=?', [bookingId]);
        if (!b.length) return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' });
        const [t] = await pool.query('SELECT name FROM teachers WHERE teacher_id=?', [teacherId]);
        const teacherName = (t[0] && t[0].name) || '任课教师';
        const msg = (content && String(content).trim()) || `您的直播（${b[0].date} 直播间${b[0].room}）已超过 ${SUBMIT_GRACE_DAYS} 天未提交网盘链接，请尽快提交，逾期将无法继续预约直播间。`;
        await pool.query('INSERT INTO submit_reminders (booking_id, student_no, teacher_id, teacher_name, content) VALUES (?,?,?,?,?)', [b[0].id, b[0].student_no, teacherId || null, teacherName, msg]);
        addLog(req, '提醒提交网盘', bookingId);
        res.json({ code: 'OK', message: '提醒已发送，学生登录后会看到提示' });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

// 教师端：确认 / 驳回网盘链接
// 确认后该预约不再限制学生继续预约；驳回不再封禁账号，只需重新提交并等待确认
app.post('/api/teacher/confirm', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM bookings WHERE id=?', [req.body.bookingId]);
        if (!rows.length) return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' });
        const b = rows[0];
        if (req.body.action === 'confirm') {
            const nextUpload = (!b.netdisk_url && (b.upload_status === 'overdue' || b.upload_status === 'pending_upload' || b.upload_status === 'pending_live')) ? 'submitted' : b.upload_status;
            await pool.query(`UPDATE bookings SET confirm_status='confirmed', is_banned=0, confirmed_at=NOW(), upload_status=? WHERE id=?`, [nextUpload, b.id]);
            await pool.query(`UPDATE submit_reminders SET read_at=IFNULL(read_at,NOW()) WHERE booking_id=?`, [b.id]);
            addLog(req, '确认网盘链接', b.id);
            res.json({ code: 'OK', message: '已确认，学生的预约限制已解除' });
        } else {
            await pool.query(`UPDATE bookings SET confirm_status='rejected', upload_status='pending_upload', netdisk_url=NULL, is_banned=0 WHERE id=?`, [b.id]);
            addLog(req, '驳回网盘链接', b.id);
            res.json({ code: 'OK', message: '已驳回，学生需重新提交' });
        }
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.get('/api/teacher/pending-score', async (req, res) => {
    if (!req.query.teacherId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少教师ID' });
    try { const [rows] = await pool.query(`SELECT b.id, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, b.student_no, b.student_name, b.course, b.netdisk_url, b.participants FROM bookings b JOIN courses c ON b.course = c.name WHERE c.teacher_id = ? AND b.booking_status='booked' AND b.confirm_status='confirmed' AND b.review_status='pending'`, [req.query.teacherId]); rows.forEach(b => b.members = parseMembers(b.participants)); res.json({ code: 'OK', list: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/teacher/score', async (req, res) => {
    const bookingId = String(req.body.bookingId || '').trim();
    const scores = Array.isArray(req.body.scores) ? req.body.scores : [];
    if (!bookingId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少预约ID' });
    if (scores.length === 0) return res.status(400).json({ code: 'BAD_REQUEST', message: '请至少为一名组员打分' });
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        const [bk] = await conn.query('SELECT * FROM bookings WHERE id=? FOR UPDATE', [bookingId]);
        if (!bk.length) { await conn.rollback(); return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' }); }
        if (bk[0].review_status !== 'pending') { await conn.rollback(); return res.status(400).json({ code: 'ALREADY_SCORED', message: '该预约已打分，如需修改请提交分数修改申请' }); }
        const members = parseMembers(bk[0].participants), key = x => String(x == null ? '' : x).trim();
        // 校验：只有该课程的主讲教师才能打分（课程未绑定教师时不做限制）
        const [own] = await conn.query('SELECT teacher_id FROM courses WHERE name=? LIMIT 1', [bk[0].course]);
        if (own.length > 0 && own[0].teacher_id && req.body.teacherId && String(own[0].teacher_id) !== String(req.body.teacherId)) {
            await conn.rollback(); return res.status(403).json({ code: 'FORBIDDEN', message: '只有该课程的主讲教师可以打分' });
        }
        await conn.query('DELETE FROM group_scores WHERE booking_id=?', [bookingId]);
        let saved = 0, sum = 0;
        for (const s of scores) {
            const val = Number(s && s.score);
            if (!isFinite(val)) continue;
            const m = members.find(x => key(x.studentNo) === key(s.studentNo));
            if (!m) continue;
            await conn.query(`INSERT INTO group_scores (booking_id, student_no, student_name, score, comment) VALUES (?,?,?,?,?)`, [bookingId, key(m.studentNo), m.name, val, s.comment || '']);
            sum += val; saved++;
        }
        if (saved === 0) { await conn.rollback(); return res.status(400).json({ code: 'NO_VALID_SCORE', message: '打分数据与组员不匹配，请刷新后重试' }); }
        // 平均分按“实际保存成功的分数”计算，避免个别无效分数拉低均分
        const avg = Math.round(sum / saved);
        await conn.query(`UPDATE bookings SET score=?, teacher_id=?, review_status='completed', scored_at=NOW() WHERE id=?`, [avg, req.body.teacherId || null, bookingId]);
        await conn.commit(); res.json({ code: 'OK', message: '打分成功' });
    } catch (e) { await conn.rollback(); console.error('❌ 打分失败：', e.sqlMessage || e.message); res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) }); } finally { conn.release(); }
});

app.get('/api/teacher/scored', async (req, res) => {
    if (!req.query.teacherId) return res.status(400).json({ code: 'BAD_REQUEST', message: '缺少教师ID' });
    try { const [rows] = await pool.query(`SELECT b.id, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st, b.student_no, b.student_name, b.course, b.score, b.scored_at, b.participants FROM bookings b JOIN courses c ON b.course = c.name WHERE c.teacher_id = ? AND b.review_status='completed'`, [req.query.teacherId]); for (const b of rows) { const [gs] = await pool.query(`SELECT * FROM group_scores WHERE booking_id=?`, [b.id]); b.scores = gs; } res.json({ code: 'OK', list: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.post('/api/teacher/score-change-apply', async (req, res) => {
    try {
        const bookingId = String(req.body.bookingId).trim();
        const studentNo = String(req.body.studentNo).trim();
        const [scoreRows] = await pool.query('SELECT score, student_name FROM group_scores WHERE booking_id=? AND student_no=?', [bookingId, studentNo]);
        let oldScore = scoreRows.length > 0 ? scoreRows[0].score : null;
        let sName = scoreRows.length > 0 ? scoreRows[0].student_name : req.body.studentNo;

        await pool.query(`INSERT INTO score_change_requests (booking_id, student_no, student_name, old_score, new_score, new_comment, reason, apply_teacher_id) VALUES (?,?,?,?,?,?,?,?)`, [bookingId, studentNo, sName, oldScore, req.body.newScore, req.body.newComment, req.body.reason, req.body.teacherId]);
        res.json({ code: 'OK', message: '申请已提交' });
    } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); }
});

app.get('/api/teacher/score-change-list', async (req, res) => { try { const [rows] = await pool.query(`SELECT * FROM score_change_requests WHERE apply_teacher_id=?`, [req.query.teacherId]); res.json({ code: 'OK', list: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });

app.get('/api/approver/pending-changes', async (req, res) => {
    try {
        const [reqs] = await pool.query(`
            SELECT r.*, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st
            FROM score_change_requests r
                     INNER JOIN bookings b ON TRIM(r.booking_id) = TRIM(b.id)
            WHERE r.status = 'pending' AND b.college = ?
        `, [req.query.college]);

        for (let r of reqs) {
            if (!r.student_name || r.old_score === null) {
                const [gs] = await pool.query('SELECT score, student_name FROM group_scores WHERE booking_id=? AND student_no=?', [String(r.booking_id).trim(), String(r.student_no).trim()]);
                if (gs.length > 0) {
                    r.old_score = r.old_score !== null ? r.old_score : gs[0].score;
                    r.student_name = r.student_name || gs[0].student_name;
                } else {
                    const [stu] = await pool.query('SELECT name FROM students WHERE student_no=?', [String(r.student_no).trim()]);
                    r.student_name = r.student_name || (stu.length > 0 ? stu[0].name : r.student_no);
                }
            }
            r.old_score = r.old_score !== null ? r.old_score : '无';
        }
        res.json({ code: 'OK', list: reqs });
    } catch (e) {
        res.status(500).json({ code: 'SERVER_ERROR', message: e.message });
    }
});

app.get('/api/approver/all-changes', async (req, res) => {
    try {
        const [reqs] = await pool.query(`
            SELECT r.*, b.room, b.date, DATE_FORMAT(b.start_time,'%H:%i') AS st
            FROM score_change_requests r
                     INNER JOIN bookings b ON TRIM(r.booking_id) = TRIM(b.id)
            WHERE r.status != 'pending' AND b.college = ?
        `, [req.query.college]);

        for (let r of reqs) {
            if (!r.student_name || r.old_score === null) {
                const [gs] = await pool.query('SELECT score, student_name FROM group_scores WHERE booking_id=? AND student_no=?', [String(r.booking_id).trim(), String(r.student_no).trim()]);
                if (gs.length > 0) {
                    r.old_score = r.old_score !== null ? r.old_score : gs[0].score;
                    r.student_name = r.student_name || gs[0].student_name;
                } else {
                    const [stu] = await pool.query('SELECT name FROM students WHERE student_no=?', [String(r.student_no).trim()]);
                    r.student_name = r.student_name || (stu.length > 0 ? stu[0].name : r.student_no);
                }
            }
            r.old_score = r.old_score !== null ? r.old_score : '无';
        }
        res.json({ code: 'OK', list: reqs });
    } catch (e) {
        res.status(500).json({ code: 'SERVER_ERROR', message: e.message });
    }
});

app.post('/api/approver/review-change', async (req, res) => {
    // 只有院长 / 教务主任 / 管理员可以审批改分，且只能审批本学院的预约
    const token = req.signedCookies && req.signedCookies[COOKIE_NAME];
    let me = null;
    try { me = token ? JSON.parse(Buffer.from(token, 'base64').toString('utf8')) : null; } catch (e) { me = null; }
    if (!me || !ROLES[me.role]) return res.status(401).json({ code: 'NOT_LOGIN', message: '请先登录' });
    if (!['dean', 'director', 'admin'].includes(me.role)) return res.status(403).json({ code: 'FORBIDDEN', message: '只有院长或教务主任可以审批改分申请' });
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        const [reqs] = await conn.query('SELECT * FROM score_change_requests WHERE id=? AND status="pending" FOR UPDATE', [req.body.requestId]);
        if (reqs.length === 0) { await conn.rollback(); return res.status(404).json({ code: 'NOT_FOUND', message: '申请不存在或已处理' }); }
        const r = reqs[0];
        const bId = String(r.booking_id).trim();
        // 学院归属校验：防止跨学院审批
        if (me.role !== 'admin') {
            const [bcol] = await conn.query('SELECT college FROM bookings WHERE TRIM(id)=? LIMIT 1', [bId]);
            const [meRow] = await conn.query(`SELECT college FROM ${ROLES[me.role].table} WHERE ${ROLES[me.role].idField}=? LIMIT 1`, [me.id]);
            if (bcol.length === 0) { await conn.rollback(); return res.status(404).json({ code: 'NOT_FOUND', message: '预约不存在' }); }
            if (!meRow.length || String(bcol[0].college || '') !== String(meRow[0].college || '')) {
                await conn.rollback(); return res.status(403).json({ code: 'FORBIDDEN', message: '只能审批本学院的改分申请' });
            }
        }
        if (req.body.action === 'approve') {
            if (r.new_score === null || r.new_score === undefined || r.new_score === '') { await conn.rollback(); return res.status(400).json({ code: 'BAD_REQUEST', message: '申请里的新分数为空，无法通过' }); }
            const newScore = Number(r.new_score);
            if (!isFinite(newScore)) { await conn.rollback(); return res.status(400).json({ code: 'BAD_REQUEST', message: '新分数不是有效数字' }); }
            const sNo = String(r.student_no).trim();
            const [upd] = await conn.query(`UPDATE group_scores SET score=?, comment=? WHERE TRIM(booking_id)=? AND TRIM(student_no)=?`, [newScore, r.new_comment || '', bId, sNo]);
            // 原打分记录缺失时补一条，避免“审批通过但分数没变”
            if (upd.affectedRows === 0) {
                await conn.query('INSERT INTO group_scores (booking_id, student_no, student_name, score, comment) VALUES (?,?,?,?,?)', [bId, sNo, r.student_name || r.student_no, newScore, r.new_comment || '']);
            }
            const [scores] = await conn.query('SELECT score FROM group_scores WHERE TRIM(booking_id)=?', [bId]);
            if (scores.length > 0) {
                // score 是 decimal，mysql2 返回字符串，必须转数字后再求和，否则会拼成 NaN
                const total = scores.reduce((a, b) => a + (Number(b.score) || 0), 0);
                const avg = Math.round(total / scores.length);
                await conn.query('UPDATE bookings SET score=? WHERE TRIM(id)=?', [avg, bId]);
            }
        }
        await conn.query(`UPDATE score_change_requests SET status=?, review_comment=?, review_at=NOW(), reviewer_id=? WHERE id=?`, [req.body.action === 'approve' ? 'approved' : 'rejected', req.body.comment || '', me.id, req.body.requestId]);
        await conn.commit();
        addLog(req, '审批改分', `${req.body.action === 'approve' ? '通过' : '拒绝'} · 申请#${req.body.requestId}`);
        res.json({ code: 'OK', message: '审批完成' });
    } catch (e) {
        await conn.rollback();
        console.error('❌ 改分审批失败：', e.sqlMessage || e.message);
        res.status(500).json({ code: 'SERVER_ERROR', message: dbErrorMessage(e) });
    } finally {
        conn.release();
    }
});

app.get('/api/quotes/random', async (req, res) => { try { const [rows] = await pool.query('SELECT content, author FROM quotes ORDER BY RAND() LIMIT 1'); res.json({ code: 'OK', quote: rows[0] || { content: '欢迎使用', author: '系统' } }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });
app.get('/api/admin/quotes', async (req, res) => { if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' }); try { const [rows] = await pool.query('SELECT * FROM quotes ORDER BY id DESC'); res.json({ code: 'OK', list: rows }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });
app.post('/api/admin/quotes/add', async (req, res) => { if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' }); const content = String(req.body.content || '').trim(); if (!content) return res.status(400).json({ code: 'BAD_REQUEST', message: '内容不能为空' }); try { await pool.query('INSERT INTO quotes (content, author) VALUES (?, ?)', [content, String(req.body.author || '佚名')]); notifyQuoteUpdate(); res.json({ code: 'OK', message: '添加成功' }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });
app.post('/api/admin/quotes/delete', async (req, res) => { if (!getAdminIdFromReq(req)) return res.status(403).json({ code: 'FORBIDDEN', message: '无权访问' }); try { await pool.query('DELETE FROM quotes WHERE id = ?', [req.body.id]); notifyQuoteUpdate(); res.json({ code: 'OK', message: '删除成功' }); } catch (e) { res.status(500).json({ code: 'SERVER_ERROR', message: e.message }); } });

// 直播结束后超过 SUBMIT_GRACE_DAYS 天仍未提交网盘 → 标记逾期（不封号，只是暂停预约）
async function autoSubmitAfterLive() {
    try {
        await pool.query(`UPDATE bookings SET upload_status = 'overdue', late_flag = 1
                          WHERE booking_status = 'booked' AND upload_status IN ('pending_live', 'pending_upload')
                            AND CONCAT(date, ' ', end_time) < DATE_SUB(NOW(), INTERVAL ${SUBMIT_GRACE_DAYS} DAY)`);
    } catch (e) { }
}
autoSubmitAfterLive(); setInterval(autoSubmitAfterLive, 60000);

app.listen(PORT, () => { console.log('✅ 服务已启动: http://localhost:' + PORT); });