'use strict';
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const pair = (a, b) => [a, b].sort();
class UserStore {
  constructor() { this.tail = Promise.resolve(); }
  async open() {
    if (process.env.DATABASE_URL) {
      const { Pool } = require('pg');
      this.pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 4,
        ssl: process.env.PG_SSL === 'require' ? { rejectUnauthorized: true } : undefined });
      this.kind = 'postgres';
    } else {
      const { DatabaseSync } = require('node:sqlite');
      const location = process.env.DB_PATH || path.join(__dirname, 'data', 'users.sqlite');
      fs.mkdirSync(path.dirname(location), { recursive: true });
      this.sqlite = new DatabaseSync(location);
      this.sqlite.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;');
      this.kind = 'sqlite';
      if (process.env.RENDER) console.warn('SQLite requires a Render persistent disk. Set DATABASE_URL for external PostgreSQL.');
    }
    await this.serial(async q => {
      await q(`CREATE TABLE IF NOT EXISTS mp_users (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, name TEXT NOT NULL, created_at BIGINT NOT NULL)`);
      await q(`CREATE TABLE IF NOT EXISTS mp_buddies (a TEXT NOT NULL REFERENCES mp_users(id) ON DELETE CASCADE, b TEXT NOT NULL REFERENCES mp_users(id) ON DELETE CASCADE, PRIMARY KEY(a,b), CHECK(a<b))`);
      await q(`CREATE INDEX IF NOT EXISTS mp_buddies_b ON mp_buddies(b)`);
      await q(`CREATE TABLE IF NOT EXISTS mp_requests (id TEXT PRIMARY KEY, sender TEXT NOT NULL REFERENCES mp_users(id) ON DELETE CASCADE, recipient TEXT NOT NULL REFERENCES mp_users(id) ON DELETE CASCADE, created_at BIGINT NOT NULL, UNIQUE(sender,recipient), CHECK(sender<>recipient))`);
      await q(`CREATE INDEX IF NOT EXISTS mp_requests_recipient ON mp_requests(recipient)`);
    });
  }
  serial(fn) {
    const job = this.tail.then(async () => {
      const client = this.pool ? await this.pool.connect() : null;
      const q = async (sql, values = []) => {
        if (client) return (await client.query(sql, values)).rows;
        const args = [];
        const translated = sql.replace(/\$(\d+)/g, (_, i) => { args.push(values[Number(i)-1]); return '?'; });
        return this.sqlite.prepare(translated).all(...args);
      };
      try { await q('BEGIN'); const result = await fn(q); await q('COMMIT'); return result; }
      catch (error) { try { await q('ROLLBACK'); } catch {} throw error; }
      finally { if (client) client.release(); }
    });
    this.tail = job.catch(() => {});
    return job;
  }
  login(token, name) {
    return this.serial(async q => {
      let rows = typeof token === 'string' && /^[a-f0-9]{64}$/.test(token) ? await q('SELECT id,name FROM mp_users WHERE token_hash=$1', [hash(token)]) : [];
      if (rows.length) {
        await q('UPDATE mp_users SET name=$1 WHERE id=$2', [name, rows[0].id]);
        return { id: rows[0].id, name, token };
      }
      const id = crypto.randomUUID(); token = crypto.randomBytes(32).toString('hex');
      await q('INSERT INTO mp_users(id,token_hash,name,created_at) VALUES($1,$2,$3,$4)', [id,hash(token),name,Date.now()]);
      return { id,name,token };
    });
  }
  rename(id,name) { return this.serial(q => q('UPDATE mp_users SET name=$1 WHERE id=$2',[name,id])); }
  list(id) {
    return this.serial(q => q('SELECT u.id,u.name FROM mp_buddies f JOIN mp_users u ON u.id=CASE WHEN f.a=$1 THEN f.b ELSE f.a END WHERE f.a=$1 OR f.b=$1 ORDER BY u.name,u.id',[id]));
  }
  incoming(id) {
    return this.serial(q => q('SELECT r.id AS request_id,r.sender AS user_id,u.name FROM mp_requests r JOIN mp_users u ON u.id=r.sender WHERE r.recipient=$1 ORDER BY r.created_at LIMIT 32',[id]));
  }
  request(from,to) {
    return this.serial(async q => {
      if (from===to || !(await q('SELECT id FROM mp_users WHERE id=$1',[to])).length) return {error:'User unavailable.'};
      const [a,b]=pair(from,to);
      if ((await q('SELECT a FROM mp_buddies WHERE a=$1 AND b=$2',[a,b])).length) return {error:'Already buddies.'};
      if ((await q('SELECT id FROM mp_requests WHERE (sender=$1 AND recipient=$2) OR (sender=$2 AND recipient=$1)',[from,to])).length) return {error:'A buddy request is already pending.'};
      const [{n}]=await q('SELECT COUNT(*) AS n FROM mp_requests WHERE recipient=$1',[to]);
      if (Number(n)>=32) return {error:'That player has too many pending requests.'};
      const id=crypto.randomUUID();
      await q('INSERT INTO mp_requests(id,sender,recipient,created_at) VALUES($1,$2,$3,$4)',[id,from,to,Date.now()]);
      return {request_id:id,user_id:from};
    });
  }
  respond(recipient,id,accept) {
    return this.serial(async q => {
      const rows=await q('SELECT sender,recipient FROM mp_requests WHERE id=$1 AND recipient=$2',[id,recipient]);
      if (!rows.length) return null;
      const from=rows[0].sender;
      if (accept) {
        const [a,b]=pair(from,recipient);
        const [{n}]=await q('SELECT COUNT(*) AS n FROM mp_buddies WHERE a=$1 OR b=$1 OR a=$2 OR b=$2',[from,recipient]);
        if(Number(n)>=1000) throw new Error('Buddy limit reached.');
        await q('INSERT INTO mp_buddies(a,b) VALUES($1,$2) ON CONFLICT(a,b) DO NOTHING',[a,b]);
      }
      await q('DELETE FROM mp_requests WHERE (sender=$1 AND recipient=$2) OR (sender=$2 AND recipient=$1)',[from,recipient]);
      return from;
    });
  }
  remove(id,other) { const [a,b]=pair(id,other); return this.serial(q=>q('DELETE FROM mp_buddies WHERE a=$1 AND b=$2',[a,b])); }
  clearBuddies() { return this.serial(async q=> { await q('DELETE FROM mp_requests'); await q('DELETE FROM mp_buddies'); }); }
  clearUsers() { return this.serial(q=>q('DELETE FROM mp_users')); }
  async close() { await this.tail; if(this.pool) await this.pool.end(); else this.sqlite.close(); }
}
module.exports = {UserStore};
