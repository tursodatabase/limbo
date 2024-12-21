import { drizzle } from 'drizzle-orm/better-sqlite3';
import * as s from 'drizzle-orm/sqlite-core';
import { Database } from 'limbo-wasm';

const db = new Database('sqlite.db', { useOPFS: true });
const users = s.sqliteTable("users", {
  id: s.integer(),
  name: s.text(),
})
const result = db.select().from(users).all();
console.log(result);
