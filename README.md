# Mad Boredom SQL  🧸

Boring Writing Just SQL SCRIPT To Scan Database and Truncate & Delete
Extent Using Python To write the Automate Script.

This Python App Using Poetry And Cross Platfrom Now Support MS SQL & Postgres.

- 🌀 Poetry - Project & dependency manager
- 🐘 PostgreSQL — Supported DB
- 🏢 MS SQL Server — Supported DB
- 🧪 SQLAlchemy — Database ORM/engine handling
- 🗂️  Pandas — Generate Report
- 🔎 Database Scanning — Automate detects tables, excluding system schemas.
- 📝 Exception List — Skip specific tables you don’t want touched.
- 🧹Smart Cleanup —
   - If a table has your filter column (AccountId by default), it runs filtered deletes.
   - Otherwise, it safely truncates the table.
- ⚙️ Config Driven — Manage DB connections and cleanup behavior via config.py.

