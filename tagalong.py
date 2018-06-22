#!/usr/bin/env python3

import sys
import os
import hashlib
import sqlite3
import re
import uuid
from datetime import date, datetime

pattern_dated_dir = re.compile('^\d{4}/\d{2}/\d{2}/')


def kv_set(c, k, v):
    c.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (k, str(v)))


def kv_get(c: sqlite3.Cursor, k):
    c.execute('SELECT value FROM kv WHERE key = ?', (k, ))
    return c.fetchone()[0]


def main(args):
    dbpath = args[1]
    dbexists = os.path.exists(dbpath)

    sql = sqlite3.connect(dbpath)

    c = sql.cursor()

    if not dbexists:
        print("Init DB")

        c.execute('CREATE TABLE kv (key text PRIMARY KEY, value text) ')
        kv_set(c, 'db.version', 0)

        c.execute('CREATE TABLE fileinfo (hash text PRIMARY KEY, path text, size int) ')

    getversion = lambda: int(kv_get(c, 'db.version'))

    if getversion() == 0:
        c.execute(
            "CREATE TABLE document ( "
            "  uuid TEXT PRIMARY KEY NOT NULL, "
            "  date_registered INT NOT NULL, "
            "  date_created INT "
            ")"
        )
        kv_set(c, 'db.version', 1)

    if getversion() == 1:
        c.execute(
            "CREATE TABLE document_file ( "
            "  document_uuid TEXT NOT NULL, "
            "  page INT NOT NULL, "
            "  file_hash TEXT NOT NULL, "
            "  CONSTRAINT pk PRIMARY KEY (document_uuid, page), "
            "  CONSTRAINT document_file_document_uuid_fk FOREIGN KEY (document_uuid) "
            "    REFERENCES document (uuid) ON DELETE CASCADE ON UPDATE CASCADE "
            ")"
        )
        kv_set(c, 'db.version', 2)

    if getversion() == 2:
        c.execute(
            "CREATE TABLE document_tag ( "
            "  document_id TEXT NOT NULL, "
            "  tag TEXT NOT NULL, "
            "  CONSTRAINT pk PRIMARY KEY (document_id, tag), "
            "  CONSTRAINT document_tag_document_id_fk FOREIGN KEY (document_id) "
            "    REFERENCES document (uuid) ON DELETE CASCADE ON UPDATE CASCADE "
            ")"
        )
        kv_set(c, 'db.version', 3)

    sql.commit()
    c.close()

    root = args[2]
    scandir(root, sql)
    autosort(sql)


def scandir(root: str, sql: sqlite3.Connection):
    c = sql.cursor()
    for dirpath, dirs, files in os.walk(root):
        d = os.path.relpath(dirpath, root)
        for f in files:
            rf = os.path.join(dirpath, f)
            relpath = os.path.join(d, f)
            size = os.stat(rf).st_size
            hsah = hashfile(rf)
            print(relpath, hsah, size)
            c.execute('INSERT OR REPLACE INTO fileinfo VALUES(?,?,?)', (hsah, relpath, size))
    c.close()
    sql.commit()


def autosort(sql: sqlite3.Connection):
    print("AUTOSORT")
    c = sql.cursor()

    c.execute('DELETE FROM document')
    c.execute('DELETE FROM document_file')

    c.execute(
            "SELECT hash, path "
            "FROM fileinfo "
            "WHERE hash NOT IN (SELECT file_hash FROM document_file) "
    )
    for hsah, path in c.fetchall():
        print(hsah, path)
        if pattern_dated_dir.match(path):
            sp = path.split('/')
            d = datetime(*[int(x) for x in sp[:3]])
            spp = sp[3:]

            docname = spp[0].partition('.')[0]
            dockey = '-'.join(sp[:3]) + '/' + docname

            print(d, dockey)

            c.execute('INSERT OR REPLACE INTO document VALUES (?, ?, ?)', (dockey, datetime.today(), d))

            afzender = docname.partition('_')[0]
            c.execute('INSERT OR REPLACE INTO document_tag VALUES (?, ?)', (dockey, 'afzender:' + afzender))

            if len(spp) == 1:
                page = 0
            else:
                page = int(spp[1].partition('.')[0])

            c.execute('INSERT INTO document_file VALUES (?,?,?)', (dockey, page, hsah))
    c.close()
    sql.commit()

def hashfile(filename):
    h = hashlib.sha256()
    with open(filename, 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128 * 1024), b''):
            h.update(b)
    return h.hexdigest()


if __name__ == '__main__':
    main(sys.argv)
