import os
import click
import csv
import sqlite3
from sqlite3.dbapi2 import Connection
import requests
import mimetypes
from urllib.parse import urljoin, urlparse
from lxml.html.soupparser import fromstring
from lxml import etree
from lxml.etree import tostring
from analysis import lmdict, tone_count_with_negation_check
from parser import parse_text

@click.command()
@click.option('-s','--batch-size', 'batch_size', default=50)
def analyze(batch_size):
  db = db_connect()
  db_ensure_init(db)

  cmd = db.execute("SELECT id, url FROM reports WHERE is_analyzed = 0")
  for batch in iter(lambda: cmd.fetchmany(batch_size), []):
    to_update = list()
    for r in batch:
      print("Analyzing: " + r[1])
      response = requests.get(r[1])

      text = parse_text(response.text)
      print(text[0:400] + '\n[CLIPPED]')

      # perform text analysis
      result = tone_count_with_negation_check(lmdict, text)

      has_positive_sentiment = result[1] > result[2]

      # TODO: FIXME
      # Here you should pass in all the variables that you want to store in the database
      # Refer to "db_update" method in what order params should be passed
      to_update.append((
        True,
        has_positive_sentiment,
        result[0],
        result[1],
        result[2],
        " ".join(result[3]),
        " ".join(result[4]),
        r[0]))

    db_update(db, to_update)


@click.command()
@click.argument('start', nargs=1)
@click.argument('end', nargs=1)
@click.option('-s','--batch-size', 'batch_size', default=50)
def fetch_report_urls(start, end, batch_size):
  """Fetches and stores the 10-K report URLs"""
  db = db_connect()
  db_ensure_init(db)

  with open('log.csv', 'w', newline='') as log:
    logwriter = csv.writer(log)

    cmd = db.execute("""
      SELECT ix.id, ix.conm, ix.type, ix.cik, ix.date, ix.path
      FROM "index" ix
      LEFT JOIN reports r ON ix.id = r.index_id
      WHERE ix.type = '10-K' AND r.id IS NULL AND
        CAST(strftime('%Y', DATE(ix.date)) as INT) >= {start} AND
        CAST(strftime('%Y', DATE(ix.date)) as INT) <= {end}
      ORDER BY ix.date DESC
    """.format(start=start, end=end))

    for batch in iter(lambda: cmd.fetchmany(batch_size), []):
      to_insert = list()
      for r in batch:
        # print(r)
        log_row = r

        response = requests.get(r[5])
        href = parse_href(response.content)
        url = fix_url(href, r[5])
        print(url)

        filetype = mimetypes.guess_type(url)[0]
        print(filetype)

        filename = os.path.basename(urlparse(url).path)
        print(filename)

        to_insert.append((r[0], r[1], r[2], r[3], r[4], url, filetype, filename))

        logwriter.writerow(log_row)

      db_insert(db, to_insert)

def parse_href(html_content):
  # print(html_content)
  root = to_doc(html_content)
  # f = open("debug_idx.html", "wb")
  # f.write(tostring(root, pretty_print=True))
  # f.close()
  elements = root.xpath('(//div[@id="formDiv"]//table//tr[2]/td[3]/a)')

  if len(elements) == 0:
    raise Exception("Unable to parse URL from index page")

  href = elements[0].get('href')
  return href

def fix_url(href, base_url):
  # if the url links to an interactive iXBRL adjust the URL to link to the normal html
  # eg. https://www.sec.gov/ix?doc=/Archives/edgar/data/1018840/000101884020000094/anf-20201031.htm
  # -> https://www.sec.gov/Archives/edgar/data/1018840/000101884020000094/anf-20201031.htm
  path = href.replace('ix?doc=/', '')
  # a relative url needs to be joined with the base url
  url = urljoin(base_url, path)
  return url

def to_doc(content):
  # Try to parse as XML/XHTML and fallback to soupparser
  try:
    doc = etree.fromstring(content)
  except:
    doc = fromstring(content)

  return doc

def db_connect():
  db = sqlite3.connect('edgar_htm_idx.sqlite3')
  return db

def db_insert(db: Connection, records):
  c = db.cursor()
  c.executemany("INSERT INTO reports(index_id, conm, type, cik, date, url, filetype, filename) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", records)
  db.commit()

def db_update(db: Connection, records):
  c = db.cursor()
  c.executemany("""
    UPDATE reports SET
    is_analyzed = ?,
    has_positive_sentiment = ?,
    word_count = ?,
    pos_count = ?,
    neg_count = ?,
    pos_words = ?,
    neg_words = ?
    where id = ?""", records)
  db.commit()

def db_ensure_init(db: Connection):
  cur = db.cursor()
  # TODO: FIXME add any new columns you want to store in the database
  cur.execute("""CREATE TABLE IF NOT EXISTS "reports" (
    "id"	INTEGER NOT NULL,
    "index_id" INTEGER UNIQUE,
    "conm" TEXT,
    "type" TEXT,
    "cik" TEXT,
    "date" TEXT,
    "url"	TEXT,
    "filetype"	TEXT,
    "filename"	TEXT,
    "is_analyzed"	INTEGER DEFAULT 0,
    "has_positive_sentiment" INTEGER,
    "word_count" INTEGER,
    "pos_count" INTEGER,
    "neg_count" INTEGER,
    "pos_words" TEXT,
    "neg_words" TEXT,
    PRIMARY KEY("id" AUTOINCREMENT)
    FOREIGN KEY (index_id) REFERENCES "index"(id)
  );""")


@click.group()
def cli():
  pass

cli.add_command(fetch_report_urls)
cli.add_command(analyze)

if __name__ == '__main__':
  cli()
