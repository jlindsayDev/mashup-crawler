# XML Sitemap Crawler

import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path
import time
import datetime


DB_FILE = 'crawled_urls.db'
ZIP_DIR = 'zips/'
ARCHIVE_TYPES = tuple(['.zip', '.7z', '.rar', '(Full%20Mix).mp3'])
NON_ARCHIVAL_LINKS = ['Name', 'Last modified', 'Date', 'Size', 'Description']
SLEEP_TIME_MS = 100


def xml_tag_name(tag):
  end_of_namespace = tag.rfind('}') + 1
  return tag[end_of_namespace:]


def request_url(url, cur):
  response = requests.get(url)
  print(f'[{response.status_code}] {response.url}')
  time.sleep(SLEEP_TIME_MS / 1000)

  if 'text/xml' in response.headers['Content-Type']:
    nodes = []

    for node in ET.fromstring(response.text):
      url_node = {xml_tag_name(n.tag): n.text for n in node}
      nodes.append({'url': url_node['loc'], 'last_modified': url_node.get('lastmod')})

    with con:
      cur = con.cursor()
      cur.executemany('INSERT OR IGNORE INTO url VALUES (:url, :last_modified)', nodes)

    return nodes

  elif 'text/html' in response.headers['Content-Type']:
    links = []
    descend = []

    soup = BeautifulSoup(response.text, 'html.parser')
    # is_index_page = soup.title.string.startswith('Index of')

    for a in soup.find_all('a'):
      if not a.get('href'):
        continue

      if a['href'].endswith(ARCHIVE_TYPES):
        url = requests.compat.urljoin(response.url, a['href'])
        links.append({'url': url})
      elif a['href'].startswith('https://bootiemashup.com') and a['href'].endswith('/'):
        url = requests.compat.urljoin(response.url, a['href'])
        descend.append({'url': url})

    if len(links):
      print(f'\t↳ Found {len(links)} links')

    # with con:
    #   cur = con.cursor()
    #   cur.executemany('INSERT OR IGNORE INTO to_download VALUES (:url)', links)

    return links + descend


def main(con, urls):
  visited = set([
    'https://bootiemashup.com/page-sitemap.xml',
    'https://bootiemashup.com/parties-sitemap.xml',
    'https://bootiemashup.com/product-sitemap.xml',
  ])

  while len(urls):
    url = urls.pop()
    if url in visited or url.endswith('/'):
      continue
    visited.add(url)

    if url.endswith(ARCHIVE_TYPES):
      with con:
        cur = con.cursor()
        cur.execute('INSERT OR IGNORE INTO to_download VALUES (:url)', {'url': url})
    else:
      links = request_url(url, con)
      urls.update([l['url'] for l in links])


# https://docs.python.org/3/library/sqlite3.html#adapter-and-converter-recipes
def adapt_datetime_iso(val):
  """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
  if val is None:
    return None
  return val.isoformat()

def convert_datetime(val):
  """Convert ISO 8601 datetime to datetime.datetime object."""
  if val is None:
    return None
  return datetime.datetime.fromisoformat(val.decode())


if __name__ == "__main__":
  Path(ZIP_DIR).mkdir(parents=True, exist_ok=True)

  urls = set([
    'https://bootiemashup.com/sitemap_index.xml',
    # 'https://bootiemashup.com/best-of-bootie/2020'
    # 'https://bootiemashup.com/themealbumfiles/',
    # 'https://bootiemashup.com/disneymashed/',
    # 'https://bootiemashup.com/top10files',
  ])

  sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
  sqlite3.register_converter("datetime", convert_datetime)

  con = sqlite3.connect(DB_FILE)
  with con:
    cur = con.cursor()
    cur.execute(f'CREATE TABLE IF NOT EXISTS to_download (url TEXT PRIMARY KEY)')
    cur.execute(f'CREATE TABLE IF NOT EXISTS url (url TEXT PRIMARY KEY, last_modified DATETIME)')

  main(con, urls)

  con.close()
