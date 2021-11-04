import time
import queue
import socket
import argparse
import threading
import collections
import pandas as pd

import urllib.robotparser
# Google
from googlesearch import search
from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urljoin, quote
from http.client import RemoteDisconnected, InvalidURL


# Parsing for hyperlinks
class LinksExtractor(HTMLParser):
    def error(self, message):
        pass

    def __init__(self):
        HTMLParser.__init__(self)
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            if len(attrs) > 0:
                for attr in attrs:
                    if attr[0] == "href":  # ignore all non HREF attributes
                        self.links.append(attr[1])  # save the link info in the list

    def get_links(self):  # return the list of extracted links
        return self.links


class Crawler:
    def __init__(self, query, num, num_of_threads):
        # Ignore these kinds of files
        self.black_list = ['asp', 'jpg', 'png', 'jpeg',
                           'pdf', 'cgi', 'svg', 'mp3',
                           'mp4', 'avi', 'aspx', 'mpeg',
                           'pptx', 'ppt', 'doc', 'docx',
                           'csv', 'jsp', 'gif']
        # Visited URLs
        self.seenurl = collections.defaultdict(bool)
        # Results for logging
        self.results = collections.defaultdict(list)
        self.num_of_threads = num_of_threads
        self.query = query
        self.threads_pool = []
        # Count crawled pages
        self.PAGE_COUNT = 0
        # Start time
        self.st = time.time()
        self.MAX_NUM_PAGES = num
        # Cache for robots.txt
        self._rp = {}
        # FIFO queue for URLs
        # Each element is a two-tuple (depth, url)
        self.q = queue.Queue()

        for imp, url in enumerate(search(query, num_results=10)):
            url_struct = urlparse(url)
            if url_struct.scheme in ['http', 'https']:
                self.q.put((0, quote(url, safe="%/:=&?~#+!$,;'@()*[]")))

    # Implement Robot Exclusion Protocol
    def RobotcanFetch(self, url):
        url_struct = urlparse(url)
        base = url_struct.netloc
        # Look up in the cache or update it
        if base not in self._rp:
            self._rp[base] = urllib.robotparser.RobotFileParser()
            self._rp[base].set_url(url_struct.scheme + "://" + base + "/robots.txt")
            try:
                urlopen(url_struct.scheme + "://" + base + "/robots.txt", timeout=0.5)
                self._rp[base].read()
            except (RemoteDisconnected, InvalidURL, ValueError, HTTPError, URLError, ConnectionResetError, socket.timeout):
                return False
        return self._rp[base].can_fetch('*', url)

    # Dispatch workers
    def dispatch(self):
        while PAGE_COUNT < self.MAX_NUM_PAGES:
            tp = self.q.get()
            depth, url = tp

            filetype_1 = url.split('.')[-1]
            filetype_2 = (url.split('?')[-2]).split('.')[-1] if '?' in url else ''

            if self.seenurl[url] or not self.RobotcanFetch(url) \
                    or filetype_1 in self.black_list or filetype_2 in self.black_list:
                self.seenurl[url] = True
                self.q.task_done()
            else:
                # Maintain a limited thread pool
                if len(self.threads_pool) > self.num_of_threads:
                    self.threads_pool = self.threads_pool[:self.num_of_threads - 5]
                worker = FetchandParse(st=self.st,
                                       tp=tp,
                                       q=self.q,
                                       results=self.results,
                                       seenurl=self.seenurl)
                worker.start()
                self.threads_pool.append(worker)

                for worker in self.threads_pool:
                    if not worker.is_alive():
                        self.threads_pool.remove(worker)
        for worker in self.threads_pool:
            worker.join()

    def log(self):
        # Dump to CSV
        print('seenurl:', len(self.seenurl))
        data_frame = pd.DataFrame(data=self.results, index=range(0, len(self.results['url'])))
        data_frame.to_csv('log_naive_{}.csv'.format(self.query), index_label='index')


class FetchandParse(threading.Thread):
    def __init__(self, st, tp, q, results, seenurl):
        threading.Thread.__init__(self)
        self.st = st
        self.depth, self.url = tp
        self.q = q
        self.results = results
        self.seenurl = seenurl
        self.parser = LinksExtractor()

    def run(self):
        global PAGE_COUNT
        try:
            with urlopen(self.url, timeout=3.0) as response:
                size = response.headers.get('content-length')
                code = response.getcode()
                # print(f"The URL {response.geturl()} crawled with status {code}")
                if response.headers.get_content_type() == "text/html" and code == 200:
                    parser = LinksExtractor()
                    parser.feed(response.read().decode(response.headers.get_content_charset() or 'utf-8'))
                    raw_links = parser.get_links()
                    links = []
                    for ref in raw_links:
                        if not ref: continue
                        ref = quote(ref, safe="%/:=&?~#+!$,;'@()*[]")
                        # Join relative paths if necessary
                        if ref.startswith('#'): continue
                        ref = ref if urlparse(ref).scheme in ['http', 'https'] else urljoin(self.url, ref)
                        if not self.seenurl[ref] and ref not in links:
                            links.append(ref)

                    for ref in links[:100]:
                        self.q.put((self.depth + 1, ref))

                    # Add to log
                    self.results['url'].append(self.url)
                    self.results['Size in bytes'].append(size)
                    self.results['Return code'].append(code)
                    self.results['Depth'].append(self.depth)
                    cur_time = time.strftime("%m-%d-%Y_%H-%M-%S")
                    self.results['Time'].append(cur_time)
                    # Update
                    self.seenurl[self.url] = True

                    PAGE_COUNT += 1
                    print(PAGE_COUNT, PAGE_COUNT / (time.time()-self.st))
        except (RemoteDisconnected, InvalidURL, ValueError, HTTPError, URLError, ConnectionResetError, socket.timeout, IndexError):
            pass
        finally:
            self.q.task_done()


parser = argparse.ArgumentParser()
parser.add_argument('-q', type=str, default='brooklyn union', help='query')
parser.add_argument('-p', type=int, default=10000, help='maximum page number')
parser.add_argument('-t', type=int, default=50, help='maximum thread number')
opt = parser.parse_args()

PAGE_COUNT = 0

if __name__ == '__main__':
    crawler = Crawler(opt.q, opt.p, opt.t)
    crawler.dispatch()
    crawler.log()
