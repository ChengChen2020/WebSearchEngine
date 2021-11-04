import time
import heapq
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


# Parsing for HREF hyperlinks
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
                    if attr[0] == "href":  # Ignore non HREF attributes
                        self.links.append(attr[1])  # Save the link

    def get_links(self):  # Return the list of extracted links
        return self.links


class Crawler:
    def __init__(self, query, num, num_of_threads):
        """
            Novelty is always positive and increasing
            Importance is always negative and decreasing
        """
        # Ignore these kinds of files
        self.black_list = ['asp', 'jpg', 'png', 'jpeg',
                           'pdf', 'cgi', 'svg', 'mp3',
                           'mp4', 'avi', 'aspx', 'mpeg',
                           'pptx', 'ppt', 'doc', 'docx',
                           'csv', 'jsp', 'gif']
        # Novelty of domains
        self.novelty = collections.defaultdict(int)
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
        # Priority queue for URLs
        # Each element is a six-tuple (score, depth, importance, novelty, url, domain)
        self.q = queue.PriorityQueue()

        # Different version's search may have different argument names
        # On Colab, it should be `search(query, stop=10)`
        for imp, url in enumerate(search(query, num_results=10)):
            url = quote(url, safe="%/:=&?~#+!$,;'@()*[]")
            url_struct = urlparse(url)
            if url_struct.scheme in ['http', 'https']:
                # Extract domain
                domain = '.'.join(url_struct.netloc.split('.')[-2:])
                self.novelty[domain] = 1
                self.q.put((imp - 21 + self.novelty[domain],  # score
                            0,  # depth
                            imp - 21,  # importance
                            self.novelty[domain],  # novelty
                            url,
                            domain)
                           )

    # Implement Robot Exclusion Protocol
    def RobotcanFetch(self, url):
        url_struct = urlparse(url)
        base = url_struct.netloc
        # Look up in the cache or update it
        if base not in self._rp:
            self._rp[base] = urllib.robotparser.RobotFileParser()
            self._rp[base].set_url(url_struct.scheme + "://" + base + "/robots.txt")
            try:
                urlopen(url_struct.scheme + "://" + base + "/robots.txt", timeout=1.)
                self._rp[base].read()
            except (RemoteDisconnected, InvalidURL, ValueError, HTTPError,
                    URLError, ConnectionResetError, socket.timeout,
                    IndexError, LookupError):
                return False
        return self._rp[base].can_fetch('*', url)

    # Dispatch workers
    def dispatch(self):
        while PAGE_COUNT < self.MAX_NUM_PAGES:
            tp = self.q.get()
            score, depth, imp, nov, url, domain = tp
            # Check if a page should be crawled
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
                                       seenurl=self.seenurl,
                                       novelty=self.novelty)
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
        print('novelty:', len(self.novelty))
        data_frame = pd.DataFrame(data=self.results, index=range(0, len(self.results['url'])))
        data_frame.to_csv('log_{}.csv'.format(self.query), index_label='index')


class FetchandParse(threading.Thread):
    def __init__(self, st, tp, q, results, seenurl, novelty):
        threading.Thread.__init__(self)
        self.start_time = st
        self.score, self.depth, self.imp, self.nov, self.url, self.domain = tp
        self.q = q
        self.results = results
        self.seenurl = seenurl
        self.novelty = novelty
        self.parser = LinksExtractor()

    def run(self):
        global PAGE_COUNT
        try:
            with urlopen(self.url, timeout=3.0) as response:
                size = response.headers.get('content-length')
                code = response.getcode()
                print(f"The URL {response.geturl()} crawled with status {code}")
                if response.headers.get_content_type() == "text/html" and code == 200:

                    self.parser.feed(response.read().decode(response.headers.get_content_charset() or 'utf-8'))

                    # Increase the novelty of the domain
                    self.novelty[self.domain] += 10

                    raw_links = self.parser.get_links()
                    links = []
                    for ref in raw_links:
                        if not ref: continue
                        ref = quote(ref, safe="%/:=&?~#+!$,;'@()*[]")
                        # Join relative paths if necessary
                        if ref.startswith('#'): continue
                        ref = ref if urlparse(ref).scheme in ['http', 'https'] else urljoin(self.url, ref)
                        if not self.seenurl[ref] and ref not in links:
                            links.append(ref)

                    # tp = (score, depth, importance, novelty, url, domain)
                    # IMPORTANCE -= 1
                    for i, tp in enumerate(self.q.queue):
                        if tp[5] == self.domain:
                            self.q.queue[i] = (tp[2] + self.novelty[tp[5]],
                                               tp[1],
                                               tp[2],
                                               self.novelty[tp[5]],
                                               tp[4],
                                               tp[5]
                                               )
                        if tp[4] in links:
                            self.q.queue[i] = (tp[2] - 1 + tp[3],
                                               tp[1],
                                               tp[2] - 1,
                                               tp[3],
                                               tp[4],
                                               tp[5]
                                               )
                            links.remove(tp[4])

                    # Initially IMPORTANCE == -1
                    # Initially NOVELTY == DEPTH + 1
                    for ref in links[:100]:
                        rdomain = '.'.join(urlparse(ref).netloc.split('.')[-2:])
                        rnov = self.novelty[rdomain] or self.depth + 1
                        self.novelty[rdomain] = rnov
                        self.q.put((rnov - 1,
                                    self.depth + 1,
                                    -1,
                                    rnov,
                                    ref,
                                    rdomain)
                                   )

                    # Sort
                    heapq.heapify(self.q.queue)
                    if len(self.q.queue) > 200:
                        self.q.queue = self.q.queue[:200]

                # Add to log
                self.results['url'].append(self.url)
                self.results['Size in bytes'].append(size)
                self.results['Return code'].append(code)
                self.results['Depth'].append(self.depth)
                self.results['Priority score'].append(self.score)
                cur_time = time.strftime("%m-%d-%Y_%H-%M-%S")
                self.results['Time'].append(cur_time)
                # Update
                self.seenurl[self.url] = True

                PAGE_COUNT += 1
                print(PAGE_COUNT, PAGE_COUNT / (time.time() - self.start_time))
        except:
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
    # Dispatch workers
    crawler.dispatch()
    # Dump results to CSV files
    crawler.log()
