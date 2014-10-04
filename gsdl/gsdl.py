#!/usr/bin/env python3

# gsdl - google scrape, download and parse results
# 2014, Laurent Ghigonis <laurent@gouloum.fr>

import sys
import threading
import queue

import GoogleScraper
import urllib.parse
import requests
import requests_cache
requests_cache.install_cache('.requests_gsdl_cache')

def init_from_args(obj, just_args=True):
  """ initialise self.* for all caller function local variable.
  if just_args=False, only caller function paramaters are used. """
  caller_name = sys._getframe(1).f_code.co_name
  code_obj = sys._getframe(1).f_code
  for key, value in sys._getframe(1).f_locals.items():
    if ((not just_args)
        or key in code_obj.co_varnames[1:code_obj.co_argcount]):
      setattr(obj, key, value)

class Fetch_parse(object):
    def __init__(self, parsers, usrcb_match):
        init_from_args(self)
        self.threads = list()
        self.matches_count = 0
        self.dlerrors_count = 0

    def fetch_parse(self, item):
        # print("XXX fetch_parse %s" % str(item))
        t = threading.Thread(target=self._thread, args=[item])
        self.threads.append(t)
        # t.daemon = True
        t.start()

    def __str__(self):
        s = ""
        s += "Urls            : %s\n" % self.urls
        s += "Urls count      : %d\n" % len(self.urls)
        s += "Download errors : %d\n" % self.dlerrors_count
        s += "Matches count   : %d\n" % self.matches_count
        return s

    def _thread(self, item):
        title = item[0]
        url = item[1]
        description = item[2]
        self._parse(url, title)
        self._parse(url, description)
        if url is None or url == "":
            return
        r = None
        headers = { 'User-Agent': "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)" }
        try:
            r = requests.get(url, headers=headers, allow_redirects=True, verify=False, timeout=20)
        except Exception:
            try:
                with requests_cache.disabled():
                    r = requests.get(url, headers=headers, allow_redirects=True, verify=False, timeout=20)
            except Exception:
                pass
        if not r or r.status_code != 200:
            self.dlerrors_count += 1
            if r:
                print("ERROR: %s (%d)" % (url, r.status_code))
            else:
                print("ERROR: %s" % (url))
            return
        self._parse(url, r.text)

    def _parse(self, url, text):
        parser = self._parse_fallback # no user parser
        for p in self.parsers.keys():
            if p == '*':              # user provided default parser
                parser = self.parsers[p]
                continue
            if not url.endswith(p): # XXX gross, use mime ?
                continue
            parser = self.parsers[p]  # user provided specific parser
            break
        results = parser(text)
        for r in results:
            self.matches_count += 1
            ok = self.usrcb_match(r, url)
            if not ok:
                return # stop here

    def _parse_fallback(text):
        return []

class Gsdl_search(object):
    NUM_PAGES_SEL = 30
    NUM_PAGES_HTTP = 2

    def __init__(self, searches, cb_results, domain=None, scrapemethod='http', proxyfile=None):
        init_from_args(self)
        for s in searches.keys():
            if domain and len(domain) > 0:
                searches[s] = searches[s] + ' site:%s' % domain
        if scrapemethod == 'http':
            self.num_pages = self.NUM_PAGES_HTTP
        else:
            self.num_pages = self.NUM_PAGES_SEL
        self.urls = list()

    def run(self):
        config = {
            'SCRAPING': {
                'use_own_ip': 'True',
                'keywords': '\n'.join(self.searches.values()),
                'num_of_pages': "%s" % self.num_pages,
                'scrapemethod': self.scrapemethod
            },
            'SELENIUM': {
                'sel_browser': 'chrome',
                'manual_captcha_solving': 'True',
                # 'sleeping_ranges': '5; 1, 2', # more agressive than defaults
            },
            'GLOBAL': {
                'do_caching': 'True',
                #'do_caching': 'False',
                #'cachedir': 'dc
                'db': "results_{asctime}.db",
                # 'debug': 'WARNING',
                'debug': 'ERROR',
            },
            'GOOGLE_SEARCH_PARAMS': {
                'start': "0",
                'num': "20",
            }
        }
        if self.proxyfile:
            print("Using proxies from %s" % self.proxyfile)
            config['GLOBAL']['proxy_file'] = self.proxyfile
        # GoogleScraper.config.update_config(config) # hack, GoogleScraper config 'db' path is broken when 2nd time
        db = GoogleScraper.scrape_with_config(config, usrcb_result=self.cb_results)
        urls = db.execute('SELECT * FROM link').fetchall()
        db.close()
        self.urls.extend(urls)
        return urls

    def __str__(self):
        s += "Search patterns     : %s\n" % self.searches
        if self.domain:
            s += "Restict to domain   : %s\n" % self.domain
        s += "Total results count : %d\n" % len(self.urls)
        return s

class Gsdl(object):
    def __init__(self, searches, domain, count_objective=-1, parsers=None, outfile=None, scrapemethod='http', proxyfile=None):
        """ Google scrape and download
searches          : List of patterns to lookup on Google
domain            : Restrict search to this domain
[count_objective] : Continue search until this number of distinct matches are found.
                    Default: -1 = searches until no more google results are found
[parsers]         : List of additionnal parsers for results links
                    Default: perform simple match around the pattern
[outfile]         : Output file for matched results in pages, written in real time
                    Default: do not write to file
[verbose]         : Display matches in results to stdout in real time
"""
        init_from_args(self)
        self.search_t = threading.Thread(target=self._search_thread)
        self.fetch_t = threading.Thread(target=self._fetch_thread)
        self.search_t.daemon = True
        self.stop = False
        if outfile:
            print("Writing to %s" % outfile)

    def run(self):
        self.res_q = queue.Queue()
        self.fetch_q = queue.Queue()
        self.matches = dict() # { 'myresult': ['http://...', ... ] }
        if self.outfile:
            self.outf = open(self.outfile, 'w')
        self.search_t.start()
        self.fetch_t.start()
        while self.count_objective == -1 or len(self.matches.keys()) < self.count_objective:
            res = self.res_q.get()
            if res is None:
                break
            (match, url) = res
            if match not in self.matches.keys():
                self.matches[match] = [url]
                output = "%s\t\t%s" % (res[0], res[1])
                if self.outfile:
                    self.outf.write(output+"\n")
                    self.outf.flush()
                print(output)
            else:
                self.matches[match].append(url)
        self.stop = True
        if self.outfile:
            self.outf.close()
        self.search_t.join()
        self.fetch_t.join()

    def __str__(self):
        s = ""
        s += "Search patterns        : %s\n" % self.searches
        s += "Parsers                : %s\n" % self.parsers
        s += "Distinct matches found : %d\n" % len(self.matches)
        if self.outfile:
            s += "Output file            : %s\n" % self.outfile
        return s

    def _search_thread(self):
        g = Gsdl_search(self.searches, self._cb_search_results, self.domain, self.scrapemethod, self.proxyfile)
        g.run()

    def _cb_search_results(self, item):
        # print("XXX ========== _cb_search_results %s" % str(item))
        (first, second) = item
        #print("XXX _cb_search_results first: %s" % str(first))
        #print("XXX _cb_search_results second: %s" % str(second))
        for item in second:
            # print("XXX FETCH %s" % str(item))
            self.fetch_q.put(item)

    def _fetch_thread(self):
        f = Fetch_parse(self.parsers, self._cb_fetch_match)
        while True:
            item = self.fetch_q.get()
            f.fetch_parse(item)

    def _cb_fetch_match(self, match, url):
        # Called from fetch_t-Thread(Fetch_parse-Thread)
        self.res_q.put((match, url))
        if self.stop:
            return False
        return True

