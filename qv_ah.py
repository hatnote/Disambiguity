import gevent
from gevent.pool import Pool
from gevent import monkey
monkey.patch_all()

import itertools
import requests
import json
import time
import random
import re
from optparse import OptionParser
from pyquery import PyQuery as pq
from collections import namedtuple, OrderedDict
from functools import partial

from progress import ProgressMeter

API_URL = "http://en.wikipedia.org/w/api.php"
DEFAULT_CAT = "Featured articles that have appeared on the main page"
DEFAULT_LIMIT = 100
DEFAULT_CONC  = 100
DEFAULT_PER_CALL = 4
DEFAULT_TIMEOUT = 30
DEFAULT_DB = 'dab_store'
CAT_CONC = 10
ALL = 20000

class WikiException(Exception): pass

def api_req(action, params=None, raise_exc=True, **kwargs):
    all_params = {'format': 'json',
                  'servedby': 'true'}
    all_params.update(kwargs)
    all_params.update(params)
    all_params['action'] = action
    
    resp = requests.Response()
    resp.results = None
    try:
        if action == 'edit':
            resp = requests.post(API_URL, params=all_params)
        else:
            resp = requests.get(API_URL, params=all_params)
            
    except Exception as e:
        if raise_exc:
            raise
        else:
            resp.error = e
            resp.results = None
            return resp
    
    try:
        resp.results = json.loads(resp.text)
        resp.servedby = resp.results.get('servedby')
        # TODO: warnings?
    except Exception as e:
        if raise_exc:
            raise
        else:
            resp.error = e
            resp.results = None
            resp.servedby = None
            return resp

    mw_error = resp.headers.get('MediaWiki-API-Error')
    if mw_error:
        error_str = mw_error
        error_obj = resp.results.get('error')
        if error_obj and error_obj.get('info'):
            error_str += ' ' + error_obj.get('info')
        if raise_exc:
            raise WikiException(error_str)
        else:
            resp.error = error_str
            return resp

    return resp

CategoryMember = namedtuple("CategoryMember", "pageid, ns, title")
def get_category(cat_name, count=500, cont_str=""):
    ret = []
    if not cat_name.startswith('Category:'):
        cat_name = 'Category:'+cat_name
    while len(ret) < count and cont_str is not None:
        cur_count = min(count - len(ret), 500)
        params = {'list':       'categorymembers', 
                  'cmtitle':    cat_name, 
                  'prop':       'info', 
                  'cmlimit':    cur_count,
                  'cmcontinue': cont_str}
        resp = api_req('query', params)
        try:
            qres = resp.results['query']
        except:
            print resp.error # log
            raise
        ret.extend([ CategoryMember(pageid=cm['pageid'],
                                    ns    =cm['ns'],
                                    title =cm['title'])
                     for cm in qres['categorymembers']
                     if cm.get('pageid') ])
        try:
            cont_str = resp.results['query-continue']['categorymembers']['cmcontinue']
        except:
            cont_str = None

    return ret


def get_articles(page_ids=None, titles=None, parsed=True, follow_redirects=False, **kwargs):
    ret = []
    params = {'prop':   'revisions',  
              'rvprop': 'content|ids' }

    if page_ids:
        if not isinstance(page_ids, (str,unicode)):
            try:
                page_ids = "|".join([str(p) for p in page_ids])
            except:
                pass
        params['pageids'] = str(page_ids)
    elif titles:
        if not isinstance(titles, basestring):
            try:
                titles = "|".join([unicode(t) for t in titles])
            except:
                print "Couldn't join: ",repr(titles)
        params['titles'] = titles
    else:
        raise Exception('You need to pass in a page id or a title.')

    if parsed:
        params['rvparse'] = 'true'
    if follow_redirects:
        params['redirects'] = 'true'

    parse_resp = api_req('query', params, **kwargs)
    if parse_resp.results:
        try:
            pages = parse_resp.results['query']['pages'].values()
            redirect_list = parse_resp.results['query'].get('redirects', [])
        except:
            print "Couldn't get_articles() with params: ", params
            print 'URL:', parse_resp.url
            return ret

        redirects = dict([ (r['to'],r['from']) for r in redirect_list ])
        # this isn't perfect since multiple pages might redirect to the same page
        for page in pages:
            if not page.get('pageid') or not page.get('title'):
                continue
            title = page['title']
            pa = Page( title  = title,
                       req_title  = redirects.get(title, title),
                       pageid = page['pageid'],
                       revisionid = page['revisions'][0]['revid'],
                       revisiontext = page['revisions'][0]['*'],
                       is_parsed = parsed,
                       fetch_date = time.time())
            ret.append(pa)
    return ret

def chunked_pimap(func, iterable, concurrency=DEFAULT_CONC, chunk_size=DEFAULT_PER_CALL, **kwargs):
    func = partial(func, **kwargs)
    chunked = (iterable[i:i + chunk_size]
               for i in xrange(0, len(iterable), chunk_size))
    pool = Pool(concurrency)
    return pool.imap_unordered(func, chunked)

Page = namedtuple("Page", "title, req_title, pageid, revisionid, revisiontext, is_parsed, fetch_date")

def find_article_history(text):
    matches = re.findall(r'{{\s*ArticleHistory(.+?)}}', text, re.DOTALL)
    if not matches:
        return None
    else:
        if len(matches) > 1:
            print 'Warning: multiple ArticleHistory instances found.'
        return matches[0].strip().strip('|') #get rid of excess whitespace and pipes

def tmpl_text_to_odict(text):
    ret = OrderedDict()
    pairs = text.split('|')
    for p in pairs:
        p = p.strip()
        if not p:
            continue
        k,_,v = p.partition('=')
        k = k.strip()
        v = v.strip()
        if not k:
            print 'blank key error'
            import pdb;pdb.set_trace()
            continue
        if k in ret:
            print 'duplicate key error'
            import pdb;pdb.set_trace()
            continue
        ret[k] = v
    return ret

from dateutil.parser import parse
class HistoryAction(object):
    def __init__(self, name, **kwargs): #num, date_str, link, result_str, old_id_str):
        if not name or not action_name_re.match(name):
            raise ValueError('Expected HistoryAction name in the format "action#".')
        self.name = name
        self.num = int(name[6:])
        self.type = kwargs.pop('a_type')
        self.date = None
        date = kwargs.pop('date', None)
        try:
            self.date = parse(date)
        except ValueError:
            print 'Could not parse date string: ', date

        self.link = kwargs.pop('link', None)
        self.result = kwargs.pop('result', None)
        self.old_id = kwargs.pop('oldid', None)
        

import copy
action_name_re = re.compile('^action\d+$')
def parse_article_history(hist_orig):
    actions = OrderedDict()
    hist_dict = copy.deepcopy(hist_orig)
    action_names = [ k for k in hist_dict.keys() if action_name_re.match(k) ]
    action_names.sort(key=lambda x: int(x[6:]))
    for a_name in action_names:
        cur_action = HistoryAction(name=a_name,
                                   a_type=hist_dict[a_name],
                                   **dict([(k[len(a_name):],v)
                                           for k,v in hist_dict.items()
                                           if k.startswith(a_name) ])
                                   )
        actions[cur_action.num] = cur_action
    return actions


def main(**kwargs):
    cat_mems = get_category("Featured articles that have appeared on the main page", 10)
    page_ids = [c.pageid for c in cat_mems]
    concurrency = kwargs.pop('concurrency')
    chunk_size = kwargs.pop('grouping')
    pages = []
    histories = []
    am = ProgressMeter(total=len(page_ids), unit="articles", ticks=30)
    for cpages in chunked_pimap(get_articles, 
                                page_ids,
                                parsed=False,
                                concurrency=concurrency,
                                chunk_size=chunk_size):
        for p in cpages:
            am.update(1)
            pages.append(p)
            ah_text = find_article_history(p.revisiontext)
            tmpl_dict = tmpl_text_to_odict(ah_text)
            ah = parse_article_history(tmpl_dict)
            

    import pdb;pdb.set_trace()



def parse_args():
    parser = OptionParser()
    parser.add_option("-d", "--database", dest="database", 
                      type="string", default=DEFAULT_DB,
                      help="name of sqlite database used for saving Dabblets")

    parser.add_option("-a", "--all", dest="get_all", 
                      action="store_true", default=False,
                      help="save as many Dabblets as we can find")

    parser.add_option("-l", "--limit", dest="limit", 
                      type="int", default=DEFAULT_LIMIT,
                      help="max number of articles to search for Dabblets (see -a)")

    parser.add_option("-C", "--category", dest="category", 
                      type="string", default=DEFAULT_CAT,
                      help="category to search for Dabblets (recursive)")

    parser.add_option("-c", "--concurrency", dest="concurrency", 
                      type="int", default=DEFAULT_CONC,
                      help="concurrency factor to use when querying the" 
                      "Wikipedia API (simultaneous requests)")

    parser.add_option("-g", "--grouping", dest="grouping", 
                      type="int", default=DEFAULT_PER_CALL,
                      help="how many sub-responses to request per API call")

    parser.add_option('-D', "--debug", dest="debug",
                      action="store_true", default=False,
                      help="enable debugging (and pop up pdb at the end of successful run")

    parser.add_option("-q", "--quiet", dest="verbose", action="store_false",
                      help="suppress output (TODO)")
    return parser.parse_args()

if __name__ == '__main__':
    opts, args = parse_args()
    main(**opts.__dict__)
