import string
import re
from rdd import RDD


def clean_string(s):
    s = s.translate(string.maketrans('', ''), string.punctuation)
    s = s.translate(string.maketrans('', ''), string.whitespace)
    return s


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


def executeRDD(parent, func_name, params):
    if func_name == "RDD" and parent == "head":
        return RDD(eval(params))
    param = ()
    if len(params) > 0:
        param = eval(params)
    if hasattr(param, '__call__'):
        return getattr(parent, func_name)(param)
    return getattr(parent, func_name)(*param)
