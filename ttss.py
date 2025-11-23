import re
import time
import math
import random
import multiprocessing as mp
from collections import defaultdict, Counter

#data
from crawl_data import main 
data = main() 

#  CMS
import mmh3
class CountMinSketch:
    def __init__(self, width=2000, depth=5):
        self.width = width
        self.depth = depth
        self.table = [[0]*width for _ in range(depth)]

    def add(self, key, count=1):
        for i in range(self.depth):
            idx = mmh3.hash(key, i) % self.width
            #VD: hash('cat',0)%2000=6
            self.table[i][idx] += count

    def query(self, key):
        return min(self.table[i][mmh3.hash(key, i) % self.width] for i in range(self.depth))
        # cac key co the trung 1 idx trong 1 dong --> lay min
# clean
VN_STOPWORDS = {
    "và", "trong", "với", "của", "là", "được", "tại", "từ", "cho", "đến",
    "này", "kia", "đó", "này", "ấy", "này", "sẽ", "đã", "đang", "cũng",
    "nhưng", "hay", "hoặc", "nếu", "thì", "rằng", "vì", "do", "khi",
    "trên", "dưới", "giữa", "sau", "trước", "nơi", "đây", "này",
    "người", "ông", "bà", "anh", "chị", "họ", "chúng","ta", "tôi",
    "một", "hai", "ba", "nhiều", "ít", "các", "những", "nhiều",
    "năm", "tháng", "ngày", "hôm",
}

def clean_and_tokenize(text):
    text = text.lower()
    text = re.sub(r"[^a-zA-ZÀ-Ỹà-ỹ0-9\s]", " ", text)
    tokens = text.split()
    tokens = list({t for t in tokens if t not in VN_STOPWORDS and len(t) > 1})
    return tokens

# score title
def compute_title_score(cms, title):
    tokens = clean_and_tokenize(title)
    score = 0
    for w in tokens:
        tf = cms.query(w)
        score +=tf
    return score

# gop cms
def merge_multiple_cms(cms_list):
    base = cms_list[0]
    for cms in cms_list[1:]:
        for d in range(base.depth):
            for w in range(base.width):
                base.table[d][w] += cms.table[d][w]
    return base

# cms 
def streaming_pipeline_realtime(stream_data):
    cms = CountMinSketch()
    for item in stream_data:
        title = item["title"]
        tokens = clean_and_tokenize(title)
        for t in tokens:
            cms.add(t)
    return cms

# last_title
def find_title(stream_data, cms_list):
    cms_last = merge_multiple_cms(cms_list)
    best_score = -1
    best_title = None
    for item in stream_data:
        title = item["title"]
        score = compute_title_score(cms_last, title)
        if score > best_score:
            best_score = score
            best_title = title
    return best_title


