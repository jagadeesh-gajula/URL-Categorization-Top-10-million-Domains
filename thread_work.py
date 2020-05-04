import random
import threading 
import time
import numpy as np
import pandas as pd
import re
import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os
import requests
import urllib
import html2text
from datetime import datetime
from pytz import timezone
from tzlocal import get_localzone
import threading
import time
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
import nltk
import os
import psutil
import warnings
import random
import re
warnings.filterwarnings("ignore")


nltk.download('stopwords')
nltk.download('punkt')
stop_words = set(stopwords.words('english')) 
h = html2text.HTML2Text()
h.ignore_links=True
pd.low_memory=False
df = pd.read_csv("sample_data.csv")
df.columns=['url','category']
data = pd.DataFrame(columns=['url','category','text'])
avg=[]


def stop_words_remover(word_tokens): 
    filtered_sent = [w for w in word_tokens if not w in stop_words] 
    final = ' '.join(filtered_sent)
    return final


def clean_stopwords(text):
    text_tokens = word_tokenize(text)
    tokens_without_sw = [word for word in text_tokens if not word in stopwords.words()]
    return ' '.join(tokens_without_sw)

def extract(url):
    warnings.filterwarnings("ignore")
    try:
        page = requests.get(url)
        if page.status_code == 200:
            contents = page.content
            soup = BeautifulSoup(contents)
            [script.decompose() for script in soup(["script", "style"])]
            strips = list(soup.stripped_strings)
            strips = ' '.join(strips)
            strips = strips.replace(',',' ')
            return clean_stopwords(strips)
        else:
            return "NULL"
    except:
        return "NULL"


def extract_link(url):
    warnings.filterwarnings("ignore")
    try:
        page = urllib.request.urlopen(url)
    except:
        return "NULL"
    data=page.read()
    soup=BeautifulSoup(data,'html.parser',from_encoding="iso-8859-1")
    tags = ['title','a','p','input','div','html','table']
    result = []
    if len(tags) == 0:
        return "NULL"
    for k in tags:
        fi=soup.find_all(k)
        s = [h.handle(str(i)) for i in fi]
        s = ' '.join(s)
        s = s.replace('\n',' ')
        s = s.replace(',',' ')
        result.append(s)
    return stop_words_remover(' '.join(result))



def prepare_url(url):
    warnings.filterwarnings("ignore")
    urls =[]
    secure_scheme = 'https://'
    insecure_scheme = 'http://'
    netloc =  urlparse(url).netloc
    if len(netloc) < 2:
            netloc = urlparse(url).path
    
    urls.append(secure_scheme+netloc)
    urls.append(insecure_scheme+netloc)
    return urls





def create_df(i,total):
    warnings.filterwarnings("ignore")
    beg_time = time.time()
    url = prepare_url(df.url[i])
    try:
        text = extract(url[0])
        if text == 'NULL':
            text = extract(url[1])
        data.loc[i] = [df.url[i],df.category[i],text]
    except:
        data.loc[i] = [df.url[i],df.category[i],"NULL"]
    end_time = time.time()
    avg.append(end_time - beg_time)
    load = round(psutil.cpu_percent(),2)
    ram = psutil.virtual_memory()
    ram  = ram.percent
    parallels = threading.activeCount()
    print("Threads :",parallels,"| avg_life: ",round(np.mean(avg),2),"secs | CPU: ",load,"%  | RAM: ",ram," % |",round((data.shape[0]/total)*100,3)," % complete      ",end='\r')




format = "%Y-%m-%d %H:%M:%S"
now_utc = datetime.now(timezone('GMT'))
now_local = now_utc.astimezone(get_localzone())
print("Program started at",now_local.strftime(format),"\n\n")

for j in range(1,int(df.shape[0]/75)):
    data = pd.DataFrame(columns=['url','category','text'])
    begin = j * 1000
    end = begin + 1000
    total = end -  begin
    print("now on ",j," th part")
    for i in range(begin,end):
        t = threading.Thread(target=create_df,args=(i,total))
        t.start()
        avail = psutil.virtual_memory().available >> 20
        while threading.activeCount() > 20 or avail < 300 :
            avail = psutil.virtual_memory().available >> 20
            time.sleep(0.1)
    time.sleep(60)
    data.to_csv("part_"+str(j)+".csv")
    del(data)



now_utc = datetime.now(timezone('GMT'))
now_local = now_utc.astimezone(get_localzone())
print("Program Ended at",now_local.strftime(format),"\n\n")
print("data saved if program is still runnning close with CTRL + C.","." * 30)



