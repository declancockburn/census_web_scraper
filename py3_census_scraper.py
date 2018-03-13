# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 15:02:50 2017

@author: dcockbur

V2 is an attempt to edit it and do multiprocessing perhaps, faster

"""

import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl
import pandas as pd
import os
import time
import requests
import multiprocessing as mp
# Ignore SSL certificate errors for https
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#%%

#Code adjusted for test - delete later, random variable declared

variable1 = 'ABC'


#%%

def geturlcounty(urlcensus):
    html = urllib.request.urlopen(urlcensus, context = ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    censusname = urlcensus.split("/")[-2]
    tags = soup('a')
    urlcounties = []
    for tag in tags:
        taglink = (tag.get('href', None))
        try: 
            if taglink.split('/')[-3] == censusname:
                countyname = taglink.split('/')[-2]
                urlcounties.append(urlcensus + countyname + '/')
        except: pass
    return urlcounties

#urlcounties = geturlcounty(urlcensus)
#urlcounties
#urltowns = geturltown(urlcounty)


#%%
#Quick test closing urls    
#urlcensus = 'http://www.census.nationalarchives.ie/pages/1911/'
#urlcounties = geturlcounty(urlcensus)
#urlcounties = urlcounties[0:5]
#urlcounties
#
#start = time.time()
#for item in urlcounties:
#    geturltown(item)
#
#end = time.time()
#print(end - start)


#%%
#Get the street names and links
def geturltown(urlcounty):
    html = urllib.request.urlopen(urlcounty, context = ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    countyname = urlcounty.split("/")[-2]
    tags = soup('a')
    urltowns = []
    for tag in tags:
        taglink = (tag.get('href', None))
        try: 
            if taglink.split('/')[-3] == countyname:
                townname = taglink.split('/')[-2]
                urltowns.append(urlcounty + townname + '/')
        except: pass
    return urltowns
#
#urltowns = geturltown(urlcounty)
#urltowns


#%%
#Get the street names and links
def geturlstreet(urltown):
    html = urllib.request.urlopen(urltown, context = ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    townname = urltown.split("/")[-2]
    tags = soup('a')
    urlstreets = []
    for tag in tags:
        taglink = (tag.get('href', None))
        try: 
            if taglink.split('/')[-3] == townname:
                streetname = taglink.split('/')[-2]
                urlstreets.append(urltown + streetname + '/')
        except: pass
    return urlstreets


#%%
#get the housenumbers and 
def geturlhouse(urlstreet):
    html = urllib.request.urlopen(urlstreet, context = ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    tablerows = table.find_all('tr')
    list1 = []
    for rows in tablerows:
        rowdata = rows.find_all('td')
        rowlist = [i.text for i in rowdata]
        links = rows('a')
        for link in links:
            if link.contents[0] == 'occupants':
                dict1={}
                dict1['House Number'] = rowlist[0]
                dict1['Link'] = urlstreet + (link.get('href',None)).split('/')[-2]
                list1.append(dict1)
    urlhouse = pd.DataFrame(list1)#, columns = ['House Number', 'House Link'])
    urlhouse.drop_duplicates(inplace=True)
    urllist = urlhouse['Link'].tolist()
    housenumber = urlhouse['House Number'].tolist()
    return urlhouse, urllist, housenumber
#
#urlstreet = urlstreets[0]
#urlhouse = geturlhouse(urlstreet)
#urlhouse

#%%
def makedf(urllist, housenumber):
    dflist=[]
    url1 = urllist[0]
    town = url1.split('/')[-3]
    county =  url1.split('/')[-4]      
    for i in range(len(urllist)):
        url = urllist[i]
        street = url.split('/')[-2]
        res = requests.get(url)
        soup = BeautifulSoup(res.content,'lxml')
        table = soup.find_all('table')[0] 
        frame = pd.read_html(str(table))
        frame = frame[0]
        frame['House Number'] = housenumber[i]  
        frame['Street'] = street
        frame['Town'] = town
        frame['County'] = county
        dflist.append(frame)
    return dflist, county, town

#%%

#
#def executefortown():
#    for i in range(len(urlcounties)):   
#        urlcounty = urlcounties[i]
#        urltowns = geturltown(urlcounty)
#        for n in range(len(urltowns)):
#            urltown = urltowns[n] 
#            urlstreets = geturlstreet(urltown)
#            towndflist = []
#            for p in range(len(urlstreets)):
#                urlstreet = urlstreets[p]
#                urlhouse, urllist, housenumber = geturlhouse(urlstreet)
#                if p < 1:
#                    dflist, county, town = makedf(urllist, housenumber)
#                    for item in dflist:
#                        towndflist.append(item)
#                    if not os.path.exists(county):
#                        os.makedirs(county)
#                else: 
#                    dflist, county, town = makedf(urllist, housenumber)
#                    for item in dflist:
#                        towndflist.append(item)
#            newdf = pd.concat(towndflist, axis=0, ignore_index=True)
#            newdf.to_csv('{}/{}.csv'.format(county, town))
#
#
#
#start = time.time()
#
#urlcensus = 'http://www.census.nationalarchives.ie/pages/1911/'
#urlcounties = geturlcounty(urlcensus)
#executefortown()
#
#end = time.time()
#print(end - start)



#%% Quick testing ground


def executefortown(b,e,s):
    for i in range(b,e,s):   
        urlcounty = urlcounties[i]
        urltowns = geturltown(urlcounty)
        for n in range(1):
            urltown = urltowns[n] 
            urlstreets = geturlstreet(urltown)
            towndflist = []
            for p in range(1):
                urlstreet = urlstreets[p]
                urlhouse, urllist, housenumber = geturlhouse(urlstreet)
                if p < 1:
                    dflist, county, town = makedf(urllist, housenumber)
                    for item in dflist:
                        towndflist.append(item)
                    if not os.path.exists(county):
                        os.makedirs(county)
                else: 
                    dflist, county, town = makedf(urllist, housenumber)
                    for item in dflist:
                        towndflist.append(item)
            newdf = pd.concat(towndflist, axis=0, ignore_index=True)
            newdf.to_csv('{}/{}.csv'.format(county, town))

#%%

urlcensus = 'http://www.census.nationalarchives.ie/pages/1911/'
urlcounties = geturlcounty(urlcensus)

start = time.time()

if __name__ == '__main__':
    args = mp.Value('i', (0,2,2))
    p1 = mp.Process(target = executefortown, args = (args,))  
#    p2 = mp.Process(target = executefortown, args = (1,3,2))
    
    p1.start()
#    p2.start()
    p1.join()
#    p2.join()

end = time.time()
print(end - start)


#%% 118s
urlcensus = 'http://www.census.nationalarchives.ie/pages/1911/'
urlcounties = geturlcounty(urlcensus)
start = time.time()
executefortown(0,2,2)
executefortown(1,3,2)
end = time.time()
print(end - start)


