import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import parser
from datetime import datetime
import os, re, time

# scrapes Nova Scotia data
def web_scrape_NS(soup):
    data = []
    table = soup.find_all('td', attrs={'width': '45%'})
    table = [ele.text.strip() for ele in table] # strip tags
    data.append([ele for ele in table if ele]) # strip empty values
    data = data[0]

    load = findValue('Net Load', data)
    return load
	
# scrapes New Brunswick data
def web_scrape_NB(soup):
    load_box = soup.find('td', attrs={'id': 'nb-load'})
    load = load_box.text.strip() # strip() is used to remove starting and trailing
    return load
	
# scrapes Ontario data
def web_scrape_ON(soup):
    span = soup.findAll(string=re.compile("MW")) #'div', class_="col-sm-3")
    for i in range(len(span)):
        span[i] = span[i][:len(span[i])-3]
    load = span[1]
    types = ['0', span[15], span[13], span[17], span[14], span[16], span[12]]
    return load, types
	
# scrapes Newfoundland and Labrador data
def web_scrape_NF(soup):
    loadStr = soup.find_all(string=re.compile("MW"))[0]
    load = loadStr[:len(loadStr)-4]
    return load
	
# scrapes Alberta data
def web_scrape_AB(soup):
    link = soup.find(string = re.compile("Alberta Total Net Generation"))
    load = link.find_next(string = True)
    srcs = ["COAL", "GAS", "HYDRO", "OTHER", "WIND"]
    sources = []
    for i in range(len(srcs)):
        sources.append(soup.find(string = re.compile(srcs[i])).find_next(string = True).find_next(string = True))
    sources.append('0')
    sources.append('0')
    return load, sources
	
def findValue(element, data):
    i = 0
    for ele in data:
        if ele == element:
            loadIndex = i
        else:
            i += 1
    load = data[loadIndex+1]
    return load

# returns the BeautifulSoup given a url to scrape
def urlOpen(url):
    html = urllib.request.urlopen(url)
    return BeautifulSoup(html, 'html.parser')

def csvWriter():
	# repositories to get data from
	urls = {"NF": "https://nlhydro.com/system-information/supply-and-demand/", 
			"ON": "http://www.ieso.ca/en/Power-Data/This-Hours-Data", 
			"NB": "https://tso.nbpower.com/Public/en/SystemInformation_realtime.asp",
			"NS": "https://resourcesprd-nspower.aws.silvertech.net/oasis/current_report.shtml",
			"AB": "http://ets.aeso.ca/ets_web/ip/Market/Reports/CSDReportServlet"}
	soupNF = urlOpen(urls["NF"])
	soupON = urlOpen(urls["ON"])
	soupNB = urlOpen(urls["NB"])
	soupNS = urlOpen(urls["NS"])
	soupAB = urlOpen(urls["AB"])
	# finding time at retrieval from url
	time = str(datetime.now().hour) + ":" + str(datetime.now().minute)

	# getting all load data from repositories
	loadNF = web_scrape_NF(soupNF)
	loadON, sourcesON = web_scrape_ON(soupON)
	loadNB = web_scrape_NB(soupNB)
	loadNS = web_scrape_NS(soupNS)
	loadAB, sourcesAB = web_scrape_AB(soupAB)
	loadList = [loadNF, loadON, loadNB, loadNS, loadAB]

	# strip any commas from the load values
	for i in range(len(loadList)):
		loadList[i] = loadList[i].replace(',', '')
	for i in range(len(sourcesON)):
		sourcesON[i] = sourcesON[i].replace(',', '')
	for i in range(len(sourcesAB)):
		sourcesAB[i] = sourcesAB[i].replace(',', '') 
		
	# Output energy source information to csv    
	sourcesType = ["Coal", "Gas", "Hydro", "Biomass", "Wind", "Solar", "Nuclear"]

	dfSources = pd.DataFrame(columns=['Time', 'Source (MW)', 'Type', 'Province'])
	for i in range(len(sourcesON)):
		dfSources = dfSources.append({'Time': time, 'Source (MW)': sourcesON[i], 'Type': sourcesType[i], 'Province': 'Ontario'}, ignore_index = True)
		dfSources = dfSources.append({'Time': time, 'Source (MW)': sourcesAB[i], 'Type': sourcesType[i], 'Province': 'Alberta'}, ignore_index = True)
	dfSources.to_csv('EnergySources.csv', index = False, mode = 'a', header = False)
		
	# Output load information to csv
	provinces = ["Newfoundland and Labrador", "Ontario", "New Brunswick", "Nova Scotia", "Alberta"]

	dfLoad = pd.DataFrame(columns=['Time','Net Load (MW)', 'Province'])
	# dfLoad = pd.DataFrame(columns=['Time','Net Load (MW)', 'Province'])
	for i in range(len(loadList)):
		dfLoad = dfLoad.append({'Time': time, 'Net Load (MW)': loadList[i], 'Province': provinces[i]}, ignore_index = True)
	dfLoad.to_csv('Energy.csv', index = False, mode = 'a', header = False)

dfSources = pd.DataFrame(columns=['Time', 'Source (MW)', 'Type', 'Province'])
dfSources.to_csv('EnergySources.csv', index = False, header = True)

dfLoad = pd.DataFrame(columns=['Time','Net Load (MW)', 'Province'])
dfLoad.to_csv('Energy.csv', index = False, header = True)

for i in range(4):
	csvWriter()
	time.sleep(10)
    
print("Done")