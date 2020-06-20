import json
import traceback
import requests
import re
import datetime
from bs4 import BeautifulSoup
from pymongo import MongoClient

'''
Class to scrap the economic time data
'''

class EconomicScrapper(object):
    
    def __init__(self):
        with open("./config.json", "r") as fp:
            self.config_dct = json.load(fp)
        try:
            self.client = MongoClient('localhost', 27017)
            print ("connected to mongodb")
        except Exception as e:
            print ("exception occured while connecting to database",e)
        # db = self.client.scrap_econo
        
    '''
    Main crawler function
    '''
    def crawler(self):
        result = {}
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
                }
            result_content = requests.get(self.config_dct["target_url"], headers= headers, proxies = self.config_dct["proxy_config"], timeout = 5 )
            if result_content.status_code == 200:
                if result_content.content:
                    crawled_data = result_content.content
                    rgex_search = re.search(b'ajaxResponse\((.*)\)(\r)?(\n)?', crawled_data)
                    if rgex_search:
                        result = json.loads(rgex_search.group(1))
                        # print json.dumps(result, indent=3)
                else:
                    print ("Data is blank.")
            else:
                print ("*"*50, " Site is blocking the crawling activity. Hit again.\n Error Code", result_content.status_code, "*"*50)
                
        except Exception as e:
            print ("Exception in crawler for crawling: ", e)
        return result


    
    def crawlerNseBse(self,co_id):
        result = {}
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
                }
            result_content = requests.get("https://json.bselivefeeds.indiatimes.com/ET_Community/companypagedata?companyid="+str(co_id)+"&companytype=&callback=ets.hitMarket",
             headers= headers, proxies = self.config_dct["proxy_config"], timeout = 5 )
            if result_content.status_code == 200:
                if result_content.content:
                    crawled_data = result_content.content
                    rgex_search = re.search(b'ets.hitMarket\((.*)\)(\r)?(\n)?', crawled_data)
                    if rgex_search:
                        result = json.loads(rgex_search.group(1))
                        # print json.dumps(result, indent=3)
                else:
                    print ("Data is blank.")
            else:
                print ("*"*50, " Site is blocking the crawling activity. Hit again.\n Error Code", result_content.status_code, "*"*50)
                
        except Exception as e:
            print ("Exception in crawler for crawling: ", e)
        # print result
        return result


    '''
    Dump in mongodb
    '''
    def dump_db(self, data_to_dump):
        companyId = []
        db_name = self.config_dct["db_name"]
        db_ref = self.client[db_name]
        collection = db_ref["historic_data"]
        data_to_insert = {}
        try:
            if data_to_dump:
                for items in data_to_dump['searchresult']:
                    data_to_insert = items
                    co_id = items['companyid']
                    date_str = items['xdividenddatestr'].strip()
                    format_str = '%d-%m-%Y'
                    dividendDate = datetime.datetime.strptime(date_str,format_str) 
                    # print co_id
                    print (type(dividendDate))
                    bse_nse_data = self.crawlerNseBse(co_id)
                    bseNseJson = bse_nse_data['bseNseJson']
                    bsePrice = ''
                    nsePrice = ''
                    bseDividend = ''
                    nseDividend = ''
                    if len(bseNseJson) > 0:
                        for item_d in bseNseJson:
                            
                            if 'segment' in item_d:
                                if item_d['segment'] == 'BSE':
                                    
                                    print ("bse found")
                                    bsePrice = item_d['lastTradedPrice']
                                    bseDividend = float(items['dividendvalue']) / float(bsePrice) 
                            
                                else:
                                    print ("nse found")
                                    
                                    nsePrice = item_d['lastTradedPrice']
                                    nseDividend = float(items['dividendvalue']) / float(nsePrice)  

                    else:
                        bsePrice = ''
                        nsePrice = ''
                        bseNseJson = []
                        bseDividend = ''
                        nseDividend = ''        
                    
                    data_to_insert['dividendDate'] = dividendDate
                    data_to_insert['bseNseJson'] = bseNseJson
                    data_to_insert['bsePrice'] = bsePrice
                    data_to_insert['nsePrice'] = nsePrice
                    data_to_insert['bseDividend'] = bseDividend
                    data_to_insert['nseDividend'] = nseDividend
                    collection.update(
                        {
                            'companyid':items['companyid']
                        },data_to_insert,upsert=True)
               
                print ("Data inserted successfully in historic_data of data_center db")

        except Exception as e:
            print ("Exception in mongodb dump operation", e)
        
        
if __name__ == '__main__':
    obj = EconomicScrapper()
    return_data = obj.crawler()
    obj.dump_db(return_data)



# // "https://mfapps.indiatimes.com/etcal/currencycontroller?pagesize=25&pid=58&pageno=1&sortby=mcap&sortorder=asc&year=2017&callback=ajaxResponse",
