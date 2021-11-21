import requests
from bs4 import BeautifulSoup
import concurrent.futures
import csv
import time



class NCES_Scrapper:
    filename = "hbcu_colleges.csv"
    columns={"General information","Website","Type","Awards offered","Campus setting","Campus housing","Student population","Student-to-faculty ratio"}
    url="https://nces.ed.gov/COLLEGENAVIGATOR/?s=all&ct=1+2+3&ic=1+2+3&pg="
    result=[]
    MAX_THREADS = 6
    def __map_college_data__(self,table_rows):
        keys=set()
        college_info=dict()
        for tr in table_rows:
            tdList=tr.find_all("td")
            key=tdList[0].getText().rstrip()[:-1]
            if key in self.columns:
                college_info[key]=tdList[1].getText()
                keys.add(key)
        for i in self.columns.difference(keys):
            college_info[i]=""
        college_info["Phone"]=college_info.pop("General information")
        return college_info

    def __parse_address__(self,address_data):
        college_data=dict()
        add=address_data.split(",")
        if len(add)==3:
            state_pin=add[-1].split()
            college_data["State"]=" ".join(state_pin[0:-1])
            college_data["Zip"]=state_pin[-1]
            college_data["Street"]=add[0]
            college_data["City"]=add[1]
        else:
            college_data["Street"]=""
            college_data["City"]=""
            college_data["State"]=""
            college_data["Zip"]=""
        return college_data

    def __map_data_to_row__(self,row,url):
        college_info=dict()
        a=row.find("a")
        link=a["href"].split("&")[-1]
        resp= requests.get(url+"&"+link)
        s = BeautifulSoup(resp.content,"html.parser")
        gen_desc=s.find("div",{"class":"collegedash"}).find("span",{"style":"position:relative"})
        college_info["Name"]=gen_desc.find("span",{"class":"headerlg"}).getText()
        address=gen_desc.find("br").next_sibling.getText()
        college_info.update(self.__parse_address__(address))
        table_rows=s.find("table",{"class":"layouttab"}).find_all("tr")
        college_info.update(self.__map_college_data__(table_rows))
        return college_info

    def __get_paged_data__(self,page_num):
        page_url=self.url+page_num
        response= requests.get(page_url)
        soup = BeautifulSoup(response.content,"html.parser")
        rows=soup.find_all("tr",{"class":"resultsW"})
        for row in rows:
            row_data=self.__map_data_to_row__(row,page_url)
            self.result.append(row_data)
    
    def run_scrapper(self):
        page_nums=[str(i+1) for i in range(34)]
        print("Running Scapper...\n")
        start=time.time()
        threads = min(self.MAX_THREADS, len(page_nums))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            executor.map(self.__get_paged_data__, page_nums)
        end=time.time()
        print("\nScrapped "+str(len(self.result))+" colleges in "+str(end-start)+" seconds.")
        
        
        with open(self.filename, 'w') as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = self.result[0].keys())
            writer.writeheader() 
            writer.writerows(self.result) 


if __name__=="__main__":
    bot=NCES_Scrapper()
    bot.run_scrapper()
