from multiprocessing import Process, Manager
from selenium import webdriver
from selenium.webdriver.common.by import By
import bs4
import re
import requests
from lxml import etree
import html
import pymongo
import multiprocessing
import random
import time
import pandas as pd
import os
from csv import reader


def get_tpe_all(taipei_url_list):
    # taipei_url_list = []
    # ==============================Taipei==================================
    driver_path = "./chromedriver"

    option = webdriver.ChromeOptions()
    option.add_argument('--windows-size=1280,1024')
    browser = webdriver.Chrome(executable_path=driver_path, chrome_options=option)

    browser.get('https://rent.591.com.tw/?kind=0&region=1')
    browser.implicitly_wait(1)

    browser.find_element_by_xpath("//dd[@data-id=1]").click()

    while True:
        html_source = browser.page_source

        soup = bs4.BeautifulSoup(html_source, 'html.parser')

        next_page = soup.find_all("a", class_="last")

        print(len(next_page))

        if len(next_page) == 0:

            h3_list = soup.find_all("h3")
            title_list = []
            for i in h3_list:
                tmp = i.find_all("a")
                title_list.append(tmp[0])

            for j in title_list:
                tmp_str = re.findall("rent.591\\S+html", str(j))[0]
                # print(tmp_str)
                taipei_url_list.append("https://%s" % tmp_str)
            try:
                browser.find_element_by_class_name("pageNext").click()
            except Exception as ex:
                try:
                    browser.find_element_by_class_name("pageNext").click()
                except Exception as ex:
                    browser.find_element_by_class_name("pageNext").click()

        elif len(next_page) == 1:

            h3_list = soup.find_all("h3")
            title_list = []
            for i in h3_list:
                tmp = i.find_all("a")
                title_list.append(tmp[0])

            for j in title_list:
                tmp_str = re.findall("rent.591\\S+html", str(j))[0]
                # print(tmp_str)
                taipei_url_list.append("https://%s" % tmp_str)

            break

    # taipei_url_list = list(set(taipei_url_list))
    # print(taipei_url_list)
    # print(len(taipei_url_list))
    # print("=" * 250)

    browser.close()

    browser.quit()

    # return taipei_url_list


def get_npc_all(newtai_url_list):
    driver_path = "./chromedriver"

    option = webdriver.ChromeOptions()
    option.add_argument('--windows-size=1280,1024')
    browser = webdriver.Chrome(executable_path=driver_path, chrome_options=option)

    browser.get('https://rent.591.com.tw/?kind=0&region=1')
    browser.implicitly_wait(1)

    browser.find_element_by_xpath("//dd[@data-id=3]").click()

    while True:
        html_source = browser.page_source

        soup = bs4.BeautifulSoup(html_source, 'html.parser')

        next_page = soup.find_all("a", class_="last")

        print(len(next_page))

        if len(next_page) == 0:

            h3_list = soup.find_all("h3")
            title_list = []
            for i in h3_list:
                tmp = i.find_all("a")
                title_list.append(tmp[0])

            for j in title_list:
                tmp_str = re.findall("rent.591\\S+html", str(j))[0]
                # print(tmp_str)
                newtai_url_list.append("https://%s" % tmp_str)
            try:
                browser.find_element_by_class_name("pageNext").click()
            except Exception as ex:
                try:
                    browser.find_element_by_class_name("pageNext").click()
                except Exception as ex:
                    browser.find_element_by_class_name("pageNext").click()

        elif len(next_page) == 1:

            h3_list = soup.find_all("h3")
            title_list = []
            for i in h3_list:
                tmp = i.find_all("a")
                title_list.append(tmp[0])

            for j in title_list:
                tmp_str = re.findall("rent.591\\S+html", str(j))[0]
                # print(tmp_str)
                newtai_url_list.append("https://%s" % tmp_str)

            break

    # newtai_url_list = list(set(newtai_url_list))
    # print(newtai_url_list)
    # print(len(newtai_url_list))

    browser.close()

    browser.quit()

    # return newtai_url_list


def split_list_by_n(list_collection, n):
    for i in range(0, len(list_collection), n):
        yield list_collection[i: i + n]


def main():
    # =================新北url========================
    npc_url_list = []
    get_npc_all(npc_url_list)
    npc_url_list = list(set(npc_url_list))

    temp = split_list_by_n(npc_url_list, 1000)
    split_npc_list = []
    for each_split in temp:
        split_npc_list.append(each_split)

    num = 1
    for each_1000_url in split_npc_list:
        url_dict = {"url": each_1000_url}
        df = pd.DataFrame(url_dict)
        df.to_csv('./npc_url/591_url_%d.csv' % num, index=False, encoding='utf_8_sig')
        num = num + 1

    # =================臺北url========================

    tpe_url_list = []
    get_tpe_all(tpe_url_list)
    tpe_url_list = list(set(tpe_url_list))

    temp = split_list_by_n(tpe_url_list, 1000)
    split_tpe_list = []
    for each_split in temp:
        split_tpe_list.append(each_split)

    num = 1
    for each_1000_url in split_tpe_list:
        url_dict = {"url": each_1000_url}
        df = pd.DataFrame(url_dict)
        df.to_csv('./tpe_url/591_url_%d.csv' % num, index=False, encoding='utf_8_sig')
        num = num + 1


    # ============create empty tpe finish csv==================
    os.system("touch tpe_final.csv")
    os.system("echo renter,renter_type,phone,renter_last_name,renter_gender,house_type,now_type,city,gender_request,url > tpe_final.csv")
    os.system("touch npc_final.csv")
    os.system("echo renter,renter_type,phone,renter_last_name,renter_gender,house_type,now_type,city,gender_request,url > npc_final.csv")

    # ==========================call tpe_2_db.py======================
    tpe_folder_path = './tpe_url'

    tpe_file_temp_list = os.listdir(tpe_folder_path)

    tpe_file_final_list = []

    for each_file in tpe_file_temp_list:
        if "591" in each_file:
            tpe_file_final_list.append(each_file)

    for num in range(len(tpe_file_final_list)):
        call_num = num + 1
        os.system('python tpe_2_db.py %d' % call_num)

    # ==========================call npc_2_db.py======================

    npc_folder_path = './npc_url'

    npc_file_temp_list = os.listdir(npc_folder_path)

    npc_file_final_list = []

    for each_file in npc_file_temp_list:
        if "591" in each_file:
            npc_file_final_list.append(each_file)

    for num in range(len(npc_file_final_list)):
        call_num = num + 1
        print(call_num)
        os.system('python npc_2_db.py %d' % call_num)

    # read all csv 2 mongo
    # tpe
    with open('./tpe_final.csv', 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Pass reader object to list() to get a list of lists
        list_of_rows = list(csv_reader)
    # print(list_of_rows)

    tpe_2_db_list = []
    for i in list_of_rows:
        tpe_2_db_list.append({"renter": i[0],
                              "renter_type": i[1],
                              "phone": i[2],
                              "renter_last_name": i[3],
                              "renter_gender": i[4],
                              "house_type": i[5],
                              "now_type": i[6],
                              "city": i[7],
                              "gender_request": i[8],
                              "url": i[9]})

    print(tpe_2_db_list)

    # npc
    with open('./npc_final.csv', 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Pass reader object to list() to get a list of lists
        list_of_rows = list(csv_reader)
    # print(list_of_rows)

    npc_2_db_list = []
    for i in list_of_rows:
        npc_2_db_list.append({"renter": i[0],
                              "renter_type": i[1],
                              "phone": i[2],
                              "renter_last_name": i[3],
                              "renter_gender": i[4],
                              "house_type": i[5],
                              "now_type": i[6],
                              "city": i[7],
                              "gender_request": i[8],
                              "url": i[9]})

    print(npc_2_db_list)

    final_2_db_list = tpe_2_db_list + npc_2_db_list

    conn = pymongo.MongoClient("mongodb://591_crawler:1qaz2wsx@192.168.0.102:27017/")
    db = conn.test
    collection = db.renter_test

    collection.drop()
    collection.insert(final_2_db_list)
    collection.delete_many({"renter_type": "renter_type"})
    conn.close()

    # 刪除暫存檔
    for each_tmp_file in tpe_file_final_list:
        os.system("rm -f ./tpe_url/%s" % each_tmp_file)

    for each_tmp_file in npc_file_final_list:
        os.system("rm -f ./npc_url/%s" % each_tmp_file)

    os.system("rm -f tpe_final.csv")
    os.system("rm -f npc_final.csv")


if __name__ == "__main__":
    main()

