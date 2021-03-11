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
import sys

def insert_2_mongo_npc(L, i, each_npc_url):
    fail_url = []
    try:
        # headers = {
        #     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        #     'Connection': 'close'
        # }
        #
        # # print(each_tai_url)
        # requests.adapters.DEFAULT_RETRIES = 5
        s = requests.session()
        s.keep_alive = False
        while True:
            try:
                # print(each_tai_url)
                response = s.get(each_npc_url)
                break
            except:
                # print("Connection refused by the server..")
                # print("Let me sleep for 5 seconds")
                # print("ZZzzzz...")
                time.sleep(20)
                # print("Was a nice sleep, now let me continue...")
                continue

        response.encoding = 'utf-8'

        soup = bs4.BeautifulSoup(response.text, 'html.parser')

        # 2-1 出租者 出租者身分 出租者姓氏 出租者性別
        a_list = soup.find_all('div', class_='avatarRight')

        xml_str_encode = etree.fromstring(a_list[0].encode("utf-8"))

        for elem in xml_str_encode.iter():
            if elem.text is None:
                elem.text = ''

        xml_str_origin = xml_str_encode[0].xpath("//div")

        b_list = []

        for each in xml_str_origin:
            temp = etree.tostring(each).decode("utf-8")
            temp = html.unescape(temp)
            b_list.append(temp)

        b_list = list(b_list)

        # print(b_list[1])

        if "仲介" not in b_list[1]:
            if "屋主" not in b_list[1]:
                # 代理人
                # print(b_list[1])
                tmp_str_list = re.findall("<i.+）", b_list[1])[0].replace("<i>", "").replace("</i>", "").replace("（",
                                                                                                                 " ").replace(
                    "）", "").split(" ")
            elif "屋主" in b_list[1]:
                # 屋主
                # print(b_list[1])
                tmp_str_list = re.findall("<i.+）", b_list[1])[0].replace("<i>", "").replace("</i>", "").replace("（",
                                                                                                                 " ").replace(
                    "）", "").split(" ")

        elif "仲介" in b_list[1]:

            if "屋主" not in b_list[1]:
                # 仲介
                # print(b_list[1])
                tmp_str_list = re.findall("<i.+", b_list[1])[0].replace("<i>", "").replace("</i>", "").replace("(",
                                                                                                                 " ").replace(
                    ")", "").replace("</div>", "").split(" ")
            elif "屋主" in b_list[1]:
                # 屋主
                # print(b_list[1])
                tmp_str_list = re.findall("<i.+）", b_list[1])[0].replace("<i>", "").replace("</i>", "").replace("（",
                                                                                                                 " ").replace(
                    "）", "").split(" ")

        # print(tmp_str_list)

        c_list = [tmp_str_list[0], tmp_str_list[-1]]

        # print(c_list)

        d_list = []

        if "仲介" not in c_list[1]:
            if "屋主" not in c_list[1]:
                d_list.append("代理人")
                if "先生" in c_list[0]:
                    d_list.append("男")
                elif "小姐" in c_list[0] or "太太" in c_list[0] or "媽媽" in c_list[0] or "姐" in c_list[0]:
                    d_list.append("女")
                else:
                    d_list.append("其他")
            elif "屋主" in c_list[1]:
                d_list.append("屋主")
                if "先生" in c_list[0]:
                    d_list.append("男")
                elif "小姐" in c_list[0] or "太太" in c_list[0] or "媽媽" in c_list[0] or "姐" in c_list[0]:
                    d_list.append("女")
                else:
                    d_list.append("其他")

            d_list.append(c_list[0][0])
        elif "仲介" in c_list[1]:
            if "屋主" not in c_list[1]:
                d_list.append("仲介")
                if "先生" in c_list[0]:
                    d_list.append("男")
                elif "小姐" in c_list[0] or "太太" in c_list[0] or "媽媽" in c_list[0] or "姐" in c_list[0]:
                    d_list.append("女")
                else:
                    d_list.append("其他")
            elif "屋主" in c_list[1]:
                d_list.append("屋主")
                if "先生" in c_list[0]:
                    d_list.append("男")
                elif "小姐" in c_list[0] or "太太" in c_list[0] or "媽媽" in c_list[0] or "姐" in c_list[0]:
                    d_list.append("女")
                else:
                    d_list.append("其他")
            d_list.append(c_list[0][0])

        # print(d_list)

        # 2-2 出租者
        # renter_list = soup.find_all('span', class_='kfCallName')

        # 2-3 聯絡電話
        phone_list = soup.find_all('span', class_='dialPhoneNum')
        if len(phone_list) == 0:
            phone = '無電話'
        elif len(phone_list) > 0:
            phone = phone_list[0].get('data-value')

        # 2-4 型態 & 現況
        house_type = soup.find_all('ul', class_='attr')

        xml_str_encode = etree.fromstring(house_type[0].encode("utf-8"))

        for elem in xml_str_encode.iter():
            if elem.text is None:
                elem.text = ''

        xml_str_origin = xml_str_encode[0].xpath("//li")

        li_list = []

        for each in xml_str_origin:
            temp = etree.tostring(each).decode("utf-8")
            temp = html.unescape(temp)
            li_list.append(temp)

        li_list = list(li_list)

        type_li = "無型態"
        now_li = "無現況"
        for each_li in li_list:
            if "型" in each_li:
                type_li = each_li
            elif "現" in each_li:
                now_li = each_li

        # 2-4-1 型態
        if type_li != "無型態":
            type_li_replace_list = list(set(re.findall("<\/?\\w+>", type_li)))

            for replace_str in type_li_replace_list:
                type_li = type_li.replace(replace_str, "")

            type_li = type_li.replace(" ", "").replace("型態:", "")

        # 2-4-2 現況
        if now_li != "無現況":
            now_li_replace_list = list(set(re.findall("<\/?\\w+>", now_li)))

            for replace_str in now_li_replace_list:
                now_li = now_li.replace(replace_str, "")

            now_li = now_li.replace(" ", "").replace("現況:", "")

        # 2-5 抓男女要求
        sex_request_search = soup.find_all('li', class_='clearfix')

        # print("*" * 250)

        sex_request_result = []
        for each_li in sex_request_search:
            if "性" in str(each_li) and "別" in str(each_li) and "要" in str(each_li) and "求" in str(each_li):
                # if "性別要求" in str(each_li):
                sex_request_result.append(each_li)

        # print(len(sex_request_result))
        sex_em = "男女生皆可"

        if len(sex_request_result) > 0:
            em_list = soup.find_all('em')

            for each_em in em_list:
                # print(each_em.get('title'))
                # print("*" * 100)

                if each_em.get('title') != None:
                    if "男" in each_em.get('title') or "女" in each_em.get('title'):
                        # print(each_em)
                        sex_em_replace_list = list(set(re.findall("<\/?\\w+>", str(each_em))))
                        # print(sex_em_replace_list)
                        sex_em = each_em.get('title')

                        for replace_str in sex_em_replace_list:
                            sex_em = sex_em.replace(replace_str, "")

        obj_data = {"renter": c_list[0],
                     "renter_type": d_list[0],
                     "phone": phone,
                     "renter_last_name": d_list[2],
                     "renter_gender": d_list[1],
                     "house_type": type_li,
                     "now_type": now_li,
                     "city": "新北市",
                     "gender_request": sex_em,
                     "url": each_npc_url}

        L.append(obj_data)

    except Exception as ex:
        print(ex)
        fail_url.append(each_npc_url)
        print(each_npc_url)


def split_list_by_n(list_collection, n):
    for i in range(0, len(list_collection), n):
        yield list_collection[i: i + n]

file_num = sys.argv[1]

# print(file_num)
# print(type(file_num))


df = pd.read_csv("./npc_url/591_url_%s.csv" % file_num)

urls = df["url"].to_list()
# print(urls)
# print(urls[0])
# print(urls[1])

temp = split_list_by_n(urls, 1000)
split_tpe_list = []
for each_split in temp:
    split_tpe_list.append(each_split)

with Manager() as manager:
    L = manager.list()  # <-- can be shared between processes.
    processes = []

    for each_url_list in split_tpe_list:
        # time.sleep(10)
        if len(each_url_list) == 1000:
            for i in range(1000):
                p = Process(target=insert_2_mongo_npc, args=(L, i, each_url_list[i]))  # Passing the list
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
            print(L)
            print(len(L))

        elif len(each_url_list) < 1000:

            for i in range(len(each_url_list)):
                p = Process(target=insert_2_mongo_npc, args=(L, i, each_url_list[i]))  # Passing the list
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
            # print(L)
    print(L)
    print(len(L))

    # conn = pymongo.MongoClient("mongodb://591_crawler:1qaz2wsx@192.168.43.223:27017/")
    # db = conn.test
    # collection = db.renter_test
    #
    # collection.insert(L)
    # conn.close()

    df = pd.read_csv("./npc_final.csv")
    # url_dict = {"rent_data": L}
    url_dict = L
    df_tmp = pd.DataFrame(list(url_dict))
    df = pd.concat([df, df_tmp], axis=0, ignore_index=True)
    print(df)
    df.to_csv('./npc_final.csv', index=False, encoding='utf_8_sig')