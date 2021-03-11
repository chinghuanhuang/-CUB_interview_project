import pandas as pd
import re


def zh_num_2_num(zh_num):

    zh2digit_table = {'零': 0, '一': 1, '二': 2, '兩': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
                      '百': 100, '千': 1000, '〇': 0, '○': 0, '○': 0, '０': 0, '１': 1, '２': 2, '３': 3, '４': 4, '５': 5,
                      '６': 6, '７': 7, '８': 8, '９': 9, '壹': 1, '貳': 2, '參': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8,
                      '玖': 9, '拾': 10, '佰': 100, '仟': 1000, '萬': 10000, '億': 100000000}

    digit_num = 0
    result = 0
    tmp = 0
    billion = 0

    while digit_num < len(zh_num):
        tmp_zh = zh_num[digit_num]
        tmp_num = zh2digit_table.get(tmp_zh, None)
        if tmp_num == 100000000:
            result = result + tmp
            result = result * tmp_num
            billion = billion * 100000000 + result
            result = 0
            tmp = 0
        elif tmp_num == 10000:
            result = result + tmp
            result = result * tmp_num
            tmp = 0
        elif tmp_num >= 10:
            if tmp == 0:
                tmp = 1
            result = result + tmp_num * tmp
            tmp = 0
        elif tmp_num is not None:
            tmp = tmp * 10 + tmp_num
        digit_num += 1
    result = result + tmp
    result = result + billion

    return result


def parking_num(string):

    tmp_str = re.findall('車位\d+', string)[0]

    result = int(re.findall('\d+', tmp_str)[0])

    return result

# 使用【pandas】套件，讀取檔名【 a_lvr_land_a 】【 b_lvr_land_a 】 【 e_lvr_land_a 】
# 【 f_lvr_land_a 】 【 h_lvr_land_a 】五份資料集，建立 dataframe 物件【df_a】【df_b】
# 【df_e】【df_f】 【df_h】

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

df_a = pd.read_csv("./a_lvr_land_a.csv").drop(index=[0])
df_b = pd.read_csv("./b_lvr_land_a.csv").drop(index=[0])
df_e = pd.read_csv("./e_lvr_land_a.csv").drop(index=[0])
df_f = pd.read_csv("./f_lvr_land_a.csv").drop(index=[0])
df_h = pd.read_csv("./h_lvr_land_a.csv").drop(index=[0])


# 操作 dataframe 物件，將五個物件合併成【df_all】。

df_all = pd.concat([df_a, df_b, df_e, df_f, df_h])
df_all.reset_index(drop=True, inplace=True)

df_all.to_csv("df_all.csv", index=False, quoting=1, encoding='utf_8_sig')

# 以下列條件從【df_all】篩選/計算出結果，並分別輸出【csv 檔案】 :
# * filter_a.csv
# -【主要用途】為【住家用】
# -【建物型態】為【住宅大樓】
# -【總樓層數】需【大於等於十三層】
# * filter_b.csv
# - 計算【總件數】
# - 計算【總車位數】(透過交易筆棟數)
# - 計算【平均總價元】
# - 計算【平均車位總價元】

# ==============================filter_a.csv==============================
# 總樓層數處理

df_all['總樓層數'] = df_all['總樓層數'].str.replace("層", "")

df_all['總樓層數'].fillna('零', inplace=True)

df_tmp = df_all[df_all['總樓層數'] != '見其他登記事項']

df_tmp['總樓層數'] = df_tmp['總樓層數'].apply(lambda x: zh_num_2_num(x))

# print(df_all['總樓層數'])

filter_a_df = df_tmp[(df_tmp['主要用途'] == '住家用') & (df_tmp['總樓層數'] >= 13) & (df_tmp['建物型態'].str.contains('住宅大樓'))]

filter_a_df.to_csv("filter_a.csv", index=False, quoting=1, encoding='utf_8_sig')

# ==============================filter_b.csv==============================

# 總車位數 re 車位\d+ \d+

# re 車位\d+ \d+
df_parking_num = df_all['交易筆棟數'].apply(lambda x: parking_num(x))

# print(df_parking_num)
parking_num_sum = df_parking_num.sum()

df_all['總價元'] = df_all['總價元'].astype('int64')

df_all['車位總價元'] = df_all['車位總價元'].astype('int64')

avg_park_price = float(df_all['車位總價元'].sum())/parking_num_sum

data = [[len(df_all), parking_num_sum, df_all['總價元'].mean(), avg_park_price]]

filter_b_df = pd.DataFrame(data, columns=['總件數', '總車位數', '平均總價元', '平均車位總價元'])

filter_b_df.to_csv("filter_b.csv", index=False, quoting=1, encoding='utf_8_sig')