from flask import Flask, request, jsonify
import pymongo
import json


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/api/591_filter', methods=['POST'])
def api_591_filter():
    conn = pymongo.MongoClient("mongodb://591_crawler:1qaz2wsx@192.168.0.102:27017/")
    db = conn.test
    collection = db.renter_test

    # read json from app post
    ip = request.remote_addr
    content = request.json
    print(content, ip)
    # get key list
    key_list = list(content.keys())

    combine_list = []
    combine_filter = "{"

    if "renter_type" in key_list:
        if len(content["renter_type"]) == 1:
            combine_list.append('"renter_type":"%s"' % content["renter_type"][0])
        elif len(content["renter_type"]) > 1:
            many_type = ""
            for each_type_num in range(len(content["renter_type"])):
                if each_type_num == len(content["renter_type"]) - 1:
                    many_type = many_type + '{"renter_type": "%s"}' % content["renter_type"][each_type_num]
                else:
                    many_type = many_type + '{"renter_type": "%s"},' % content["renter_type"][each_type_num]

            combine_list.append('"$or":[%s]' % many_type)

    if "phone" in key_list:
        combine_list.append('"phone":{"$regex":"%s"}' % content["phone"])

    if "renter_last_name" in key_list:
        combine_list.append('"renter_last_name":"%s"' % content["renter_last_name"])

    if "renter_gender" in key_list:
        combine_list.append('"renter_gender":"%s"' % content["renter_gender"])

    if "city" in key_list:
        combine_list.append('"city":"%s"' % content["city"])

    if "gender_request" in key_list:
        if content["gender_request"] == "限男":
            combine_list.append('"gender_request":"男生"')
        elif content["gender_request"] == "限女":
            combine_list.append('"gender_request":"女生"')
        elif content["gender_request"] == "男可":
            combine_list.append('"$or":[{"gender_request":"男生"},{"gender_request":"男女生皆可"}]')
        elif content["gender_request"] == "女可":
            combine_list.append('"$or":[{"gender_request":"女生"},{"gender_request":"男女生皆可"}]')
        elif content["gender_request"] == "男女可":
            combine_list.append('"gender_request":"男女生皆可"')


    # 結合所有條件
    for each_filter_num in range(len(combine_list)):
        if each_filter_num == len(combine_list) - 1:
            combine_filter = combine_filter + combine_list[each_filter_num] + "}"
        else:
            combine_filter = combine_filter + combine_list[each_filter_num] + ","

    filter_result = collection.find(eval(combine_filter))
    # list_cur = list(filter_result)


    result_list = []
    for find_result in filter_result:
        result_list.append({'renter': find_result['renter'],
                            'renter_type': find_result['renter_type'],
                            'phone': find_result['phone'],
                            'renter_last_name': find_result['renter_last_name'],
                            'renter_gender': find_result['renter_gender'],
                            'house_type': find_result['house_type'],
                            'now_type': find_result['now_type'],
                            'city': find_result['city'],
                            'gender_request': find_result['gender_request'],
                            'url': find_result['url']})

    conn.close()

    return_json = {'result': result_list}

    return jsonify(return_json)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=7788)