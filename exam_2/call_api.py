import requests

# res = requests.post("http://localhost:7788/api/591_filter", json={"renter_type":["仲介", "代理人"],
#                                                                   "phone":"0939-265-182",
#                                                                   "renter_last_name":"黃",
#                                                                   "renter_gender":"男",
#                                                                   "city":"新北市",
#                                                                   "gender_request":"可男"})

# res = requests.post("http://localhost:7788/api/591_filter", json={"city": "新北市", "gender_request": "可男"})


# res = requests.post("http://localhost:7788/api/591_filter", json={"phone": "0986-851-077"})


# res = requests.post("http://localhost:7788/api/591_filter", json={"renter_type": ["仲介", "代理人"]})

res = requests.post("http://localhost:7788/api/591_filter", json={"city": "台北市", "renter_type": ["屋主"], "renter_last_name": "吳", "renter_gender": "女"})


print(res.json())

