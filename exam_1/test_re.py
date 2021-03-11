import re

string = "土地2建物1車位999"

tmp_str = re.findall('車位\d+', string)[0]

result = re.findall('\d+', tmp_str)[0]

print(result)