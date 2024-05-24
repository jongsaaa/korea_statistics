#directory 내에 있는 모든 file 불러오기
import os
def get_files(path): return os.listdir(path)

#extract number
def extract_number(column_name):
    # 숫자만 추출 (예: '100세 이상'에서 100 추출)
    import re
    match = re.search(r'\d+', column_name)
    if match:
        return int(match.group())
    else:
        return float('inf')  # '100세 이상' 같은 경우 매우 큰 값으로 처리
    

import _codes
sido_cd_map = {info['sido_nm']:info['sido_cd'] for info in _codes.sido_codes}
#sgg_cd setting
def process_sgg_codes(sgg_codes, case_no):
	sido_sgg_cd_map = {}
	for key in sido_cd_map.keys():
		val = sido_cd_map.get(key, None)
		sido_sgg_cd_map[val] = {key:val}    

	if case_no == 1:
		for info in sgg_codes:
			sido_cd = info['sgg_cd'][0:2]
			if sido_cd not in sido_sgg_cd_map:
				sido_sgg_cd_map[sido_cd] = {}
			sgg_nm = info['sgg_nm'].split(' ')
			sgg_nm = sgg_nm[1] if len(sgg_nm) == 2 else sgg_nm[0]
			sido_sgg_cd_map[sido_cd][sgg_nm] = info['sgg_cd']
	elif case_no == 2:
		for info in sgg_codes:
			sido_cd = info['sgg_cd'][0:2]
			if sido_cd not in sido_sgg_cd_map:
				sido_sgg_cd_map[sido_cd] = {}
			sgg_nm = info['sgg_nm']
			sido_sgg_cd_map[sido_cd][sgg_nm] = info['sgg_cd']

	return sido_sgg_cd_map

import psycopg2 #db connect

#query execute
def execute_sql(sql):
	conn = psycopg2.connect('dbname=postgres user=postgres password=force1234 host=localhost port=5432')
	cursor = conn.cursor()

	print(sql)
	cursor.execute(sql)
	result = None
	if str(sql).count('create') > 0: pass
	elif str(sql).count('drop') > 0: pass
	elif str(sql).count('insert') > 0: pass
	else: result = cursor.fetchall()
	if result is not None: print(result)

	conn.commit()
	cursor.close()
	conn.close()

	return result