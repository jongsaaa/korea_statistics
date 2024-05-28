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

#sido_sgg_cd_map 만들기
def getSggCd(sido_cd, sgg_nm, year, case_no):
	year = int(year)
	if year < 2005: code = _codes.sgg_codes_2000
	elif year < 2010: code = _codes.sgg_codes_2005
	elif year < 2015: code = _codes.sgg_codes_2010
	elif year < 2020: code = _codes.sgg_codes_2015
	elif year < 2023: code = _codes.sgg_codes_2020
	elif year >= 2023: code = _codes.sgg_codes_2023
	else: raise Exception

	sido_sgg_cd_map = process_sgg_codes(code, case_no)
	sido = sido_sgg_cd_map.get(sido_cd, {})
	sgg_cd = dict(sido).get(sgg_nm, None)

	return sgg_cd

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
	elif str(sql).count('update') > 0: pass
	else: result = cursor.fetchall()
	if result is not None: print(result)

	conn.commit()
	cursor.close()
	conn.close()

	return result

#list type data 분할하기
def chunker(seq, size):
	return (seq[pos:pos + size] for pos in range(0, len(seq), size))

# 특정 테이블 컬럼명 꺼내기
def getTableColumns(table_nm):
	sql = f'''
		SELECT column_name as col_nm
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = '{table_nm}';
	'''
	return execute_sql(sql)

def getMatViewCols(view_nm):
	sql = f'''
	SELECT a.attname as col_nm
	FROM pg_attribute a
	JOIN pg_class c ON a.attrelid = c.oid
	WHERE c.relname = '{view_nm}' 
	AND a.attnum > 0 
	AND NOT a.attisdropped;
	'''
	return execute_sql(sql)

# object to string
def objToStr(obj):
	_str = ''
	try:
		_str = str(obj)
	except:
		_str = ''
	finally:
		return _str
	
# object to integer
def objToInt(obj):
	_int = 0
	try:
		_int = int(obj)
	except:
		_int = 0
	finally:
		return _int