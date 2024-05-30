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

	# print(sql)
	try:
		cursor.execute(sql)
	except Exception as e:
		print("error", sql)
		print(e)
		return

	result = None
	# if str(sql).count('create') > 0: pass
	# elif str(sql).count('drop') > 0: pass
	# elif str(sql).count('insert') > 0: pass
	# elif str(sql).count('update') > 0: pass
	# if result is not None: print(result)
	try: result = cursor.fetchall()
	except: pass
	finally:
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
	
def objToFloat(obj):
	_flt = 0
	try:
		_flt = float(obj)
	except:
		_flt = float('inf')
	finally:
		return _flt	

# object to divide
def objDivide(obj1, obj2):
	_rst = 0
	try:
		_rst = float(obj1) / float(obj2)
	except:
		_rst = float('inf')
	finally:
		return _rst
	
def objMultiple(obj1, obj2):
	_rst = 0
	try:
		_rst = float(obj1) * float(obj2)
	except:
		_rst = float('inf')
	finally:
		return _rst
	
# 지도 레이어 년도 설정
def makeGisYear (year):
	if year < 2005: gis_year = 2000
	elif year < 2010: gis_year = 2005
	elif year < 2015: gis_year = 2010
	elif year < 2020: gis_year = 2015
	elif year < 2023: gis_year = 2020
	else: gis_year = 2023

	return gis_year

# sgg_cd mapping
def beforeSggCdMapping(sgg_cd, year = None):
	if sgg_cd == '34080' and year is not None and year < 2015: sgg_cd = '34390' #당진시 / 당진군
	if sgg_cd == '31280' and year is not None and year < 2015: sgg_cd = '31320' #여주시 / 여주군
	if sgg_cd == '33040' and year is not None and year < 2015: sgg_cd = '33010' #청주시
	if sgg_cd == '34070' and year is not None and year < 2005: sgg_cd = '34400' #계룡시 / 계룡출장소
	if sgg_cd == '31270' and year is not None and year < 2005: sgg_cd = '31360' #포천시
	if sgg_cd == '23090' and year is not None and year < 2020: sgg_cd = '23030' #인천 미추홀구 / 남구
	if sgg_cd == '38110' and year is not None and year < 2010: sgg_cd = '38010' #창원시
	if sgg_cd == '31260' and year is not None and year < 2005: sgg_cd = '31310' #양주시 / 양주군
	if sgg_cd == '31250' and year is not None and year < 2005: sgg_cd = '31340' #광주시 / 광주군
	if sgg_cd == '31240' and year is not None and year < 2005: sgg_cd = '31330' #화성시
	if sgg_cd == '32570': sgg_cd = '32370' #화천군
	if sgg_cd == '22520': sgg_cd = '37310' #군위군
	if sgg_cd == '21510': sgg_cd = '21310' #기장군
	if sgg_cd == '22510': sgg_cd = '22310' #달성군
	if sgg_cd == '23510': sgg_cd = '23310' #강화군
	if sgg_cd == '23520': sgg_cd = '23320' #옹진군
	if sgg_cd == '26510': sgg_cd = '26310' #울주군
	if sgg_cd == '31550': sgg_cd = '31350' #연천군
	if sgg_cd == '31570': sgg_cd = '31370' #가평군
	if sgg_cd == '31580': sgg_cd = '31380' #양평군
	if sgg_cd == '32510': sgg_cd = '32310' #홍천군
	if sgg_cd == '32520': sgg_cd = '32320' #횡성군
	if sgg_cd == '32530': sgg_cd = '32330' #영월군
	if sgg_cd == '32540': sgg_cd = '32340' #평창군
	if sgg_cd == '32550': sgg_cd = '32350' #정선군
	if sgg_cd == '32560': sgg_cd = '32360' #칠원군
	if sgg_cd == '32580': sgg_cd = '32380' #양구군
	if sgg_cd == '32590': sgg_cd = '32390' #인제군
	if sgg_cd == '32600': sgg_cd = '32400' #고성군
	if sgg_cd == '32610': sgg_cd = '32410' #양양군
	if sgg_cd == '33520': sgg_cd = '33320' #보은군
	if sgg_cd == '33530': sgg_cd = '33330' #옥천군
	if sgg_cd == '33540': sgg_cd = '33340' #영동군
	if sgg_cd == '33550': sgg_cd = '33350' #진천군
	if sgg_cd == '33560': sgg_cd = '33360' #괴산군
	if sgg_cd == '33570': sgg_cd = '33370' #음성군
	if sgg_cd == '33580': sgg_cd = '33380' #단양군
	if sgg_cd == '33590': sgg_cd = '33390' #증평군
	if sgg_cd == '34510': sgg_cd = '34310' #금산군
	if sgg_cd == '34530': sgg_cd = '34330' #부여군
	if sgg_cd == '34540': sgg_cd = '34340' #서천군
	if sgg_cd == '34550': sgg_cd = '34350' #청양군
	if sgg_cd == '34560': sgg_cd = '34360' #홍성군
	if sgg_cd == '34570': sgg_cd = '34370' #예산군
	if sgg_cd == '34580': sgg_cd = '34380' #태안군
	if sgg_cd == '35510': sgg_cd = '35310' #완주군
	if sgg_cd == '35520': sgg_cd = '35320' #진안군
	if sgg_cd == '35530': sgg_cd = '35330' #무주군
	if sgg_cd == '35540': sgg_cd = '35340' #장수군
	if sgg_cd == '35550': sgg_cd = '35350' #임실군
	if sgg_cd == '35560': sgg_cd = '35360' #순창군
	if sgg_cd == '35570': sgg_cd = '35370' #고창군
	if sgg_cd == '35580': sgg_cd = '35380' #부안군
	if sgg_cd == '36510': sgg_cd = '36310' #담양군
	if sgg_cd == '36520': sgg_cd = '36320' #곡성군
	if sgg_cd == '36530': sgg_cd = '36330' #구례군
	if sgg_cd == '36550': sgg_cd = '36350' #고흥군
	if sgg_cd == '36560': sgg_cd = '36360' #보성군
	if sgg_cd == '36570': sgg_cd = '36370' #화순군
	if sgg_cd == '36580': sgg_cd = '36380' #장흥군
	if sgg_cd == '36590': sgg_cd = '36390' #강진군
	if sgg_cd == '36600': sgg_cd = '36400' #해남군
	if sgg_cd == '36610': sgg_cd = '36410' #영암군
	if sgg_cd == '36620': sgg_cd = '36420' #무안군
	if sgg_cd == '36630': sgg_cd = '36430' #함평군
	if sgg_cd == '36640': sgg_cd = '36440' #영광군
	if sgg_cd == '36650': sgg_cd = '36450' #장성군
	if sgg_cd == '36660': sgg_cd = '36460' #완도군
	if sgg_cd == '36670': sgg_cd = '36470' #진도군
	if sgg_cd == '36680': sgg_cd = '36480' #신안군
	if sgg_cd == '37520': sgg_cd = '37320' #의성군
	if sgg_cd == '37530': sgg_cd = '37330' #청송군
	if sgg_cd == '37540': sgg_cd = '37340' #영양군
	if sgg_cd == '37550': sgg_cd = '37350' #영덕군
	if sgg_cd == '37560': sgg_cd = '37360' #청도군
	if sgg_cd == '37570': sgg_cd = '37370' #고령군
	if sgg_cd == '37580': sgg_cd = '37380' #성주군
	if sgg_cd == '37590': sgg_cd = '37390' #칠곡군
	if sgg_cd == '37600': sgg_cd = '37400' #예천군
	if sgg_cd == '37610': sgg_cd = '37410' #봉화군
	if sgg_cd == '37620': sgg_cd = '37420' #울진군
	if sgg_cd == '37630': sgg_cd = '37430' #울릉군
	if sgg_cd == '38510': sgg_cd = '38310' #의령군
	if sgg_cd == '38520': sgg_cd = '38320' #함안군
	if sgg_cd == '38530': sgg_cd = '38330' #창녕군
	if sgg_cd == '38540': sgg_cd = '38340' #고성군
	if sgg_cd == '38550': sgg_cd = '38350' #남해군
	if sgg_cd == '38560': sgg_cd = '38360' #하동군
	if sgg_cd == '38570': sgg_cd = '38370' #산청군
	if sgg_cd == '38580': sgg_cd = '38380' #함양군
	if sgg_cd == '38590': sgg_cd = '38390' #거창군
	if sgg_cd == '38600': sgg_cd = '38400' #합천군

	return sgg_cd