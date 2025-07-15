import pandas as pd

def load_data(master_path, code_a_path, code_b_path):
	master = pd.read_csv(master_path)
	code_a = pd.read_csv(code_a_path)
	code_b = pd.read_csv(code_b_path)
	return master, code_a, code_b

def build_code_dicts(code_b):
	code_dicts = {}
	for code in code_b['cd_a'].unique():
		sub = code_b[code_b['cd_a'] == code][['cd_b', 'cd_nm']].copy()
		sub['cd_b'] = pd.to_numeric(sub['cd_b'], errors='coerce').astype('Int64')
		code_dicts[code] = sub.set_index('cd_b')['cd_nm'].to_dict()
	return code_dicts

def map_codes(df, mapping_plan, code_dicts):
	for col, code_key in mapping_plan.items():
		df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
		df[col] = df[col].map(code_dicts.get(code_key, {}))
	return df

def rename_columns(df, rename_map):
	return df.rename(columns=rename_map)

def preprocess_traveller_data(master_path, code_a_path, code_b_path, output_path=None):
	'''
	사용방법은 master_path = 바꿀 csv파일경로
					code_a_path = code_a 파일경로
					code_b_path = code_b 파일경로
					output_path = 전처리된 파일 출력경로
					
	'''
	# 1. Load
	df, code_a, code_b = load_data(master_path, code_a_path, code_b_path)
	# 2. Build code dictionaries
	code_dicts = build_code_dicts(code_b)

	# 3. Define mapping plan (col: code_key) 숫자형 -> 범주형라벨로 매핑
	mapping_plan = {
    'MVMN_CD_1': 'MOV',
    'MVMN_CD_2': 'MOV'
	}

	# 4. Map codes
	df = map_codes(df, mapping_plan, code_dicts)

	# 5. Rename columns 컬럼코드 -> 한글로 변경
	rename_map = {
    'TRAVEL_ID': '여행ID',
    'TRIP_ID': 'Trip ID',
    'START_VISIT_AREA_ID': '출발 방문지 ID',
    'END_VISIT_AREA_ID': '도착 방문지 ID',
    'START_DT_MIN': '출발시간_분',
    'END_DT_MIN': '도착시간_분',
    'MVMN_CD_1': '이동방법코드_1',
    'MVMN_CD_2': '이동방법코드_2'
	}
	df = rename_columns(df, rename_map)

	# 6. Save if output_path is given
	if output_path:
		df.to_csv(output_path, index=False, encoding='utf-8')

	return df

def preprocess_traveller_data2(master_path, code_a_path, code_b_path, output_path=None):
	'''
	사용방법은 master_path = 바꿀 csv파일경로
					code_a_path = code_a 파일경로
					code_b_path = code_b 파일경로
					output_path = 전처리된 파일 출력경로
					
	'''
	# 1. Load
	df, code_a, code_b = load_data(master_path, code_a_path, code_b_path)
	# 2. Build code dictionaries
	code_dicts = build_code_dicts(code_b)

	# 3. Define mapping plan (col: code_key) 숫자형 -> 범주형라벨로 매핑
	mapping_plan = {
    'LODGING_TYPE_CD': 'HTY',
    'PAYMENT_MTHD_SE': 'PAY'
	}

	# 4. Map codes
	df = map_codes(df, mapping_plan, code_dicts)

	# 5. Rename columns 컬럼코드 -> 한글로 변경
	rename_map = {
    "TRAVEL_ID": "여행ID",
    "LODGING_NM": "숙소명",
    "LODGING_PAYMENT_SEQ": "숙박경비순번",
    "LODGING_TYPE_CD": "숙소유형코드",
    "RSVT_YN": "예약여부",
    "CHK_IN_DT_MIN": "체크인시간_분",
    "CHK_OUT_DT_MIN": "체크아웃시간_분",
    "PAYMENT_NUM": "소비인원",
    "BRNO": "사업자등록번호",
    "STORE_NM": "상호명",
    "ROAD_NM_ADDR": "도로명주소",
    "LOTNO_ADDR": "지번주소",
    "ROAD_NM_CD": "도로명코드",
    "LOTNO_CD": "지번코드",
    "PAYMENT_DT": "결제일시_분",
    "PAYMENT_MTHD_SE": "결제방식구분",
    "PAYMENT_AMT_WON": "결제금액_원",
    "PAYMENT_ETC": "소비내역_기타"
	}
	df = rename_columns(df, rename_map)

	# 6. Save if output_path is given
	if output_path:
		df.to_csv(output_path, index=False, encoding='utf-8')

	return df


preprocess_traveller_data("./preprocessing/tn_move_his_이동내역_B.csv", "./preprocessing/tc_codea_코드A.csv", "./preprocessing/tc_codeb_코드B.csv", output_path = "./preprocessing/이동내역_변경.csv")
preprocess_traveller_data2("./preprocessing/tn_lodge_consume_his_숙박소비내역_B.csv", "./preprocessing/tc_codea_코드A.csv", "./preprocessing/tc_codeb_코드B.csv", output_path = "./preprocessing/숙박소비내역_변경.csv")
