import pandas as pd

def load_data(master_input, code_a_path, code_b_path):
    """
    master_input: str (경로) 또는 pd.DataFrame
    """
    # master는 경로 or DataFrame
    if isinstance(master_input, str):
        df = pd.read_csv(master_input)
    elif isinstance(master_input, pd.DataFrame):
        df = master_input.copy()
    else:
        raise ValueError("master_input must be a file path or a DataFrame")

    code_a = pd.read_csv(code_a_path)
    code_b = pd.read_csv(code_b_path)

    return df, code_a, code_b

def build_code_dicts(code_b):
    code_dicts = {}
    for code in code_b['cd_a'].unique():
        sub = code_b[code_b['cd_a'] == code][['cd_b', 'cd_nm']].copy()
        sub['cd_b'] = pd.to_numeric(sub['cd_b'], errors='coerce').astype('Int64')
        code_dicts[code] = sub.set_index('cd_b')['cd_nm'].to_dict()
    return code_dicts

def map_codes(df, mapping_plan, code_dicts):
    for col, code_key in mapping_plan.items():
        if col not in df.columns:
            print(f"컬럼 '{col}'이 DataFrame에 존재하지 않습니다. 건너뜁니다.")
            continue
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            df[col] = df[col].map(code_dicts.get(code_key, {}))
        except Exception as e:
            print(f"컬럼 '{col}' 매핑 중 오류 발생: {e}")
    return df

def rename_columns(df, rename_map):
    return df.rename(columns=rename_map)

def preprocess_data(master_input, code_a_path, code_b_path,
                    mapping_plan, rename_map,
                    output_path=None):
    """
    Parameters:
    - master_input (str or DataFrame): 마스터 데이터 (경로 또는 이미 로드된 DataFrame)
    - code_a_path (str): 코드 A CSV 경로
    - code_b_path (str): 코드 B CSV 경로
    - mapping_plan (dict): 컬럼별 코드 매핑 dict
    - rename_map (dict): 컬럼명 변경 dict
    - output_path (str, optional): 저장 경로

    Returns:
    - df (pd.DataFrame): 전처리된 결과
    """
    # 1. Load
    df, code_a, code_b = load_data(master_input, code_a_path, code_b_path)

    # 2. Build dictionary from code B
    code_dicts = build_code_dicts(code_b)

    # 3. Map codes
    df = map_codes(df, mapping_plan, code_dicts)

    # 4. Rename columns
    df = rename_columns(df, rename_map)

    # 5. Save if needed
    if output_path:
        df.to_csv(output_path, index=False, encoding='utf-8')

    return df
