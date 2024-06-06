import pandas as pd
import os

"""
#csv파일 읽기
df = pd.read_csv('강남_data_summary.csv')

#년월을 기준으로 그룹화
grouped_df = df.groupby('년월', as_index=False, observed=True).agg({
    '전용면적': 'sum',
    '거래금액': 'sum',
    '데이터 건수': 'sum'
})

#평당 거래가
grouped_df['평당 거래가'] = grouped_df['거래금액'] / (grouped_df['전용면적'] / 3.3)

#필요한 열만 선택 후 저장
final_df = grouped_df[['년월','데이터 건수','평당 거래가']]
final_df.to_csv('강남요약_data.csv', index=False)
"""




# 데이터 처리 함수
def process_data(file_path):
    # CSV 파일 읽기
    df = pd.read_csv(file_path)

    # 년월을 기준으로 그룹화
    grouped_df = df.groupby('년월', as_index=False, observed=True).agg({
        '전용면적': 'sum',
        '거래금액': 'sum',
        '데이터 건수': 'sum'
    })

    # 평당 거래가 계산
    grouped_df['평당 거래가'] = grouped_df['거래금액'] / (grouped_df['전용면적'] / 3.3)

    # 필요한 열만 선택
    final_df = grouped_df[['년월', '데이터 건수', '평당 거래가']]

    # 'output' 폴더 확인 및 생성
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 결과를 새 CSV 파일로 저장
    output_filename = os.path.splitext(os.path.basename(file_path))[0] + '간단.csv'
    final_df.to_csv(os.path.join('output', output_filename), index=False)
    print(f'Processed and saved: {output_filename}')


# 모든 파일 처리
def process_all_files(directory):
    # 지정된 디렉토리에서 모든 CSV 파일 목록 생성
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

    # 각 파일에 대해 데이터 처리 함수 실행
    for file in files:
        process_data(file)


# 메인 함수 호출
if __name__ == "__main__":

    data_directory = 'C:\\HB_Study\\real_estate_project\\data_preprocessing\\Regional_data\\인천'
    process_all_files(data_directory)

