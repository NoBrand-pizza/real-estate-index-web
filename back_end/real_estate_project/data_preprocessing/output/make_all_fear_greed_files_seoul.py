import pandas as pd
import os
import glob


def load_and_concatenate_files(directory, pattern):
    """
    주어진 디렉토리에서 패턴에 맞는 모든 CSV 파일을 로드하고 결합합니다.
    """
    all_files = glob.glob(os.path.join(directory, pattern))
    if not all_files:
        raise FileNotFoundError(f"No files found for pattern: {pattern}")
    df_list = [pd.read_csv(file) for file in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df


def calculate_min_max_values(df):
    """
    데이터프레임을 받아 각 지표의 최소값과 최대값을 계산합니다.
    """
    df['price_change'] = df['평당 거래가'].pct_change() * 100
    df['volatility_3m'] = df['price_change'].rolling(window=3).std()
    df['volatility_12m'] = df['price_change'].rolling(window=12).std()
    df['volume_3m'] = df['데이터 건수'].rolling(window=3).mean()
    df['volume_12m'] = df['데이터 건수'].rolling(window=12).mean()
    df['momentum_2m'] = df['price_change'].rolling(window=2).mean() * df['데이터 건수'].rolling(window=2).mean()
    df['volatility_diff'] = df['volatility_3m'] - df['volatility_12m']
    df['volume_diff'] = df['volume_3m'] - df['volume_12m']
    min_max_values = {
        'volatility_diff': (df['volatility_diff'].min(), df['volatility_diff'].max()),
        'volume_diff': (df['volume_diff'].min(), df['volume_diff'].max()),
        'momentum_2m': (df['momentum_2m'].min(), df['momentum_2m'].max())
    }
    return min_max_values


def scale_value(value, min_value, max_value):
    """
    Min-Max Scaling을 사용하여 값을 0~100 사이로 변환합니다.
    """
    if max_value - min_value == 0:
        return 50  # 변화가 없으면 중간값 50을 반환
    return (value - min_value) / (max_value - min_value) * 100


def process_real_estate_data(region, input_date, min_max_values):
    """
    부동산 데이터를 받아 특정 날짜에 대한 요약 정보를 계산합니다.
    """
    file_path = f'{region}_data_summary간단.csv'
    real_estate_data = pd.read_csv(file_path)
    real_estate_data['년월'] = pd.to_datetime(real_estate_data['년월'], format='%Y%m')
    real_estate_data.sort_values(by='년월', inplace=True)
    real_estate_data['price_change'] = real_estate_data['평당 거래가'].pct_change() * 100
    input_date = pd.to_datetime(input_date, format='%Y%m')
    target_index = real_estate_data[real_estate_data['년월'] == input_date].index[0]
    window_3m_start = max(0, target_index - 2)
    window_12m_start = max(0, target_index - 11)
    data_3m = real_estate_data.iloc[window_3m_start:target_index + 1].copy()
    data_12m = real_estate_data.iloc[window_12m_start:target_index + 1].copy()
    data_2m = real_estate_data.iloc[max(0, target_index - 1):target_index + 1].copy()
    volatility_3m = data_3m['price_change'].std()
    volatility_12m = data_12m['price_change'].std()
    volume_3m = data_3m['데이터 건수'].mean()
    volume_12m = data_12m['데이터 건수'].mean()
    data_2m.loc[:, 'momentum_2m'] = data_2m['price_change'].mean() * data_2m['데이터 건수'].mean()
    momentum_2m = data_2m['momentum_2m'].mean()
    volatility_diff = volatility_3m - volatility_12m
    volume_diff = volume_3m - volume_12m
    scaled_momentum_2m = scale_value(momentum_2m, *min_max_values['momentum_2m'])
    scaled_volatility_diff = scale_value(volatility_diff, *min_max_values['volatility_diff'])
    scaled_volume_diff = scale_value(volume_diff, *min_max_values['volume_diff'])
    summary = {
        "년월": input_date.strftime('%Y%m'),
        "Date": input_date,
        "Scaled_Volatility_Diff": scaled_volatility_diff,
        "Scaled_Volume_Diff": scaled_volume_diff,
        "Scaled_Momentum_2m": scaled_momentum_2m,
        "평당 거래가": real_estate_data.loc[target_index, '평당 거래가']
    }
    summary_df = pd.DataFrame([summary])
    return summary_df


def main():
    """
    메인 함수: 모든 지역 데이터를 처리하고 각 지역별로 fear_greed_index_full.csv 파일을 생성합니다.
    """
    directory = ''  # 현재 디렉토리
    patterns = "*_data_summary간단.csv"  # 모든 지역 파일 패턴
    date_range = pd.date_range(start='2015-12-01', end='2024-02-01', freq='MS').strftime('%Y%m').tolist()
    weights = {
        'Scaled_Volatility_Diff': 0.125,
        'Scaled_Volume_Diff': 0.125,
        'Scaled_Momentum_2m': 0.35,
        '검색량_평균': 0.15,
        'normalized_DT': 0.25
    }

    # 심리지수 데이터 로드
    df_psychology = pd.read_csv("standardized_data.csv")
    df_psychology = df_psychology[df_psychology['C1_NM'] == '서울특별시']  # '인천광역시' 행들만 필터링
    df_psychology['년월'] = pd.to_datetime(df_psychology['PRD_DE'], format='%Y-%m-%d').dt.strftime('%Y%m')
    psychology_data = df_psychology[['년월', 'normalized_DT']]

    for pattern in glob.glob(os.path.join(directory, patterns)):
        region = os.path.basename(pattern).split('_')[0]
        df_combined = load_and_concatenate_files(directory, pattern)
        min_max_values = calculate_min_max_values(df_combined)
        final_results = pd.DataFrame()

        for date in date_range:
            try:
                real_estate_summary = process_real_estate_data(region, date, min_max_values)

                # 부동산 검색량 데이터 로드 및 필터링
                trend_csv = '00부동산_검색량_월별_요약.csv'
                df_trend = pd.read_csv(trend_csv)
                df_trend['년월'] = df_trend['년월'].astype(str)
                search_trend_data = df_trend[['년월', '검색량_평균']]

                # 해당 년월의 검색량 데이터와 심리지수 데이터 필터링
                filtered_search_trend = search_trend_data[search_trend_data['년월'] == real_estate_summary.iloc[0]['년월']]
                filtered_psychology = psychology_data[psychology_data['년월'] == real_estate_summary.iloc[0]['년월']]

                if not filtered_search_trend.empty and not filtered_psychology.empty:
                    filtered_search_trend = filtered_search_trend.reset_index(drop=True)
                    filtered_psychology = filtered_psychology.reset_index(drop=True)
                    real_estate_summary = real_estate_summary.reset_index(drop=True)

                    # 데이터 병합
                    merged_data = pd.merge(real_estate_summary, filtered_search_trend, on='년월')
                    merged_data = pd.merge(merged_data, filtered_psychology, on='년월')
                    merged_data['fear_greed_index'] = (
                            merged_data['Scaled_Volatility_Diff'] * weights['Scaled_Volatility_Diff'] +
                            merged_data['Scaled_Volume_Diff'] * weights['Scaled_Volume_Diff'] +
                            merged_data['Scaled_Momentum_2m'] * weights['Scaled_Momentum_2m'] +
                            merged_data['검색량_평균'] * weights['검색량_평균'] +
                            merged_data['normalized_DT'] * weights['normalized_DT']
                    )
                    final_results = pd.concat([final_results, merged_data], ignore_index=True)
            except Exception as e:
                print(f"Error processing date {date} for region {region}: {e}")

        output_file_path = f'{region}_fear_greed_index_full.csv'
        final_results.to_csv(output_file_path, index=False)
        print(f"Final data for {region} saved to: {output_file_path}")


if __name__ == "__main__":
    main()
