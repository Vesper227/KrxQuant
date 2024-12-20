import pandas as pd

def low_per_strategy(data, date, max_stocks=20):
    """
    저 PER 전략: PER 값이 낮은 순으로 최대 max_stocks 개수만큼 선택.

    Args:
        data (pd.DataFrame): 종목 데이터.
        date (str): 전략 실행 날짜.
        max_stocks (int): 선택할 최대 종목 수.

    Returns:
        pd.DataFrame: 선택된 종목 데이터.
    """
    # 특정 날짜의 데이터 필터링
    # filtered = data[data.index == date]

    selected = data.nsmallest(max_stocks, "PER")  # PER 기준 상위 max_stocks 선택

    return selected


def low_per_high_div_strategy(data, date, max_stocks=20):
    """
    저 PER + 고 배당 전략: PER이 낮고 배당 수익률이 높은 순으로 선택.

    Args:
        data (pd.DataFrame): 종목 데이터.
        date (str): 전략 실행 날짜.
        max_stocks (int): 선택할 최대 종목 수.

    Returns:
        pd.DataFrame: 선택된 종목 데이터.
    """
    # 특정 날짜의 데이터 필터링
    # filtered = data[data.index == date]

    # PER 하위 50% + 배당수익률 상위 50%
    per_filtered = data.nsmallest(len(data) // 2, "PER")
    selected = per_filtered.nlargest(max_stocks, "DIV")

    return selected

def combined_score_strategy(data, date, max_stocks=20):
    """
    다중 팩터 전략: PER, PBR, ROE, 모멘텀 결합
    """
    monthly_data = data.loc[data.index == date].copy()
    if monthly_data.empty:
        return pd.DataFrame()

    monthly_data["PER_score"] = 1 / monthly_data["PER"]
    monthly_data["PBR_score"] = 1 / monthly_data["PBR"]
    monthly_data["ROE_score"] = monthly_data["EPS"] / monthly_data["BPS"]
    monthly_data["Momentum_score"] = monthly_data["ChangeRate"].rolling(12).sum()

    monthly_data["Total_score"] = (
        0.3 * monthly_data["PER_score"] +
        0.3 * monthly_data["PBR_score"] +
        0.2 * monthly_data["ROE_score"] +
        0.2 * monthly_data["Momentum_score"]
    )

    selected = monthly_data.nlargest(20, "Total_score")
    return selected


def quality_value_strategy(data):
    """
    품질 + 가치 전략: ROE, 부채비율, PER, PBR, 배당수익률 기준으로 점수화.
    """
    # 점수 계산
    data['ROE_Rank'] = data.groupby('Date')['ROE'].rank(ascending=False, na_option='bottom')
    data['Debt_Rank'] = data.groupby('Date')['DebtRatio'].rank(ascending=True, na_option='bottom')
    data['PER_Rank'] = data.groupby('Date')['PER'].rank(ascending=True, na_option='bottom')
    data['PBR_Rank'] = data.groupby('Date')['PBR'].rank(ascending=True, na_option='bottom')
    data['Dividend_Rank'] = data.groupby('Date')['DividendYield'].rank(ascending=False, na_option='bottom')

    # 종합 점수 계산
    data['Combined_Score'] = (
        data['ROE_Rank'] * 0.3 +
        data['Debt_Rank'] * 0.2 +
        data['PER_Rank'] * 0.2 +
        data['PBR_Rank'] * 0.2 +
        data['Dividend_Rank'] * 0.1
    )

    # 상위 30% 종목 선별
    data['Rank'] = data.groupby('Date')['Combined_Score'].rank(ascending=True, na_option='bottom')
    top_30_percent = data[data['Rank'] <= len(data) * 0.3]
    
    return top_30_percent