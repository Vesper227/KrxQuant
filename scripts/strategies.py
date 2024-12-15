import pandas as pd
import logging

# 각 전략별 로그 파일 설정
low_per_logger = logging.getLogger("low_per")
low_per_logger.addHandler(logging.FileHandler("low_per_strategy.log", encoding="utf-8"))
low_per_logger.setLevel(logging.INFO)

high_div_logger = logging.getLogger("low_per_high_div")
high_div_logger.addHandler(logging.FileHandler("low_per_high_div_strategy.log", encoding="utf-8"))
high_div_logger.setLevel(logging.INFO)

combined_score_logger = logging.getLogger("combined_score")
combined_score_logger.addHandler(logging.FileHandler("combined_score_strategy.log", encoding="utf-8"))
combined_score_logger.setLevel(logging.INFO)

def low_per_strategy(data, date):
    """저PER 전략: 월별 PER 하위 30% 선택"""
    monthly_data = data.loc[data.index == date]
    if monthly_data.empty:
        return pd.DataFrame()
    #selected = monthly_data.nsmallest(len(monthly_data) // 3, "PER")
    selected = monthly_data.nlargest(20, "PER")
    low_per_logger.info(f"{date}: Selected {len(selected)} stocks.")
    return selected

def low_per_high_div_strategy(data, date):
    """저PER + 고배당 전략"""
    monthly_data = data.loc[data.index == date]
    if monthly_data.empty:
        return pd.DataFrame()
    # PER 하위 50% + 배당수익률 상위 50%
    per_filtered = monthly_data.nsmallest(len(monthly_data) // 2, "PER")
    selected = per_filtered.nlargest(len(per_filtered) // 2, "DIV")
    high_div_logger.info(f"{date}: Selected {len(selected)} stocks.")
    return selected

def combined_score_strategy(data, date):
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
    combined_score_logger.info(f"{date}: Selected {len(selected)} stocks.")
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