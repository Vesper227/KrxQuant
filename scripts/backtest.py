
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import logging
import os
from datetime import datetime
from strategies import low_per_strategy, low_per_high_div_strategy, combined_score_strategy

selected_strategy = combined_score_strategy  # 전략 함수 지정

# 로깅 디렉토리 및 파일 설정
strategy_name = selected_strategy.__name__
current_time = datetime.now().strftime("%Y-%m-%d_%H%M%S")
log_dir = f"logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{current_time}_{strategy_name}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# 데이터베이스 연결
db_path = "krx_data.db"
conn = sqlite3.connect(db_path)

# 데이터 읽기
query = """
SELECT * FROM stock_monthly_data
WHERE PER IS NOT NULL
ORDER BY Date, Ticker
"""
data = pd.read_sql(query, conn, parse_dates=["Date"])
data.set_index("Date", inplace=True)

def filter_data(data):
    """
    결측치와 극단적인 값을 제외하는 함수
    """
    # PER, EPS, PBR이 0 이하인 데이터 제거
    filtered = data[(data["PER"] > 0) & (data["EPS"] > 0) & (data["PBR"] > 0)]

    # 극단적인 PER 값 필터링 (상/하위 5% 제거)
    per_lower_bound = filtered["PER"].quantile(0.05)
    per_upper_bound = filtered["PER"].quantile(0.95)
    filtered = filtered[(filtered["PER"] >= per_lower_bound) & (filtered["PER"] <= per_upper_bound)]
    
    return filtered

def calculate_drawdown(portfolio_values):
    """
    MDD 값을 계산하는 함수
    """
    global peak, max_drawdown, mdd_date
    drawdowns = []
    for i, value in enumerate(portfolio_values):
        peak = max(peak, value)
        drawdown = (peak - value) / peak
        drawdowns.append(drawdown)
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            mdd_date = dates[i]
    return drawdowns

# 백테스트 기간
start_date = '2015-01-01'
end_date = '2024-11-30'
data = data[(data.index >= start_date) & (data.index <= end_date)]
data = filter_data(data)

# 백테스트 수행
portfolio_values = []
dates = sorted(data.index.unique())
initial_cash = 10_000_000  # 1000만 원
peak = initial_cash  # 초기 자산 설정
max_drawdown = 0
mdd_date = None
risk_free_rate = 0.03  # 무위험 수익률(연 3%)

for date in dates:
    portfolio = selected_strategy(data, date)  # 전략 적용
    if portfolio.empty:
        portfolio_values.append(initial_cash)
        continue

    # 포트폴리오 비중 및 수익률 계산
    total_value = portfolio["Close"].sum()
    portfolio["Weight"] = portfolio["Close"] / total_value
    portfolio_return = (portfolio["ChangeRate"] / 100 * portfolio["Weight"]).sum()
    initial_cash = initial_cash * (1 + portfolio_return)
    portfolio_values.append(initial_cash)

    # 매달 선정된 종목과 정보 출력
    selected_stocks = portfolio[["Ticker", "Name", "Close", "PER", "PBR", "DIV", "EPS"]]
    logging.info(f"{date}: Portfolio Value = {initial_cash:,.2f}")
    logging.info(f"Selected Stocks:\n{selected_stocks.to_string(index=False)}")

# MDD 계산
portfolio_drawdowns = calculate_drawdown(portfolio_values)

# 결과 저장
results = pd.DataFrame({"Date": dates, "Portfolio Value": portfolio_values, "Drawdown": portfolio_drawdowns})
results.set_index("Date", inplace=True)

# 수익률 출력
total_return = (results["Portfolio Value"].iloc[-1] / results["Portfolio Value"].iloc[0]) - 1
print(f"Total Return: {total_return:.2%}")
print(f"Maximum Drawdown: {max_drawdown:.2%} on {mdd_date}")

# 샤프 지수 계산
results["Monthly Return"] = results["Portfolio Value"].pct_change() # 월간 수익률 계산
results["Excess Return"] = results["Monthly Return"] - (risk_free_rate / 12) # 초과 수익률 계산
sharpe_ratio = results["Excess Return"].mean() / results["Excess Return"].std() * (12 ** 0.5)  # 연간화
print(f"Sharpe Ratio: {sharpe_ratio:.4f}")

# 샤프 지수 로그 기록
logging.info(f"Sharpe Ratio: {sharpe_ratio:.4f}")

# 그래프 스타일 설정
plt.style.use('default')  # 스타일 시트 적용 (예: 'ggplot', 'seaborn', 'default' 등)

# 그래프 시각화
plt.figure(figsize=(14, 8))  # 그래프 크기 확장
plt.plot(results.index, results["Portfolio Value"], label="Portfolio", color="blue", linewidth=2)
plt.scatter(mdd_date, results.loc[mdd_date, "Portfolio Value"], color="red", 
            label=f"MDD ({max_drawdown:.2%})", zorder=5, s=100)  # MDD 포인트 강조

# x축과 y축 값 표시
plt.xticks(rotation=45, fontsize=10)  # x축 날짜 회전 및 폰트 크기
plt.yticks(fontsize=10)  # y축 폰트 크기
plt.ticklabel_format(axis='y', style='plain')  # y축 값 숫자 형태로 표시

# 제목 및 라벨
plt.title("Backtest Results", fontsize=16, fontweight='bold')
plt.xlabel("Date", fontsize=12)
plt.ylabel("Portfolio Value (KRW)", fontsize=12)

# 범례 설정
plt.legend(fontsize=10, loc="upper left", frameon=True, shadow=True)

# 샤프 지수 추가 (우측 상단)
plt.figtext(0.85, 0.95, f"Sharpe Ratio: {sharpe_ratio:.4f}", 
            fontsize=12, color="green", ha="right", fontweight="bold", backgroundcolor="white")

# 그리드 스타일 조정
plt.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.7)

# 그래프 출력
plt.tight_layout()  # 여백 조정
plt.show()


# 데이터베이스 연결 종료
conn.close()
