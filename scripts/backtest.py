import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import logging
import os
from datetime import datetime
from strategies import low_per_strategy, low_per_high_div_strategy, combined_score_strategy

# ------------------ 설정 및 초기화 ------------------

# 전략 설정
selected_strategy = combined_score_strategy
strategy_name = selected_strategy.__name__

# 로깅 설정
current_time = datetime.now().strftime("%Y-%m-%d_%H%M%S")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{current_time}_{strategy_name}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# 데이터베이스 설정
db_path = "data/krx_data.db"
conn = sqlite3.connect(db_path)

# ------------------ 함수 정의 ------------------

def load_data(conn, start_date, end_date):
    """데이터베이스에서 데이터를 불러와 필터링"""
    query = """
    SELECT * FROM stock_monthly_data
    WHERE PER IS NOT NULL
    ORDER BY Date, Ticker
    """
    data = pd.read_sql(query, conn, parse_dates=["Date"])
    data.set_index("Date", inplace=True)
    data = data[(data.index >= start_date) & (data.index <= end_date)]
    return filter_data(data)

def filter_data(data):
    """결측치와 극단치를 필터링"""
    filtered = data[(data["PER"] > 0) & (data["EPS"] > 0) & (data["PBR"] > 0)]
    per_bounds = filtered["PER"].quantile([0.05, 0.95])
    return filtered[(filtered["PER"] >= per_bounds[0.05]) & (filtered["PER"] <= per_bounds[0.95])]

def apply_trading_cost(price, num_shares, slippage=0.001, fee_rate=0.001):
    """슬리피지 및 거래 비용 적용"""
    executed_price = price * (1 + slippage)
    trade_amount = executed_price * num_shares
    total_cost = trade_amount + (trade_amount * fee_rate)
    return executed_price, total_cost

def calculate_drawdown(portfolio_values):
    """최대 낙폭 계산"""
    peak, max_drawdown, drawdowns = portfolio_values[0], 0, []
    for value in portfolio_values:
        peak = max(peak, value)
        drawdown = (peak - value) / peak
        drawdowns.append(drawdown)
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown, drawdowns

# ------------------ 백테스트 실행 ------------------

def sell_all_holdings(holdings, data, current_date):
    """모든 보유 주식을 매도하고 현금으로 전환"""
    cash_from_sales = 0
    for ticker, shares in holdings.items():
        try:
            # 현재 월 종가 기준으로 매도
            close_price = data.loc[(data.index == current_date) & (data['Ticker'] == ticker), 'Close'].iloc[0]
            cash_from_sales += shares * close_price
        except Exception as e:
            logging.warning(f"Failed to sell Ticker {ticker} on {current_date}: {e}")
    holdings.clear()  # 매도 후 holdings 초기화
    return cash_from_sales


# 초기 설정
start_date, end_date = '2024-01-01', '2024-11-30'
initial_cash = 10_000_000
cash, peak = initial_cash, initial_cash
holdings, portfolio_values = {}, []
dates = sorted(load_data(conn, start_date, end_date).index.unique())

monthly_returns = []   # 월별 수익률 기록
previous_value = initial_cash  # 초기 포트폴리오 가치

logging.info("백테스트 시작")
data = load_data(conn, start_date, end_date)

for date in dates:
    portfolio = selected_strategy(data, date)

    # 기존 보유 주식 매도 후 현금화
    cash += sell_all_holdings(holdings, data, date)

    # 포트폴리오가 비어있는 경우
    if portfolio.empty:
        portfolio_values.append(cash)
        monthly_returns.append(0.0)  # 수익률 0%로 기록
        previous_value = cash
        logging.info(f"No stocks selected on {date}. Portfolio Value: {cash:,.2f}")
        continue

    # 매수 및 자금 분배
    allocation = cash / len(portfolio)
    for _, row in portfolio.iterrows():
        ticker, price = row['Ticker'], row['Close']
        num_shares = int(allocation // price)

        if num_shares > 0:
            executed_price, total_cost = apply_trading_cost(price, num_shares)
            cash -= total_cost
            holdings[ticker] = num_shares

    # 포트폴리오 가치 계산 (익월말 종가 기준)
    next_month_date = data[data.index > date].index.min()
    portfolio_value = cash + sum(
        holdings[ticker] * data.loc[(data.index == next_month_date) & (data['Ticker'] == ticker), 'Close'].iloc[0]
        if not data.loc[(data.index == next_month_date) & (data['Ticker'] == ticker), 'Close'].empty
        else 0
        for ticker in holdings
    )

    # 월 수익률 계산
    monthly_return = (portfolio_value - previous_value) / previous_value
    monthly_returns.append(monthly_return)

    # 기록 및 갱신
    portfolio_values.append(portfolio_value)
    previous_value = portfolio_value
    
    logging.info(f"{date}: Portfolio Value = {portfolio_value:,.2f}")
    logging.info(f"Selected Stocks:\n{portfolio[['Name', 'Close', 'PER', 'PBR']].to_string(index=False)}")


# ------------------ 결과 분석 및 출력 ------------------

max_drawdown, drawdowns = calculate_drawdown(portfolio_values)
results = pd.DataFrame({
    "Date": dates, "Portfolio Value": portfolio_values, "Drawdown": drawdowns
}).set_index("Date")

total_return = (results["Portfolio Value"].iloc[-1] / results["Portfolio Value"].iloc[0]) - 1
sharpe_ratio = (results["Portfolio Value"].pct_change().mean() - (0.03 / 12)) / results["Portfolio Value"].pct_change().std() * (12 ** 0.5)

print(f"Total Return: {total_return:.2%}")
print(f"Maximum Drawdown: {max_drawdown:.2%}")
print(f"Sharpe Ratio: {sharpe_ratio:.4f}")

# ------------------ 그래프 출력 ------------------
# Drawdown 최댓값의 날짜와 해당 포트폴리오 값
mdd_date = results["Drawdown"].idxmax()
mdd_value = results.loc[mdd_date, "Portfolio Value"]

plt.figure(figsize=(14, 8))
plt.plot(results.index, results["Portfolio Value"], label="Portfolio", color="blue", linewidth=2)
plt.scatter(mdd_date, mdd_value, color="red", label=f"MDD ({max_drawdown:.2%})", s=100)

plt.title("Backtest Results", fontsize=16)
plt.xlabel("Date")
plt.ylabel("Portfolio Value (KRW)")
plt.legend()
plt.grid()
plt.show()

# ------------------ 종료 ------------------
conn.close()
