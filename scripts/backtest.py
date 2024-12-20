import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime
from strategies import low_per_strategy, low_per_high_div_strategy, combined_score_strategy
from tabulate import tabulate

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
    """
    데이터베이스에서 데이터를 로드하고 필터링합니다.
    """
    query = """
    SELECT * FROM stock_monthly_data
    WHERE PER IS NOT NULL
    ORDER BY Date, Ticker
    """
    data = pd.read_sql(query, conn, parse_dates=["Date"])
    data.set_index("Date", inplace=True)

    # 날짜 필터링
    data = data[(data.index >= start_date) & (data.index <= end_date)]

    # 데이터 필터링
    return data

def strategy_filter(data, date):
    """
    특정 날짜의 데이터를 필터링하여 전략에 사용 가능하도록 처리.
    """
    # 날짜 기준 데이터 필터링
    filtered = data.loc[data.index == date]

    # 음수 값 제거
    filtered = filtered[(filtered["PER"] > 0) & (filtered["EPS"] > 0) & (filtered["PBR"] > 0)]

    # 아웃라이어 제거 (상하위 5% 제외)
    if not filtered.empty:
        per_bounds = filtered["PER"].quantile([0.05, 0.95])
        pbr_bounds = filtered["PBR"].quantile([0.05, 0.95])

        filtered = filtered[
            (filtered["PER"] >= per_bounds[0.05]) & (filtered["PER"] <= per_bounds[0.95]) &
            (filtered["PBR"] >= pbr_bounds[0.05]) & (filtered["PBR"] <= pbr_bounds[0.95])
        ]

    # 결측값 제거
    filtered = filtered.dropna()

    return filtered

def apply_trading_cost(price, num_shares, slippage=0.001, fee_rate=0.001):
    """슬리피지 및 거래 비용 반영"""
    executed_price = price * (1 + slippage)
    trade_amount = executed_price * num_shares
    transaction_fee = trade_amount * fee_rate
    total_cost = trade_amount + transaction_fee
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

def calculate_monthly_return(previous_value, current_value):
    """월별 수익률 계산"""
    return (current_value - previous_value) / previous_value if previous_value > 0 else 0.0

def calculate_cagr(initial_value, final_value, start_date, end_date):
    """
    CAGR (연평균 성장률) 계산.

    Args:
        initial_value (float): 초기 포트폴리오 가치.
        final_value (float): 최종 포트폴리오 가치.
        start_date (str): 백테스트 시작 날짜 (YYYY-MM-DD).
        end_date (str): 백테스트 종료 날짜 (YYYY-MM-DD).

    Returns:
        float: CAGR 값.
    """
    # 백테스트 기간 (연수 단위)
    num_years = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.0

    # CAGR 계산
    cagr = (final_value / initial_value) ** (1 / num_years) - 1
    return cagr

import matplotlib.pyplot as plt

def plot_backtest_results(dates, portfolio_values, drawdowns, monthly_returns=None):
    """
    백테스트 결과를 시각화하는 함수.

    Args:
        dates (list): 백테스트 기간의 날짜 리스트.
        portfolio_values (list): 포트폴리오 가치 변화 리스트.
        drawdowns (list): 낙폭(MDD) 값 리스트.
        monthly_returns (list): 월별 수익률 리스트 (옵션).
    """
    # 포트폴리오 가치 변화 그래프
    plt.figure(figsize=(14, 8))
    plt.plot(dates, portfolio_values, label="Portfolio Value", color="blue", linewidth=2)
    
    # MDD 강조
    mdd_date = dates[drawdowns.index(max(drawdowns))]
    mdd_value = min(portfolio_values)
    plt.scatter(mdd_date, mdd_value, color="red", label=f"MDD ({max(drawdowns):.2%})", zorder=5, s=100)

    # 그래프 스타일 설정
    plt.title("Backtest Results: Portfolio Value", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Portfolio Value (KRW)", fontsize=12)
    plt.legend()
    plt.grid(alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 월별 수익률 그래프 (옵션)
    if monthly_returns:
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(monthly_returns)), [x * 100 for x in monthly_returns], color="green", alpha=0.6)
        plt.title("Monthly Returns (%)", fontsize=16)
        plt.xlabel("Month", fontsize=12)
        plt.ylabel("Return (%)", fontsize=12)
        plt.grid(axis="y", alpha=0.7)
        plt.tight_layout()
        plt.show()


# ------------------ 백테스트 실행 ------------------

# 초기 설정
start_date, end_date = '2020-01-01', '2024-11-30'
initial_cash = 10_000_000
cash, holdings = initial_cash, {}
portfolio_values, monthly_returns = [initial_cash], [0.0]  # 초기값 설정

data = load_data(conn, start_date, end_date)
dates = sorted(data.index.unique())

logging.info("백테스트 시작")

for i, date in enumerate(dates):
    if i == len(dates) - 1:
        break  # 마지막 월은 매도만 수행하고 종료

    next_month_date = dates[i + 1]  # 익월말 기준 종가 사용

    # 기존 보유 주식 매도 후 현금화
    for ticker, shares in list(holdings.items()):
        try:
            sell_price = data.loc[(data.index == date) & (data['Ticker'] == ticker), 'Close'].iloc[0]
            cash += shares * sell_price
            del holdings[ticker]
        except Exception as e:
            logging.warning(f"Failed to sell Ticker {ticker} on {date}: {e}")

    # 전략 실행 및 종목 선정
    filtered_data = strategy_filter(data, date) # 전략에 사용될 데이터 필터링

    # 전략 실행
    portfolio = selected_strategy(filtered_data, date)
    if portfolio.empty:
        logging.info(f"No stocks selected on {date}. Portfolio Value: {cash:,.2f}")
        portfolio_values.append(cash)
        monthly_returns.append(0.0)
        continue

    # 매수 가능한 종목별 수량 계산
    allocation = cash / len(portfolio)
    for _, row in portfolio.iterrows():
        ticker, buy_price = row['Ticker'], row['Close']
        try:
            num_shares = int(allocation // buy_price)
            if num_shares > 0:
                executed_price, total_cost = apply_trading_cost(buy_price, num_shares)
                cash -= total_cost
                holdings[ticker] = num_shares
        except Exception as e:
            logging.warning(f"Failed to buy Ticker {ticker} on {date}: {e}")

    # 포트폴리오 가치 계산 (익월말 종가 기준)
    portfolio_value = cash
    for ticker, shares in holdings.items():
        try:
            close_price = data.loc[(data.index == next_month_date) & (data['Ticker'] == ticker), 'Close'].iloc[0]
            portfolio_value += shares * close_price
        except Exception as e:
            logging.warning(f"Failed to calculate value for Ticker {ticker} on {next_month_date}: {e}")

    # 수익률 계산
    monthly_return = calculate_monthly_return(portfolio_values[-1], portfolio_value)
    monthly_returns.append(monthly_return)
    portfolio_values.append(portfolio_value)

    logging.info(f"{date}: Portfolio Value = {portfolio_value:,.2f}")
    logging.info(f"Selected Stocks:\n{tabulate(
        portfolio[["Ticker", "Close", "PER", "PBR", "Name"]],
        headers=["Ticker", "Close", "PER", "PBR", "Name"],
        tablefmt="plain"
    )}")

# ------------------ 결과 분석 및 출력 ------------------
max_drawdown, drawdowns = calculate_drawdown(portfolio_values)
results = pd.DataFrame({
    "Date": dates[:-1],  # 마지막 월은 제외
    "Portfolio Value": portfolio_values[:-1],
    "Monthly Return": monthly_returns[:-1],
    "Drawdown": drawdowns[:-1]
}).set_index("Date")

cagr = calculate_cagr(portfolio_values[0], portfolio_values[-1], start_date, end_date)
total_return = (results["Portfolio Value"].iloc[-1] / results["Portfolio Value"].iloc[0]) - 1
sharpe_ratio = (results["Monthly Return"].mean() - (0.03 / 12)) / results["Monthly Return"].std() * (12 ** 0.5)

print(f"CAGR: {cagr:.2%}")  # 퍼센트 형태로 출력
print(f"Total Return: {total_return:.2%}")
print(f"Maximum Drawdown: {max_drawdown:.2%}")
print(f"Sharpe Ratio: {sharpe_ratio:.4f}")

# 포트폴리오 가치와 낙폭 그래프
plot_backtest_results(dates, portfolio_values, drawdowns)
# plot_backtest_results(dates, portfolio_values, drawdowns, monthly_returns) # 포트폴리오 가치 + 월별 수익률 그래프

# ------------------ 종료 ------------------
conn.close()
