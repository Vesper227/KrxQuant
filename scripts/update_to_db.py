from pykrx.stock import get_market_cap_by_date
import sqlite3
import pandas as pd

# 데이터베이스 연결
db_path = "data/krx_data.db"
conn = sqlite3.connect(db_path)

def update_market_cap_by_ticker(start_date, end_date):
    """
    특정 Ticker에 대해 start_date ~ end_date 기간의 MarketCap 및 SharesOutstanding 데이터를 업데이트.
    """
    # 기존 데이터에서 Ticker 목록 가져오기
    query = "SELECT DISTINCT Ticker FROM stock_monthly_data WHERE MarketCap is NULL or SharesOutstanding is NULL"
    tickers = pd.read_sql(query, conn)['Ticker'].tolist()
    print(f"종목수 : {len(tickers)}")

    updates = []  # 업데이트할 데이터를 저장할 리스트

    for ticker in tickers:
    # for ticker in tickers[:100]:  # 100개만 테스트
        try:
            # PyKrx를 사용해 월말 데이터 가져오기
            market_cap_data = get_market_cap_by_date(start_date, end_date, ticker=ticker, freq="m")

            for date, row in market_cap_data.iterrows():
                market_cap = int(row['시가총액'])
                shares_outstanding = int(row['상장주식수'])
                date_str = pd.to_datetime(date).strftime("%Y-%m-%d")  # Date 포맷 변환

                # 업데이트 리스트에 추가
                updates.append((market_cap, shares_outstanding, date_str, ticker))

        except Exception as e:
            print(f"Error fetching data for Ticker {ticker}: {e}")

    # 데이터베이스에 업데이트
    if updates:
        cursor = conn.cursor()
        cursor.executemany("""
            UPDATE stock_monthly_data
            SET MarketCap = ?, SharesOutstanding = ?
            WHERE Date = ? AND Ticker = ?
        """, updates)
        conn.commit()
        print(f"Updated {len(updates)} rows.")

# 실행
start_date = "20150101"
end_date = "20241130"
update_market_cap_by_ticker(start_date, end_date)

# 연결 종료
conn.close()
