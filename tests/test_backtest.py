import sqlite3
from pykrx import stock
from datetime import datetime
import pandas as pd

def fetch_and_save_sector_data(db_path="krx_data.db"):
    # 데이터베이스 연결
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_sector (
        SectorIndex TEXT,
        SectorName TEXT,
        Ticker TEXT,
        Name TEXT,
        PRIMARY KEY (SectorIndex, Ticker)
    )
    """)

    date = datetime.now().strftime("%Y%m%d")
    test = stock.get_market_sector_classifications(date, market="KOSPI")

    # 모든 KOSPI 업종 가져오기
    index_tickers = stock.get_index_ticker_list(market="KOSPI")
    for sector_ticker in index_tickers:
        sector_name = stock.get_index_ticker_name(sector_ticker)
        try:
            # 업종에 속한 종목 리스트 가져오기
            tickers = stock.get_index_portfolio_deposit_file(sector_ticker)
            for ticker in tickers:
                name = stock.get_market_ticker_name(ticker)
                # 데이터 삽입
                cursor.execute("""
                INSERT OR REPLACE INTO stock_sector (SectorIndex, SectorName, Ticker, Name)
                VALUES (?, ?, ?, ?)
                """, (sector_ticker, sector_name, ticker, name))
        except Exception as e:
            print(f"Error fetching data for sector {sector_name} ({sector_ticker}): {e}")

    conn.commit()
    conn.close()
    print("Sector data saved to database.")

# 실행
fetch_and_save_sector_data()
