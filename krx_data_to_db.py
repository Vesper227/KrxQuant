import sqlite3
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pykrx import stock

# 로깅 설정
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# 오늘 날짜로 파일 이름 설정
log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # UTF-8 설정
)

# 데이터베이스 연결
db_path = "krx_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 테이블 생성 (추가된 속성 포함)
cursor.execute("""
CREATE TABLE IF NOT EXISTS stock_monthly_data (
    Date TEXT,
    Ticker TEXT,
    Name TEXT,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    Volume REAL,
    ChangeRate REAL,
    PER REAL,
    BPS REAL,
    PBR REAL,
    EPS REAL,
    DPS REAL,
    DIV REAL,
    PRIMARY KEY (Date, Ticker)
)
""")
conn.commit()

# 데이터 가져오기
start_date = "20150101"
end_date = datetime.now().strftime("%Y%m%d")  # 현재 날짜
tickers = stock.get_market_ticker_list()

for ticker in tickers:
    # DB에서 해당 ticker와 기간의 데이터가 있는지 확인
    cursor.execute("""
        SELECT 1 FROM stock_monthly_data 
        WHERE Ticker = ? AND Date BETWEEN ? AND ?
        LIMIT 1
    """, (ticker, start_date, end_date))
    exists = cursor.fetchone()

    if exists:
        logging.info(f"Data already exists for {ticker} in {start_date} - {end_date}")
        continue  # 이미 존재하는 경우 건너뜀

    try:
        # 호출 제한을 피하기 위해 1초 대기
        time.sleep(1)

        # OHLCV 데이터
        ohlcv = stock.get_market_ohlcv(start_date, end_date, ticker, freq="m")
        ohlcv.index = pd.to_datetime(ohlcv.index)
        ohlcv['ChangeRate'] = round(ohlcv['종가'].pct_change(), 2)

        # Fundamental 데이터 (BPS PER PBR EPS DIV DPS)
        fundamental = stock.get_market_fundamental_by_date(start_date, end_date, ticker, freq="m")

            # 필요한 컬럼이 모두 있는지 확인
        required_columns = ['TRD_DD', 'BPS', 'PER', 'PBR', 'EPS', 'DVD_YLD', 'DPS']
        missing_columns = [col for col in required_columns if col not in fundamental.columns]
        if missing_columns:
            logging.warning(f"Missing columns {missing_columns} for {ticker} ({start_date} to {end_date})")
            continue  # 다음 루프로 이동

        if fundamental.empty:
            logging.warning(f"No data available for {ticker} ({start_date} to {end_date})")
            continue

        # 필요한 컬럼만 선택
        fundamental = fundamental[required_columns]

        fundamental.index = pd.to_datetime(fundamental.index)

        # 데이터 통합
        combined = pd.concat([ohlcv, fundamental], axis=1)
        combined.fillna(pd.NA, inplace=True)

        # 종목 이름
        name = stock.get_market_ticker_name(ticker)

        # 데이터 삽입
        for index, row in combined.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO stock_monthly_data 
                (Date, Ticker, Name, Open, High, Low, Close, Volume, ChangeRate, PER, BPS, PBR, EPS, DPS, DIV)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                index.strftime('%Y-%m-%d'), ticker, name, row['시가'], row['고가'], row['저가'], row['종가'], 
                row['거래량'], row['ChangeRate'], row['PER'], row['BPS'], round(row['PBR'], 2), row['EPS'], 
                row['DPS'], round(row['DIV'], 2)
            ))
        conn.commit()
        logging.info(f"Processed {ticker} ({name}) for {start_date}-{end_date}")

    except Exception as e:
        error_message = f"Error processing {ticker} for {start_date}-{end_date}: {e}"
        logging.error(error_message)
        print(error_message)

# 연결 종료
conn.close()