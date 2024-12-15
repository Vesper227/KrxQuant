import sqlite3
import pandas as pd

# 데이터베이스 연결
db_path = "krx_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 테이블에서 데이터 읽기
query = "SELECT Date, Ticker, Close FROM stock_monthly_data ORDER BY Ticker, Date"
data = pd.read_sql(query, conn, parse_dates=["Date"])

# ChangeRate 계산
data['ChangeRate'] = data.groupby('Ticker')['Close'].pct_change() * 100
data['ChangeRate'] = data['ChangeRate'].round(2)  # 소숫점 2자리로 반올림

# 계산된 데이터를 테이블에 반영
for index, row in data.iterrows():
    date = row['Date'].strftime('%Y-%m-%d')
    ticker = row['Ticker']
    change_rate = row['ChangeRate']
    
    cursor.execute("""
        UPDATE stock_monthly_data
        SET ChangeRate = ?
        WHERE Date = ? AND Ticker = ?
    """, (change_rate, date, ticker))

# 변경사항 저장 및 연결 종료
conn.commit()
conn.close()

print("ChangeRate 계산 및 업데이트 완료!")