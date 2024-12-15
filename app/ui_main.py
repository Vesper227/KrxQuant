import sys
import sqlite3
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QLabel, QDateEdit, QPushButton,
    QVBoxLayout, QWidget, QComboBox, QTableWidget, QTableWidgetItem
)
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from datetime import datetime, timedelta
import pandas as pd

# 그래프 캔버스
class MplCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super().__init__(fig)

# 메인 윈도우 클래스
class BacktestApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Backtest Application")
        self.setGeometry(100, 100, 800, 600)

        # UI 요소
        self.layout = QVBoxLayout()

        # 전략 선택
        self.strategy_label = QLabel("Select Strategy:")
        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems(["Combined Score", "Low PER", "High DIV"])
        self.layout.addWidget(self.strategy_label)
        self.layout.addWidget(self.strategy_combo)

        # 기간 입력
        self.start_date_label = QLabel("Start Date:")
        self.start_date_input = QDateEdit()
        self.start_date_input.setCalendarPopup(True)

        self.end_date_label = QLabel("End Date:")
        self.end_date_input = QDateEdit()
        self.end_date_input.setCalendarPopup(True)

        # 초기값 설정
        today = datetime.today()
        one_year_ago = today - timedelta(days=365*5)
        self.start_date_input.setDate(one_year_ago)
        self.end_date_input.setDate(today)

        self.layout.addWidget(self.start_date_label)
        self.layout.addWidget(self.start_date_input)
        self.layout.addWidget(self.end_date_label)
        self.layout.addWidget(self.end_date_input)

        # 실행 버튼
        self.run_button = QPushButton("Run Backtest")
        self.run_button.clicked.connect(self.run_backtest)
        self.layout.addWidget(self.run_button)

        # 결과 테이블
        self.result_table = QTableWidget()
        self.result_table.setRowCount(10)
        self.result_table.setColumnCount(4)
        self.result_table.setHorizontalHeaderLabels(["Ticker", "Name", "Close", "PER"])
        self.layout.addWidget(self.result_table)

        # 그래프
        self.canvas = MplCanvas(self, width=5, height=3)
        self.layout.addWidget(self.canvas)

        # 메인 위젯 설정
        central_widget = QWidget()
        central_widget.setLayout(self.layout)
        self.setCentralWidget(central_widget)

    def run_backtest(self):
        # 입력값 가져오기
        strategy = self.strategy_combo.currentText()
        start_date = self.start_date_input.text()
        end_date = self.end_date_input.text()

        # 데이터베이스 연결
        db_path = "krx_data.db"
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        # 데이터 쿼리 실행
        query = f"""
        SELECT * FROM stock_monthly_data
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY Date, Ticker
        """
        try:
            data = pd.read_sql(query, connection, parse_dates=["Date"])
            if data.empty:
                raise ValueError("No data found for the selected date range.")

            # 백테스트 로직
            portfolio_values = []
            initial_cash = 10_000_000  # 1000만 원
            dates = sorted(data["Date"].unique())

            for date in dates:
                monthly_data = data[data["Date"] == date]
                top_stocks = monthly_data.nsmallest(5, "PER")  # Top 5 stocks by PER
                total_value = top_stocks["Close"].sum()

                # 비중 계산 및 수익률
                top_stocks["Weight"] = top_stocks["Close"] / total_value
                portfolio_return = (top_stocks["ChangeRate"] / 100 * top_stocks["Weight"]).sum()
                initial_cash *= (1 + portfolio_return)
                portfolio_values.append(initial_cash)

            # 결과 테이블 업데이트
            self.result_table.setRowCount(len(top_stocks))
            for i, row in top_stocks.iterrows():
                self.result_table.setItem(i, 0, QTableWidgetItem(row["Ticker"]))
                self.result_table.setItem(i, 1, QTableWidgetItem(row["Name"]))
                self.result_table.setItem(i, 2, QTableWidgetItem(str(row["Close"])))
                self.result_table.setItem(i, 3, QTableWidgetItem(str(row["PER"])))

            # 그래프 업데이트
            self.canvas.axes.clear()
            self.canvas.axes.plot(dates, portfolio_values, label="Portfolio Value", color="blue")
            self.canvas.axes.set_title("Portfolio Performance")
            self.canvas.axes.set_xlabel("Date")
            self.canvas.axes.set_ylabel("Value (KRW)")
            self.canvas.axes.legend()
            self.canvas.draw()

        except Exception as e:
            self.result_table.setRowCount(0)
            self.result_table.setColumnCount(1)
            self.result_table.setHorizontalHeaderLabels(["Error"])
            self.result_table.setItem(0, 0, QTableWidgetItem(str(e)))
        finally:
            connection.close()

# 앱 실행
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BacktestApp()
    window.show()
    sys.exit(app.exec_())
