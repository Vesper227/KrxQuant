# KrxQuant

한국거래소 데이터를 활용하여 퀀트 투자 전략을 구현하고 백테스트를 수행하는 프로젝트입니다.  
데이터 수집, 전략 개발, 백테스팅을 통해 효율적인 투자 의사결정을 돕는 것을 목표로 합니다.

---

## 📂 프로젝트 구조

KrxQuant/
├── data/                # 데이터 파일 저장 (예: SQLite, CSV 등)
├── logs/                # 실행 중 생성되는 로그 저장
├── scripts/             # 개별 실행 스크립트
│   ├── data_to_db.py    # 데이터를 DB로 저장하는 스크립트
│   ├── backtest.py      # 백테스트 실행 스크립트
│   └── strategies.py    # 퀀트 전략 구현
├── krxquant/            # 메인 모듈
│   ├── __init__.py      # 패키지 초기화 파일
│   ├── query.py         # 데이터베이스 쿼리 관리
│   ├── utils.py         # 유틸리티 함수 모음
│   └── models.py        # 데이터 모델 정의 (Optional)
├── tests/               # 테스트 코드
│   └── test_backtest.py # 백테스트 테스트 코드
├── .gitignore           # Git 무시 파일 설정
├── LICENSE              # 라이선스 파일
├── README.md            # 프로젝트 설명
└── requirements.txt     # Python 의존성 관리 파일
