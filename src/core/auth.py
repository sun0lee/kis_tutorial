import os
import sys
import datetime
from cryptography.fernet import Fernet

from core.db_manager import DatabaseManager
from core.api_client import KisClient
from core.process import MarketDataManager


def initialize_application():
    """
    애플리케이션의 핵심 구성 요소 (DB Manager, KIS Client, Market Data Manager)를 초기화합니다.
    DB 암호화 키를 로드하고, DB에서 API 인증 정보를 가져와 클라이언트를 설정합니다.
    초기화 실패 시 프로그램을 종료합니다.
    """
    print("[Initializer] 애플리케이션 초기화 시작...")

    # 1. DB 암호화 키 로드 및 유효성 검사
    encryption_key_str = os.environ.get("DB_ENCRYPTION_KEY")
    encryption_key_bytes = None
    if encryption_key_str:
        encryption_key_bytes = encryption_key_str.encode('utf-8')
    else:
        print("\n--- DB 암호화 키 설정 ---")
        print("환경 변수 'DB_ENCRYPTION_KEY'가 설정되지 않았습니다.")
        print("이 키는 DB에 저장된 API 정보를 복호화하는 데 사용됩니다.")

        user_input_key = input("DB 암호화 키를 입력하세요 (setup_credentials.py 실행 시 사용한 키, 또는 비워두면 암호화 비활성화): ")
        if user_input_key:
            encryption_key_bytes = user_input_key.encode('utf-8')
        else:
            print("경고: 암호화 키가 제공되지 않아 암호화 기능이 비활성화됩니다. API 정보가 평문으로 저장/로드될 수 있습니다.")
        print("-----------------------------\n")

    # 2. DatabaseManager 인스턴스 초기화 (암호화 키 전달)
    db = None
    try:
        db = DatabaseManager(encryption_key=encryption_key_bytes)  # bytes 또는 None 전달
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DatabaseManager 초기화 실패: {e}")
        print("애플리케이션 초기화를 중단합니다.")
        sys.exit(1)

    # 3. KIS API 인증 정보 DB에서 로드
    api_credentials = db.load_api_credentials()
    if not api_credentials:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KIS API 접속 정보를 DB에서 찾을 수 없습니다.")
        print("setup_credentials.py를 먼저 실행하여 정보를 저장해주세요.")
        if db: db.close()
        sys.exit(1)

    # 4. KisClient 인스턴스 초기화
    kis_client = None
    try:
        kis_client = KisClient(credentials=api_credentials)  # 로드된 인증 정보 딕셔너리 전달
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KisClient 초기화 실패: {e}")
        print("애플리케이션 초기화를 중단합니다.")
        if db: db.close()
        sys.exit(1)

    # 5. MarketDataManager 인스턴스 초기화
    manager = MarketDataManager(kis_client, db)

    print("[Initializer] 애플리케이션 초기화 완료.")
    return kis_client, db, manager
