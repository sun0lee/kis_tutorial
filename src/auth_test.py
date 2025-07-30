import os
import sys
import datetime
from core.db_manager import DatabaseManager
from core.process import MarketDataManager
from core.api_client import KisClient


def test_kis_authentication():
    encryption_key_str = os.environ.get("DB_ENCRYPTION_KEY")
    if not encryption_key_str:
        print("\n--- DB 암호화 키 설정 ---")
        print("환경 변수 'DB_ENCRYPTION_KEY'가 설정되지 않았습니다.")
        print("이 키는 DB에 저장된 API 정보를 복호화하는 데 사용됩니다.")
        encryption_key_str = input("DB 암호화 키를 입력하세요 (setup_credentials.py 실행 시 사용한 키): ")
        if not encryption_key_str:
            print("오류: 암호화 키가 입력되지 않았습니다. 스케줄러를 종료합니다.")
            sys.exit(1)
        print("-----------------------------\n")

    encryption_key_bytes = encryption_key_str.encode('utf-8')  # Fernet 키는 bytes여야 함

    # DatabaseManager 인스턴스 초기화 (암호화 키 전달)
    db = None
    try:
        db = DatabaseManager(encryption_key=encryption_key_bytes)
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DatabaseManager 초기화 실패: {e}")
        print("스케줄러를 종료합니다.")
        sys.exit(1)  # 오류 종료

    # KIS API 인증 정보 DB에서 로드
    api_credentials = db.load_api_credentials()
    if not api_credentials:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KIS API 접속 정보를 DB에서 찾을 수 없습니다.")
        print("setup_credentials.py를 먼저 실행하여 정보를 저장해주세요.")
        db.close()
        sys.exit(1)

    # KisClient 인스턴스 초기화
    kis_client = None
    try:
        # api_credentials 딕셔너리 전체를 KisClient 생성자에 전달합니다.
        kis_client = KisClient(credentials=api_credentials)
    except Exception as e:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KisClient 초기화 실패: {e}")
        print("스케줄러를 종료합니다.")
        if db: db.close()
        sys.exit(1)  # 오류 종료

    # MarketDataManager 인스턴스 초기화
    manager = MarketDataManager(kis_client, db)
    manager.per_symbol_jobs()


if __name__ == "__main__":
    test_kis_authentication()
