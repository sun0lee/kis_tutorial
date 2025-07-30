import os
import sys
from cryptography.fernet import Fernet
from core.db_manager import DatabaseManager


def setup_kis_credentials():

    encryption_key = os.environ.get("DB_ENCRYPTION_KEY")
    if not encryption_key:
        print("\n--- DB 암호화 키 설정 ---")
        print("환경 변수 'DB_ENCRYPTION_KEY'가 설정되지 않았습니다.")
        print("새로운 Fernet 키를 생성하거나 기존 키를 입력해주세요.")

        generate_new_key = input("새로운 암호화 키를 생성하시겠습니까? (y/n): ").lower()
        if generate_new_key == 'y':
            encryption_key = Fernet.generate_key().decode()
            print(f"\n새로운 암호화 키가 생성되었습니다: {encryption_key}")
            print("이 키를 환경 변수 'DB_ENCRYPTION_KEY'로 설정하여 재사용하는 것을 강력히 권장합니다.")
            print("예시: export DB_ENCRYPTION_KEY=\"{}\"\n".format(encryption_key))
        else:
            encryption_key = input("기존 암호화 키를 입력하세요: ")
            if not encryption_key:
                print("오류: 암호화 키가 입력되지 않았습니다. 스크립트를 종료합니다.")
                sys.exit(1)
        print("-----------------------------\n")

    # 2. DatabaseManager 초기화
    try:
        db_manager = DatabaseManager(encryption_key=encryption_key.encode('utf-8'))  # Fernet 키는 bytes여야 함
    except ValueError as e:
        print(f"오류: DatabaseManager 초기화 실패 - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"예상치 못한 오류로 DatabaseManager 초기화 실패: {e}")
        sys.exit(1)

    # 3. KIS API 접속 정보 입력받기
    print("\n--- KIS API 접속 정보 입력 ---")
    user_id = input ("HTS 로그인 ID (id): ")
    app_key = input("App Key: ")
    secret_key = input("Secret Key: ")
    account = input("계좌 번호 (예: 12345678-01): ")
    virtual_input = input("가상 계좌 여부 (true/false): ").lower()
    virtual = True if virtual_input == 'true' else False

    credentials = {
        "id": user_id,
        "appkey": app_key,
        "secretkey": secret_key,
        "account": account,
        "virtual": virtual
    }

    # 4. DB에 인증 정보 저장
    try:
        db_manager.save_api_credentials(credentials)
        print("\nKIS API 인증 정보가 성공적으로 DB에 저장되었습니다.")
    except Exception as e:
        print(f"\n오류: KIS API 인증 정보 DB 저장 실패 - {e}")
        print("스크립트를 종료합니다.")
        sys.exit(1)



if __name__ == "__main__":
    setup_kis_credentials()
