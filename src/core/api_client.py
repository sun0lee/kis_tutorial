import requests
import json
from typing import Dict, Any, Optional
from pykis import KisAuth, PyKis

class KisClient:
    def __init__(self, credentials: [Dict[str, Any]]):
        print("[KIS Client] KisClient 인스턴스 초기화 및 인증 정보 로드...")

        try:
            id = credentials.get('id')
            account = credentials.get('account')
            appkey = credentials.get('appkey')
            secretkey = credentials.get('secretkey')

            self.auth = PyKis(
                id=id,
                account=account,
                appkey=appkey,
                secretkey=secretkey,
                keep_token=True
            )

            self.app_key = appkey
            self.app_secret = secretkey

            print("[KIS Client] 인증 정보 로드 성공.")

        except Exception as e:
            print(f"[KIS Client Error] 인증 정보 로드 실패: {e}")
            raise

    @property
    def access_token(self):
        return self.auth.token.token

    def _call_api(
            self,
            base_url: str,
            endpoint: str,
            tr_id: str,
            params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:

        url = f"{base_url}{endpoint}"

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.access_token}",
            "appKey": self.app_key,
            "appSecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "tr_cont": "",
            "gt_uid": ""
        }

        try:
            print(f"[KIS Client] API 요청 시작: {endpoint} (TR_ID: {tr_id})")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 200번대 HTTP 상태 코드가 아니면 예외 발생 (예: 401, 404, 500)

            data = response.json()
            if data.get("rt_cd") == "0":
                print(f"[KIS Client] {tr_id} API 호출 성공: {data.get('msg1', '정상처리')}")
                return data
            else:
                error_code = data.get("rt_cd", "N/A")
                error_msg = data.get("msg1", "알 수 없는 오류")
                print(f"[KIS Client Error] {tr_id} API 호출 실패 ({error_code}): {error_msg}")

                return None  # API 응답 자체가 실패한 경우 None 반환
        except requests.exceptions.RequestException as e:
            print(f"[KIS Client Error] 네트워크 오류 또는 HTTP 요청 실패: {e}")

            if e.response is not None:
                print(f"HTTP Status Code: {e.response.status_code}")
                print(f"Response Body: {e.response.text[:500]}...")  # 에러 응답 본문 출력 시도
            return None  # 네트워크/HTTP 요청 자체의 오류

        except json.JSONDecodeError as e:
            print(f"[KIS Client Error] JSON 응답 파싱 실패: {e}")
            print(f"응답 본문: {response.text[:500]}...")
            return None