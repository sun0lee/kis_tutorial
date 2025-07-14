from pykis import KisAuth
from core import AUTH_CONFIG_PATH

# 접속정보 파일 생성 : config > secrete.json
def save_auth():
    auth = KisAuth(
        id="@",
        appkey="",
        secretkey="",
        account="",
        virtual=False,
    )
    auth.save(AUTH_CONFIG_PATH)

if __name__ == "__main__":
    save_auth()