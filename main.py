import requests
import xml.etree.ElementTree as ET
import firebase_admin
from firebase_admin import credentials, db

# Firebase 프로젝트 설정 정보
cred = credentials.Certificate("C:/Users/admin/Downloads/danger-detection-system-firebase-adminsdk-mj17n-b2a8f114c8.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://danger-detection-system-default-rtdb.firebaseio.com/'
})

# API 엔드포인트 및 파라미터 설정
url = "https://opendata.koroad.or.kr/data/rest/accident/death"
auth_key = "G1%2FxBvJbU8I2PT62RGAxeV9i6Wg6aTnBgdofOCQKcAohX40LCzuKdRH5A8jfRe0x"
search_year = "2021"

# 시도별 구군 정보 설정
sido_gugun_info = {
    "1100": ["1116", "1117", "1124", "1111", "1115", "1123", "1112", "1125", "1122", "1107",
             "1105", "1114", "1110", "1109", "1119", "1104", "1106", "1118", "1120", "1113",
             "1103", "1108", "1101", "1102", "1121"],
    # 다른 시도에 대한 정보 추가 가능
}

try:
    # 시도별로 반복
    for si_do, gu_gun_values in sido_gugun_info.items():
        # 각 시도에 대한 구군 정보 갱신
        gu_gun_values = sido_gugun_info.get(si_do, [])

        # 각 구군에 대해 반복
        for gu_gun in gu_gun_values:
            # API 호출을 위한 URL 조합
            api_url = f"{url}?authKey={auth_key}&searchYear={search_year}&siDo={si_do}&guGun={gu_gun}"

            # API에 요청을 보내고 응답 받기
            response = requests.get(api_url)

            # 응답이 성공인 경우 데이터 Firebase에 저장
            if response.status_code == 200:
                if response.content:
                    root = ET.fromstring(response.content)
                    items = root.findall(".//item")
                    data = []

                    for item in items:
                        item_data = {}
                        for child in item:
                            if child.tag not in ["dght_cd", "occrrnc_day_cd", "acc_ty_lclas_cd",
                                                 "acc_ty_mlsfc_cd", "acc_ty_cd", "aslt_vtr_cd",
                                                 "road_frm_lclas_cd", "road_frm_cd",
                                                 "wrngdo_isrty_vhcty_lclas_cd", "dmge_isrty_vhcty_lclas_cd"]:
                                item_data[child.tag] = child.text
                        data.append(item_data)

                    # Firebase에 데이터 저장
                    ref = db.reference('/accident_data')
                    for entry in data:
                        try:
                            dth_dnv_cnt = int(entry.get("dth_dnv_cnt", 0))  # 사망자수(F)
                            se_dnv_cnt = int(entry.get("se_dnv_cnt", 0))  # 중상자수(A)
                            si_dnv_cnt = int(entry.get("si_dnv_cnt", 0))  # 경상자수(B)

                            # EPDO 계산: 12F + 3(A+B+C)
                            epdo = 12 * dth_dnv_cnt + 3 * (se_dnv_cnt + si_dnv_cnt)

                            # Firebase에 데이터 저장
                            ref.push().set({
                                "epdo": epdo,
                                "other_fields": entry
                            })

                            print(f"{si_do} 시도, {gu_gun} 구군 데이터가 성공적으로 Firebase에 저장되었습니다.")
                            print(f"EPDO: {epdo}")
                        except ValueError as ve:
                            print(f"에러 발생: {ve}")

                        print("-" * 30)

                else:
                    print(f"{si_do} 시도, {gu_gun} 구군 - 응답 내용이 없습니다.")
            else:
                print(f"{si_do} 시도, {gu_gun} 구군 - API 호출 실패: {response.status_code}")

except requests.RequestException as re:
    print(f"API 요청 에러: {re}")
except Exception as e:
    print(f"에러 발생: {e}")
