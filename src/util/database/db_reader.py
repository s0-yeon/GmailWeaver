# 웹앱 DB → 메일에서 추출한 정보 데이터 JSON
# 현재는 가라 데이터

# 웹앱용 가라데이터
def get_mail_stats(): # 메일 송수신
    return {
        "ae-best-care-market14@deals.aliexpress.com": {
            "name": "AliExpress",
            "sent": 0,
            "received": 3
        },
        "notifications@github.com": {
            "name": "uzichoi",
            "sent": 1,
            "received": 12
        },
        "inews11@seoul.go.kr": {
            "name": "서울시청",
            "sent": 0,
            "received": 2
        },
        "team@company.com": {
            "name": "프로젝트팀",
            "sent": 7,
            "received": 5
        },
        "friend123@gmail.com": {
            "name": "김민수",
            "sent": 4,
            "received": 6
        }
    }

def get_keyword_stats(): # 메일 키워드 수
    return  {
    "keywords": [
        { "word": "회의", "count": 15 },
        { "word": "일정", "count": 2 },
        { "word": "첨부파일", "count": 9 },
        { "word": "프로젝트", "count": 58 },
        { "word": "확인", "count": 22 },
        { "word": "요청", "count": 17 },
        { "word": "보고서", "count": 11 },
        { "word": "마감", "count": 411 },
        { "word": "수정", "count": 13 },
        { "word": "공유", "count": 10 }
    ]
}

def get_high_affinity_person_stats(): # 친밀한 사람 친밀도 수치
    return [
        {
        "email": "friend123@gmail.com",
        "name": "김민수",
        "affinity": 0.92
        },
        {
        "email": "team@company.com",
        "name": "프로젝트팀",
        "affinity": 0.78
        },
        {
        "email": "notifications@github.com",
        "name": "uzichoi",
        "affinity": 0.65
        },
        {
        "email": "inews11@seoul.go.kr",
        "name": "서울시청",
        "affinity": 0.40
        },
        {
        "email": "ae-best-care-market14@deals.aliexpress.com",
        "name": "AliExpress",
        "affinity": 0.55
        }
    ]

def get_low_affinity_person_stats():  # 안 친한 사람 친밀도 수치
    return [
        {
            "email": "promo@shopping.com",
            "name": "Shopping Promo",
            "affinity": 0.12
        },
        {
            "email": "newsletter@finance.com",
            "name": "Finance News",
            "affinity": 0.18
        },
        {
            "email": "ads@travelworld.com",
            "name": "Travel World",
            "affinity": 0.10
        },
        {
            "email": "updates@jobplatform.com",
            "name": "Job Platform",
            "affinity": 0.20
        },
        {
            "email": "no-reply@eventhub.com",
            "name": "Event Hub",
            "affinity": 0.14
        }
    ]

def get_user_rating_stats(): # 모든 유저의 Olive 만족도
    return {"total_rating" : 99}

def get_mail_sync_stats(): # 메일 동기화시 동기화된 메일 수, 동기화 시간
    return {
        "mail_count":505,
        "sync_time": "2-30", # 2시간 30분
        "sync_update_date": "2026-04-22" #2026년 4월 22일
    }
