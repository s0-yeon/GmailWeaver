# 웹앱 DB → 메일에서 추출한 정보 데이터 JSON
# 현재는 가라 데이터

# 웹앱용 가라데이터
def get_mail_stats():
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

def get_keyword_stats(): 
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