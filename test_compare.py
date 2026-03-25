import requests
import time
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "util"))
from parquet_query import answer_query

BASE_URL = "http://localhost:80"

QUESTIONS = [
    "최근 이메일 내용 알려줘",
    "총 이메일이 몇 개야?",
    "누가 가장 많이 이메일을 보냈어?",
]


def test_parquet(question):
    start = time.time()
    try:
        result = answer_query(question)
        elapsed = time.time() - start
        return elapsed, result
    except Exception as e:
        return time.time() - start, f"오류: {e}"


def test_graphrag(question):
    start = time.time()
    try:
        r = requests.post(
            f"{BASE_URL}/run-query",
            json={"message": question, "resMethod": "local", "resType": "text"},
            timeout=120
        )
        elapsed = time.time() - start
        data = r.json()
        return elapsed, data.get("result", data.get("error", "응답 없음"))
    except Exception as e:
        return time.time() - start, f"오류: {e}"


if __name__ == "__main__":
    for i, question in enumerate(QUESTIONS):
        print("=" * 60)
        print(f"[질문 {i+1}] {question}")
        print("=" * 60)

        t1, r1 = test_parquet(question)
        t2, r2 = test_graphrag(question)

        print(f"[Parquet]  {t1:.2f}초")
        print(f"  답변: {r1[:300]}")
        print()
        print(f"[GraphRAG] {t2:.2f}초")
        print(f"  답변: {r2[:300]}")
        print()

    print("=" * 60)
    print("테스트 완료!")
