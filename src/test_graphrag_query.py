# test_graphrag_query.py
# GraphRAG query CLI 직접 테스트용 스크립트 (Flask 우회)
# 실행: python test_graphrag_query.py

import os
import re
import subprocess
import time
import sys
from dotenv import load_dotenv

# ── 환경변수 로드 (app.py와 동일 경로) ──────────────────────────────────────
load_dotenv("src/parquet/.env")

# ── 한글 출력 깨짐 방지 ─────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── 핵심 함수: app.py의 _run_graphrag 와 동일한 로직 ────────────────────────
def run_graphrag_query(
    message: str,
    method: str = "local",        # "local" | "global"
    response_type: str = "text",  # "text" | "calendar" | 자유 지정
    root: str = "./src/parquet",
    verbose: bool = True,          # True: 원본 stdout 전체 출력
    korean: bool = True,           # True: 한국어 답변 강제
) -> dict:
    """
    GraphRAG CLI를 subprocess로 직접 호출하고 결과를 반환.

    Returns:
        {
            "answer":      str,   # 정제된 최종 답변
            "raw_stdout":  str,   # CLI 전체 출력 (디버깅용)
            "elapsed_sec": float, # 소요 시간
            "returncode":  int,
        }
    """

    def _decode(b: bytes) -> str:
        if not b:
            return ""
        for enc in ("utf-8", "cp949", "euc-kr"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                pass
        return b.decode("utf-8", errors="replace")

    query = message
    if korean:
        query = message + " 영어 말고 한국어로 답변해줘."

    cmd = [
        "graphrag", "query",
        "--root", root,
        "--response-type", response_type,
        "--method", method,
        "--query", query,
    ]

    if verbose:
        print(f"\n{'='*60}")
        print(f"[QUERY]   {message}")
        print(f"[METHOD]  {method}  |  [TYPE] {response_type}")
        print(f"[CMD]     {' '.join(cmd)}")
        print(f"{'='*60}")

    start = time.time()
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
        text=False,
    )
    elapsed = round(time.time() - start, 2)

    stdout_text = _decode(result.stdout)
    stderr_text = _decode(result.stderr)

    if verbose:
        print(f"\n[RAW STDOUT]\n{stdout_text}")
        if stderr_text:
            print(f"[STDERR]\n{stderr_text}")
        print(f"[elapsed] {elapsed}s  |  [returncode] {result.returncode}")

    # ── 오류 처리 ──────────────────────────────────────────────────────────
    if result.returncode != 0:
        error_msg = stderr_text or stdout_text or "GraphRAG 실행 오류"
        print(f"\n[ERROR] {error_msg}")
        return {
            "answer": "",
            "raw_stdout": stdout_text,
            "elapsed_sec": elapsed,
            "returncode": result.returncode,
            "error": error_msg,
        }

    # ── 답변 파싱 (app.py와 동일) ──────────────────────────────────────────
    match = re.search(
        r"SUCCESS: (?:Local|Global) Search Response:\s*(.*)",
        stdout_text,
        re.DOTALL,
    )
    answer = match.group(1).strip() if match else stdout_text.strip()

    # 출처 태그·마크다운 제거
    answer = re.sub(r"\[Data:.*?\]|\[데이터:.*?\]", "", answer)
    answer = re.sub(r"\*+|#+", "", answer).strip()

    if verbose:
        print(f"\n[PARSED ANSWER]\n{answer}\n")

    return {
        "answer": answer,
        "raw_stdout": stdout_text,
        "elapsed_sec": elapsed,
        "returncode": result.returncode,
    }


# ── 배치 테스트 헬퍼: 여러 쿼리를 한 번에 돌릴 때 ──────────────────────────
def batch_test(queries: list[dict], root: str = "./src/parquet") -> None:
    """
    queries 예시:
        [
            {"message": "최근 메일 요약해줘", "method": "local"},
            {"message": "전체 메일의 핵심 주제는?", "method": "global"},
        ]
    """
    results = []
    for i, q in enumerate(queries, 1):
        print(f"\n{'#'*60}")
        print(f"  테스트 {i}/{len(queries)}")
        res = run_graphrag_query(
            message=q["message"],
            method=q.get("method", "local"),
            response_type=q.get("response_type", "text"),
            root=root,
            verbose=True,
            korean=q.get("korean", True),
        )
        results.append({"query": q["message"], **res})

    # ── 결과 요약 출력 ────────────────────────────────────────────────────
    print(f"\n\n{'='*60}")
    print("  전체 테스트 결과 요약")
    print(f"{'='*60}")
    for r in results:
        status = "✅ OK" if r["returncode"] == 0 else "❌ FAIL"
        print(f"{status}  [{r['elapsed_sec']}s]  Q: {r['query'][:50]}")


# ── 인터랙티브 REPL 모드 ─────────────────────────────────────────────────────
def interactive_mode(root: str = "./src/parquet") -> None:
    print("\n🔍 GraphRAG 대화형 테스트 모드 (종료: q 또는 quit)")
    print("  method 변경: /local  /global")
    print("  verbose 토글: /verbose\n")

    method = "local"
    verbose = True

    while True:
        try:
            user_input = input(f"[{method}] 질문 > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n종료합니다.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("q", "quit", "exit"):
            break
        if user_input == "/local":
            method = "local"
            print("  → method: local")
            continue
        if user_input == "/global":
            method = "global"
            print("  → method: global")
            continue
        if user_input == "/verbose":
            verbose = not verbose
            print(f"  → verbose: {verbose}")
            continue

        run_graphrag_query(user_input, method=method, root=root, verbose=verbose)


# ── 메인 진입점 ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ── 1) 사전 정의 배치 테스트 ─────────────────────────────────────────
    BATCH_QUERIES = [
    # Q1 - 최근 메일 디테일 (Local)
    {
        "message": (
            "가장 최근에 받은 이메일 한 통을 기준으로, "
            "[제목, 발신자, 수신자, 날짜, 핵심 내용 요약(3문장), 내가 다음에 해야 할 액션]을 정리해줘. "
            "가능하다면 원문에서 근거가 된 문장도 함께 보여줘."
        ),
        "method": "local",
        "response_type": "text",
    },
    
    # Q2 - 액션/마감 추출 (Local)
    {
        "message": (
            "내 전체 이메일을 모두 살펴보고, "
            "내가 앞으로 처리해야 할 '액션'과 '마감/약속'만 정리해서 알려줘. "
            "각 항목마다 [관련 이메일 제목, 발신자, 마감 날짜나 약속 시간, 내가 해야 할 일, 원문 근거 문장]을 포함해줘. "
            "가능한 한 많이 찾아주되, 의미 없는 광고나 단순 알림은 제외해줘."
        ),
        "method": "local", 
        "response_type": "text",
    },
    
    # Q3 - 전체 패턴 요약 (Global)
    {
        "message": (
            "내 전체 이메일을 분석해서, "
            "요즘 내 메일함에서 주로 오가는 주제와 업무/생활 영역을 정리해줘. "
            "큰 주제 카테고리 3~7개를 뽑아줘 (예: 학교/수업, 아르바이트, 교회, 보안 알림, SNS 알림 등). "
            "각 카테고리마다 [한 줄 설명, 대표적인 발신자 3개, 대표 이메일 제목 예시 3개, 내 삶에서 어떤 역할인지 분석]. "
            "가능하면 표 형식(마크다운 테이블)으로 정리해줘."
        ),
        "method": "global",
        "response_type": "text",
    },
]


    print("=" * 60)
    print("  GmailWeaver — GraphRAG Query 테스트 스크립트")
    print("=" * 60)
    print("\n[1] 배치 테스트 실행")
    print("[2] 인터랙티브 모드")
    print("[3] 종료")

    choice = input("\n선택 > ").strip()

    if choice == "1":
        batch_test(BATCH_QUERIES)
    elif choice == "2":
        interactive_mode()
    else:
        print("종료합니다.")
