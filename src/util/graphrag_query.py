# src/util/graphrag_query.py
# 캐싱된 서치 엔진 객체 직접 호출해서 검색 속도 개선함

import os
import re
import asyncio # 비동기 실행 지원 (LocalSearch/GlobalSearch.search()가 async 함수라 필요)
import traceback
import threading
import time
from util.graphrag_engine import get_engines, get_and_reset_usage # 유저별 캐싱된 local. global 엔진 반환 함수 임포트
from util.database.db_writer import save_query_to_db

# cli 호출 방식인 _run_graphrag() 대체용 (get_engines()로 캐싱된 LocalSearch, globalSearch 객체 직접 호출)
def run_graphrag_query(message: str, original_message: str, paths, method: str = "local") -> tuple[str, list]:
    start_time = time.time()
    result_container = {"result": None, "error": None} # 스레드 간에 결과나 에러를 공유하기 위한 컨테이너 (스레드 return 값 직접 전달 못해서 dict로 우회함)

    def _run(): # 별도 스레드에서 실행할 함수 (플라스크가 자체 이벤트 루프 갖고 있어서 asyncio.run() 바로 쓰면 충돌날수도 있음)
        loop = asyncio.new_event_loop() # 현재 스레드 전용 새 이벤트 루프 생성
        asyncio.set_event_loop(loop) # 현재 스레드의 기본 루프로 설정
        try:
            async def _search(): # 실제 검색 로직 담은 함수
                output_dir = os.path.join(paths.GRAPHRAG_ROOT, "output")
                local_engine, global_engine = get_engines(paths.GMAIL_ID, output_dir, paths.GRAPHRAG_ROOT) # 유저별 캐싱된 local + global 엔진 둘 다 가져오기 (캐시에서 재사용)
                engine = local_engine if method == "local" else global_engine
                result = await engine.search(message) # cli subprocess 대신 엔진 객체 함수 호출 (subprocess 생성이나 종료가 없어서 속도 빨라짐)
                answer = result.response # 검색 결과 객체에서 답변 텍스트 추출
                answer = re.sub(r'\[Data:.*?\]|\[데이터:.*?\]', '', answer) # graphrag가 답변에 삽입하는 출처 태그 제거
                answer = re.sub(r'\*+|#+', '', answer) # 마크다운 강조 기호 제거 (**, ## 등)
                answer = answer.strip() # 앞뒤 공백 제거

                # 1차: 답변 텍스트에서 ID 추출
                found = re.findall(r'ID:\s*([0-9a-fA-F]{16})', answer)

                # 2차: LLM이 답변에 ID를 직접 안 썼을 때 → context_text(LLM에 넘긴 원본 청크)에서 추출
                if not found:
                    ctx = result.context_text
                    if isinstance(ctx, list):
                        ctx = '\n'.join(ctx)
                    if isinstance(ctx, str):
                        found = re.findall(r'ID:\s*([0-9a-fA-F]{16})', ctx)

                # 순서 유지하면서 중복 제거
                seen = set()
                source_ids = []
                for id in found:
                    if id not in seen:
                        seen.add(id)
                        source_ids.append(id)

                return answer, source_ids # 답변 텍스트와 근거 메일 ID 목록을 튜플로 반환

            result_container["result"] = loop.run_until_complete(_search())

        except Exception as e:
            traceback.print_exc()
            result_container["error"] = e
        finally:
            loop.close()

    # 완전히 새로운 스레드에서 _run 실행
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=120)  # 최대 120초 대기. 120초 넘어도 답이 안 오면 런타임에러 발생 및 CLI fallback로 넘어감. (스레드 종료 ㄴㄴ)

    if t.is_alive():
        raise RuntimeError("graphrag 검색 타임아웃 (120초)")

    if result_container["error"]:
        raise result_container["error"]

    elapsed = time.time() - start_time
    print(f"[ENGINE] 검색 완료: {elapsed:.2f}초")
    answer, source_ids = result_container["result"]  # 언패킹
    print(f"[ENGINE] 답변: {answer}")
    print(f"[ENGINE] source_ids: {source_ids}")
    try:
        usage = get_and_reset_usage(paths.GMAIL_ID, method)
        save_query_to_db(
            paths.GMAIL_ID, original_message, elapsed, method,
            model_name=usage["model_name"],
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
        )
    except Exception as e:
        print(f"[WARN] query DB 저장 실패 (무시): {e}")
    return answer, source_ids  # app.py의 _worker()로 튜플 반환