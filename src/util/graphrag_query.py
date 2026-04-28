# src/util/graphrag_query.py

import os
import re
import asyncio
import traceback
import threading
import time
from util.graphrag_engine import get_engine

# cli 호출 방식인 _run_graphrag() 대체용 (get_engine()로 캐싱된 LocalSearch 객체 직접 호출)
def run_graphrag_query(message: str, paths) -> str:
    from util.graphrag_engine import get_engine
    start_time = time.time()
    result_container = {"result": None, "error": None}

    def _run():
        # 별도 스레드에서 새 이벤트 루프 생성 및 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def _search():
                output_dir = os.path.join(paths.GRAPHRAG_ROOT, "output")
                engine = get_engine(paths.GMAIL_ID, output_dir, paths.GRAPHRAG_ROOT) # 유저별 캐싱된 엔진 객체 가져오기
                result = await engine.search(message) # cli subprocess 대신 엔진 객체 함수 호출
                answer = result.response
                answer = re.sub(r'\[Data:.*?\]|\[데이터:.*?\]', '', answer)
                answer = re.sub(r'\*+|#+', '', answer)
                answer = answer.strip()
            
                # 답변 텍스트에서만 ID 추출 (sources 전체 아님), 답변 나온 순서 유지하면서 중복 제거
                found = re.findall(r'ID:\s*([0-9a-fA-F]{16})', answer)
                seen = set()
                source_ids = []
                for id in found:
                    if id not in seen:
                        seen.add(id)
                        source_ids.append(id)

                return answer, source_ids

            result_container["result"] = loop.run_until_complete(_search())
        except Exception as e:
            traceback.print_exc()
            result_container["error"] = e
        finally:
            loop.close()

    # 완전히 새로운 스레드에서 실행
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=120)  # 최대 120초 대기

    if t.is_alive():
        raise RuntimeError("graphrag 검색 타임아웃 (120초)")

    if result_container["error"]:
        raise result_container["error"]

    print(f"[ENGINE] 검색 완료: {time.time() - start_time:.2f}초")
    answer, source_ids = result_container["result"]  # 언패킹
    print(f"[ENGINE] 답변: {answer}")
    print(f"[ENGINE] source_ids: {source_ids}")
    return answer, source_ids  # 튜플로 반환