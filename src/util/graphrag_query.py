# src/util/graphrag_query.py

import os
import re
import asyncio
import traceback
import threading
import time


#_run_graphrag() 대체용
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
                engine = get_engine(paths.GMAIL_ID, output_dir, paths.GRAPHRAG_ROOT)
                result = await engine.search(message)
                answer = result.response
                answer = re.sub(r'\[Data:.*?\]|\[데이터:.*?\]', '', answer)
                answer = re.sub(r'\*+|#+', '', answer)
                return answer.strip()

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
    print(f"[ENGINE] 답변: {result_container['result']}")
    return result_container["result"]