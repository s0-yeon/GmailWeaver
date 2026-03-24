import os
import time 
import traceback

from util.jobs.job_store import update_job
from util.jobs.job_run import build_graph_json,build_graphrag_index,build_graphrag_update

# 메일데이터 그래프 데이터 JSON, GraphRAG 인덱싱을 수행하는 파이프라인 
def run_graph_pipeline(job_id):
    print(f"[PIPELINE] start job_id={job_id}")

    try:
        update_job(job_id, status="running", progress=1, message="그래프 파이프라인 시작")
        print(f"[PIPELINE] job updated to running job_id={job_id}")

        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["RICH_DISABLE"] = "1"

        print(f"[PIPELINE][INDEX] env prepared job_id={job_id}")
        print(f"[PIPELINE][INDEX] cwd={os.getcwd()} job_id={job_id}")

        print(f"[PIPELINE][INDEX] calling build_graphrag_index job_id={job_id}")
        build_graphrag_index(job_id, env)
        print(f"[PIPELINE][INDEX] build_graphrag_index DONE job_id={job_id}")

        print(f"[PIPELINE][INDEX] calling build_graph_json job_id={job_id}")
        build_graph_json(job_id, env)
        print(f"[PIPELINE][INDEX] build_graph_json DONE job_id={job_id}")

        update_job(
            job_id,
            status="done",
            progress=100,
            message="JSON 변환, GraphRAG 인덱싱 완료",
            finished_at=time.time(),
        )
        print(f"[PIPELINE][INDEX] finished job_id={job_id}")

    except Exception as e:
        print(f"[PIPELINE][INDEX][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()

        try:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="그래프 파이프라인 실패",
                error=str(e),
                finished_at=time.time(),
            )
        except Exception as inner_e:
            print(f"[PIPELINE][INDEX][ERROR] failed to save failed status job_id={job_id} error={inner_e}")
            traceback.print_exc()

# 메일데이터 그래프 데이터 JSON, GraphRAG 업데이트를 수행하는 파이프라인 
def run_graph_update_pipeline(job_id):
    print(f"[PIPELINE][UPDATE] start job_id={job_id}")

    try:
        update_job(job_id, status="running", progress=1, message="그래프 업데이트 파이프라인 시작")
        print(f"[PIPELINE][UPDATE] job updated to running job_id={job_id}")

        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["RICH_DISABLE"] = "1"

        print(f"[PIPELINE][UPDATE] env prepared job_id={job_id}")
        print(f"[PIPELINE][UPDATE] cwd={os.getcwd()} job_id={job_id}")

        print(f"[PIPELINE][UPDATE] calling build_graph_json job_id={job_id}")
        build_graph_json(job_id, env)
        print(f"[PIPELINE][UPDATE] build_graph_json DONE job_id={job_id}")

        print(f"[PIPELINE][UPDATE] calling build_graphrag_update job_id={job_id}")
        build_graphrag_update(job_id, env)
        print(f"[PIPELINE][UPDATE] build_graphrag_update DONE job_id={job_id}")

        update_job(
            job_id,
            status="done",
            progress=100,
            message="JSON 변환, GraphRAG 인덱싱 완료",
            finished_at=time.time(),
        )
        print(f"[PIPELINE][UPDATE] finished job_id={job_id}")

    except Exception as e:
        print(f"[PIPELINE][UPDATE][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()

        try:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="그래프 업데이트 파이프라인 실패",
                error=str(e),
                finished_at=time.time(),
            )
        except Exception as inner_e:
            print(f"[PIPELINE][UPDATE][ERROR] failed to save failed status job_id={job_id} error={inner_e}")
            traceback.print_exc()