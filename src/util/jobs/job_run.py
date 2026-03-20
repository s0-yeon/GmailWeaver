import time
import os
import sys
import subprocess
import threading
import traceback

from util.jobs.job_store import update_job, append_job_log
from util.graphrag_progress import parse_graphrag_progress  # 현재 버전에서는 실시간 파싱에 사용 안 함
from config.settings import GRAPH_BUILD_SCRIPT, GRAPHRAG_ROOT, BASE_DIR


# 백그라운드: 메일 텍스트를 그래프 데이터 JSON으로 변환
def build_graph_json(job_id, env):
    print(f"[JOB][mail2json] START job_id={job_id}")
    print(f"[JOB][mail2json] cwd={os.getcwd()}")
    print(f"[JOB][mail2json] sys.executable={sys.executable}")
    print(f"[JOB][mail2json] GRAPH_BUILD_SCRIPT={GRAPH_BUILD_SCRIPT}")
    print(f"[JOB][mail2json] script_exists={os.path.exists(GRAPH_BUILD_SCRIPT)}")

    update_job(job_id, progress=5, message="메일 텍스트를 그래프 데이터 JSON으로 변환 중")
    append_job_log(job_id, "[START] build_graph_json")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPH_BUILD_SCRIPT={GRAPH_BUILD_SCRIPT}")
    append_job_log(job_id, f"[INFO] script_exists={os.path.exists(GRAPH_BUILD_SCRIPT)}")

    cmd = [sys.executable, "-u", "-X", "utf8", GRAPH_BUILD_SCRIPT]
    print(f"[JOB][mail2json] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        # 원래처럼 자식 프로세스 로그를 현재 서버 콘솔에 직접 출력
        subprocess.run(
            cmd,
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

        append_job_log(job_id, "[END] build_graph_json success")
        update_job(job_id, progress=15, message="그래프 데이터 JSON 생성 완료")
        print(f"[JOB][mail2json] SUCCESS job_id={job_id}")

    except Exception as e:
        print(f"[JOB][mail2json][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graph_json failed: {e}")
        raise


# 백그라운드: GraphRAG 인덱싱
def build_graphrag_index(job_id, env):
    print(f"[JOB][graphrag] START job_id={job_id}")
    print(f"[JOB][graphrag] cwd={os.getcwd()}")
    print(f"[JOB][graphrag] sys.executable={sys.executable}")
    print(f"[JOB][graphrag] GRAPHRAG_ROOT={GRAPHRAG_ROOT}")
    print(f"[JOB][graphrag] root_exists={os.path.exists(GRAPHRAG_ROOT)}")

    update_job(job_id, progress=20, message="GraphRAG 인덱싱 시작")
    append_job_log(job_id, "[START] build_graphrag_index")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPHRAG_ROOT={GRAPHRAG_ROOT}")
    append_job_log(job_id, f"[INFO] root_exists={os.path.exists(GRAPHRAG_ROOT)}")

    cmd = [
        sys.executable,
        "-u",              # 중요: stdout/stderr 버퍼링 최소화
        "-X", "utf8",
        "-m", "graphrag",
        "index",
        "--root", GRAPHRAG_ROOT
    ]

    env = env.copy()
    env["PYTHONUNBUFFERED"] = "1"

    print(f"[JOB][graphrag] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        # 세밀한 진행률 파싱 대신 단계 진행률만 갱신
        update_job(job_id, progress=30, message="GraphRAG 인덱싱 실행 중")

        subprocess.run(
            cmd,
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

        append_job_log(job_id, "[END] build_graphrag_index success")
        update_job(job_id, progress=90, message="GraphRAG 인덱싱 완료")
        print(f"[JOB][graphrag] SUCCESS job_id={job_id}")

    except Exception as e:
        print(f"[JOB][graphrag][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graphrag_index failed: {e}")
        raise


# 전체 파이프라인 실행
def run_graph_pipeline(job_id, env):
    print(f"[JOB][pipeline] START job_id={job_id}")
    append_job_log(job_id, "[START] run_graph_pipeline")

    try:
        update_job(job_id, progress=0, status="running", message="작업 시작")

        build_graph_json(job_id, env)
        build_graphrag_index(job_id, env)

        update_job(job_id, progress=100, status="done", message="인덱싱 완료")
        append_job_log(job_id, "[END] run_graph_pipeline success")
        print(f"[JOB][pipeline] SUCCESS job_id={job_id}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        update_job(job_id, status="failed", message=error_msg)
        append_job_log(job_id, f"[ERROR] run_graph_pipeline failed: {error_msg}")
        print(f"[JOB][pipeline][ERROR] job_id={job_id} error={error_msg}")
        traceback.print_exc()


# 백그라운드 스레드 시작
def start_graph_pipeline_background(job_id, env):
    print(f"[JOB][pipeline] BACKGROUND START job_id={job_id}")
    append_job_log(job_id, "[INFO] background thread starting")

    t = threading.Thread(
        target=run_graph_pipeline,
        args=(job_id, env.copy()),
        daemon=True,
    )
    t.start()

    print(f"[JOB][pipeline] BACKGROUND THREAD STARTED job_id={job_id} thread={t.name}")
    append_job_log(job_id, f"[INFO] background thread started name={t.name}")
    return t
