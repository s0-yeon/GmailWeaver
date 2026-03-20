import time
import os
import sys
import subprocess
import threading
import traceback

from util.jobs.job_store import update_job, append_job_log
from util.graphrag_progress import parse_graphrag_progress

from config.setting import GRAPH_BUILD_SCRIPT, GRAPHRAG_ROOT, BASE_DIR

# [ 백그라운드 ] 메일 텍스트를 그래프 데이터 JSON으로 변환 시작
def build_graph_json(job_id, env):
    print(f"[JOB][mail2json] START job_id={job_id}")
    print(f"[JOB][mail2json] cwd={os.getcwd()}")
    print(f"[JOB][mail2json] sys.executable={sys.executable}")
    print(f"[JOB][mail2json] GRAPH_BUILD_SCRIPT={GRAPH_BUILD_SCRIPT}")
    print(f"[JOB][mail2json] script_exists={os.path.exists(GRAPH_BUILD_SCRIPT)}")

    update_job(job_id, progress=5, message="메일 텍스트를 그래프 데이터 JSON으로 변환 중")
    append_job_log(job_id, f"[START] build_graph_json")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPH_BUILD_SCRIPT={GRAPH_BUILD_SCRIPT}")
    append_job_log(job_id, f"[INFO] script_exists={os.path.exists(GRAPH_BUILD_SCRIPT)}")

    cmd = [sys.executable, "-X", "utf8", GRAPH_BUILD_SCRIPT]
    print(f"[JOB][mail2json] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        print(f"[JOB][mail2json] process started pid={p.pid}")
        append_job_log(job_id, f"[INFO] process started pid={p.pid}")

        for line in p.stdout:
            line = line.rstrip("\n")
            print("[JOB][mail2json]", line)
            append_job_log(job_id, line)

        rc = p.wait()
        print(f"[JOB][mail2json] process ended rc={rc}")
        append_job_log(job_id, f"[END] rc={rc}")

        if rc != 0:
            raise subprocess.CalledProcessError(rc, cmd)

        update_job(job_id, progress=15, message="그래프 데이터 JSON 생성 완료")
        print(f"[JOB][mail2json] SUCCESS job_id={job_id}")

    except Exception as e:
        print(f"[JOB][mail2json][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graph_json failed: {e}")
        raise


def _stream_subprocess_output(pipe, job_id, prefix, progress_handler=None):
    last_output_time = time.time()

    try:
        for raw_line in iter(pipe.readline, ''):
            if not raw_line:
                break

            line = raw_line.rstrip("\n")
            last_output_time = time.time()

            print(f"{prefix} {line}")
            append_job_log(job_id, line)

            if progress_handler:
                progress_handler(line)

    except Exception as e:
        print(f"{prefix}[STREAM_ERROR] {e}")
        append_job_log(job_id, f"[STREAM_ERROR] {e}")

    return last_output_time


def build_graphrag_index(job_id, env):
    print(f"[JOB][graphrag] START job_id={job_id}")
    print(f"[JOB][graphrag] cwd={os.getcwd()}")
    print(f"[JOB][graphrag] sys.executable={sys.executable}")
    print(f"[JOB][graphrag] GRAPHRAG_ROOT={GRAPHRAG_ROOT}")
    print(f"[JOB][graphrag] root_exists={os.path.exists(GRAPHRAG_ROOT)}")

    update_job(job_id, progress=20, message="GraphRAG 인덱싱 시작")
    append_job_log(job_id, "[START] build_graphrag_index")

    cmd = [
        sys.executable,
        "-u",              # 중요: unbuffered
        "-X", "utf8",
        "-m", "graphrag",
        "index",
        "--root", GRAPHRAG_ROOT
    ]

    env = env.copy()
    env["PYTHONUNBUFFERED"] = "1"   # 중요: 버퍼링 최소화

    print(f"[JOB][graphrag] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    current_progress = 20
    last_output_time = time.time()

    def handle_progress(line):
        nonlocal current_progress, last_output_time
        last_output_time = time.time()

        try:
            new_progress, new_message = parse_graphrag_progress(line, current_progress)
        except Exception as parse_e:
            print(f"[JOB][graphrag][PARSE_ERROR] {parse_e} line={line}")
            append_job_log(job_id, f"[PARSE_ERROR] {parse_e} | line={line}")
            return

        if new_progress != current_progress or new_message:
            current_progress = new_progress
            update_job(
                job_id,
                progress=current_progress,
                message=new_message or f"인덱싱 진행 중 ({current_progress}%)"
            )

    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        print(f"[JOB][graphrag] process started pid={p.pid}")
        append_job_log(job_id, f"[INFO] process started pid={p.pid}")

        reader_thread = threading.Thread(
            target=_stream_subprocess_output,
            args=(p.stdout, job_id, "[JOB][graphrag]", handle_progress),
            daemon=True
        )
        reader_thread.start()

        last_heartbeat = 0

        while True:
            rc = p.poll()

            now = time.time()
            silence_sec = int(now - last_output_time)

            # 15초마다 heartbeat
            if now - last_heartbeat >= 15 and rc is None:
                msg = f"GraphRAG 실행 중... (최근 로그 없음 {silence_sec}초, pid={p.pid})"
                print(f"[JOB][graphrag][HEARTBEAT] {msg}")
                append_job_log(job_id, f"[HEARTBEAT] {msg}")
                update_job(job_id, message=msg)
                last_heartbeat = now

            if rc is not None:
                reader_thread.join(timeout=2)
                print(f"[JOB][graphrag] process ended rc={rc}")
                append_job_log(job_id, f"[END] rc={rc}")
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, cmd)
                break

            time.sleep(1)

        print(f"[JOB][graphrag] SUCCESS job_id={job_id}")

    except Exception as e:
        print(f"[JOB][graphrag][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graphrag_index failed: {e}")
        raise