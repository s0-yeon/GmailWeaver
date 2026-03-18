import time
import os
import sys
import subprocess

from util.jobs.job_store import *
from util.graphrag_progress import parse_graphrag_progress

from config.setting import GRAPH_BUILD_SCRIPT, GRAPHRAG_ROOT

# [ 백그라운드 ] 메일 텍스트를 그래프 데이터 JSON으로 변환 시작
def build_graph_json(job_id, env):
    update_job(job_id, progress=5, message="메일 텍스트를 그래프 데이터 JSON으로 변환 중")

    p = subprocess.Popen(
        [sys.executable, "-X", "utf8", GRAPH_BUILD_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    for line in p.stdout:
        line = line.rstrip("\n")
        print("[JOB][mail2json]", line)
        append_job_log(job_id, line)

    rc = p.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, [sys.executable, GRAPH_BUILD_SCRIPT])

    update_job(job_id, progress=15, message="그래프 데이터 JSON 생성 완료")

# [ 백그라운드 ] GraphRAG 인덱싱 시작
def build_graphrag_index(job_id, env):
    update_job(job_id, progress=20, message="GraphRAG 인덱싱 시작")

    p = subprocess.Popen(
        [sys.executable, "-X", "utf8", "-m", "graphrag", "index", "--root", GRAPHRAG_ROOT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    current_progress = 20

    for line in p.stdout:
        line = line.rstrip("\n")
        print("[JOB][graphrag]", line)
        append_job_log(job_id, line)

        new_progress, new_message = parse_graphrag_progress(line, current_progress)

        if new_progress != current_progress or new_message:
            current_progress = new_progress
            update_job(
                job_id,
                progress=current_progress,
                message=new_message or f"인덱싱 진행 중 ({current_progress}%)"
            )

    rc = p.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(
            rc,
            [sys.executable, "-m", "graphrag", "index", "--root", GRAPHRAG_ROOT]
        )
    
def build_graphrag_update(job_id, env):
    update_job(job_id, progress=20, message="GraphRAG 업데이트 시작")

    p = subprocess.Popen(
        [sys.executable, "-X", "utf8", "-m", "graphrag", "update", "--root", GRAPHRAG_ROOT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    current_progress = 20

    for line in p.stdout:
        line = line.rstrip("\n")
        print("[JOB][graphrag][update]", line)
        append_job_log(job_id, line)

        new_progress, new_message = parse_graphrag_progress(line, current_progress)

        if new_progress != current_progress or new_message:
            current_progress = new_progress
            update_job(
                job_id,
                progress=current_progress,
                message=new_message or f"업데이트 진행 중 ({current_progress}%)"
            )

    rc = p.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(
            rc,
            [sys.executable, "-m", "graphrag", "update", "--root", GRAPHRAG_ROOT]
        )