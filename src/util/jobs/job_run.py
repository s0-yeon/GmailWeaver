import time
import os
import sys
import re
import subprocess
import threading  
import traceback
import openai
import networkx as nx

from util.jobs.job_store import update_job, append_job_log
from util.graphrag_progress import parse_graphrag_progress  # 현재 버전에서는 실시간 파싱에 사용 안 함
from util.user_path import user_graphrag_init
# from config.settings import GRAPH_BUILD_SCRIPT, GRAPHRAG_ROOT, BASE_DIR

from config.settings import MAIL_BLOCK_SEP


# 첨부파일 텍스트 요약 (공백/줄바꿈 제외 500자 미만이면 원문 그대로 반환)
def _summarize_attachment_text(text: str,paths, filename: str) -> str:
    pure_len = len(text.replace(" ", "").replace("\n", ""))
    if pure_len < 500:
        return text  # 짧은 텍스트는 요약 없이 그대로 반환

    prompt_path = os.path.join(paths.GRAPHRAG_ROOT, "prompts", "summarize_attachment.txt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read().strip()

    client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"파일명: {filename}\n\n{text}"}
            ],
            max_tokens=150  # 한글 약 300자 기준
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[summarize_attachment error] {filename}: {e}")
        return text  # 실패하면 원문 그대로 반환


# 요약된 첨부 텍스트를 mail_latest.txt 각 메일 블록 하단에 삽입
def _merge_summarized_attachments(mail_latest_path: str, attachment_texts_by_mail: dict):
    if not os.path.exists(mail_latest_path):
        return

    with open(mail_latest_path, "r", encoding="utf-8") as f:
        content = f.read()

    parts = content.split(MAIL_BLOCK_SEP)
    merged_blocks = []

    for part in parts:
        block = part.strip()
        if not block:
            continue

        # 구분선 복원
        block_text = f"{MAIL_BLOCK_SEP}\n{block}\n{MAIL_BLOCK_SEP}"

        # 블록에서 메일 ID 추출
        m = re.search(r"^\s*ID:\s*(.+?)\s*$", block_text, re.MULTILINE)
        mail_id = m.group(1).strip() if m else None

        # 해당 메일 ID에 첨부 내용이 없으면 그대로 추가
        if not mail_id or mail_id not in attachment_texts_by_mail:
            merged_blocks.append(block_text)
            continue

        # 첨부 내용 섹션 생성
        attachment_section = "\n[첨부 추출 내용]\n"
        for item in attachment_texts_by_mail[mail_id]:
            attachment_section += f"[File name] {item['name']}\n{item['text']}\n"

        # 블록 하단(마지막 구분선 직전)에 첨부 내용 삽입
        insert_pos = block_text.rfind(MAIL_BLOCK_SEP)
        if insert_pos == -1:
            merged_blocks.append(block_text + attachment_section)
        else:
            merged_blocks.append(
                block_text[:insert_pos].rstrip() + "\n\n" +
                attachment_section.rstrip() + "\n" +
                MAIL_BLOCK_SEP
            )

    # 병합 결과를 mail_latest.txt에 덮어씀
    with open(mail_latest_path, "w", encoding="utf-8") as f:
        f.write("\n".join(merged_blocks) + "\n")


# 백그라운드: 메일 텍스트를 그래프 데이터 JSON으로 변환
def build_graph_json(job_id, paths, env):
    print(f"[JOB][mail2json] START job_id={job_id}")
    print(f"[JOB][mail2json] cwd={os.getcwd()}")
    print(f"[JOB][mail2json] sys.executable={sys.executable}")
    print(f"[JOB][mail2json] GRAPH_BUILD_SCRIPT={paths.GRAPH_BUILD_SCRIPT}")
    print(f"[JOB][mail2json] script_exists={os.path.exists(paths.GRAPH_BUILD_SCRIPT)}")

    update_job(job_id, progress=5, message="메일 텍스트를 그래프 데이터 JSON으로 변환 중")
    append_job_log(job_id, "[START] build_graph_json")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPH_BUILD_SCRIPT={paths.GRAPH_BUILD_SCRIPT}")
    append_job_log(job_id, f"[INFO] script_exists={os.path.exists(paths.GRAPH_BUILD_SCRIPT)}")

    # GraphRAG CLI 실행 명령어 구성
    cmd = [
        sys.executable, "-u", "-X", "utf8", 
        paths.GRAPH_BUILD_SCRIPT,
        "--base-dir", paths.BASE_DIR,
        "--gmail-id", paths.GMAIL_ID
        ]
    print(f"[JOB][mail2json] CMD={cmd}")

    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        # 파이썬 스크립트 실행
        subprocess.run(
            cmd,
            check=True,         # 실패 시 exception 발생
            stdout=sys.stdout,  # 출력 → 서버 콘솔로 바로 전달
            stderr=sys.stderr,  # 에러 → 서버 콘솔로 바로 전달
            env=env,            # 환경변수 전달
        )

        append_job_log(job_id, "[END] build_graph_json success")
        update_job(job_id, progress=15, message="그래프 데이터 JSON 생성 완료")
        print(f"[JOB][parquet2json] SUCCESS job_id={job_id}")

    except Exception as e:
        print(f"[JOB][parquet2json][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graph_json failed: {e}")
        raise


# 백그라운드: GraphRAG 인덱싱
def build_graphrag_index(job_id, paths,env):
    print(f"[JOB][graphrag] START job_id={job_id}")
    print(f"[JOB][graphrag] cwd={os.getcwd()}")
    print(f"[JOB][graphrag] sys.executable={sys.executable}")
    print(f"[JOB][graphrag] GRAPHRAG_ROOT={paths.GRAPHRAG_ROOT}")
    print(f"[JOB][graphrag] root_exists={os.path.exists(paths.GRAPHRAG_ROOT)}")

    update_job(job_id, progress=20, message="GraphRAG 인덱싱 시작")
    append_job_log(job_id, "[START] build_graphrag_index")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPHRAG_ROOT={paths.GRAPHRAG_ROOT}")
    append_job_log(job_id, f"[INFO] root_exists={os.path.exists(paths.GRAPHRAG_ROOT)}")

    user_graphrag_init(paths)

    # GraphRAG CLI 실행 명령어 구성
    cmd = [
        sys.executable,
        "-u",              # stdout/stderr 버퍼링 최소화
        "-X", "utf8",
        "-m", "graphrag",  # graphrag 모듈 실행
        "index",           # graphrag 모듈의 index 명령
        "--root", paths.GRAPHRAG_ROOT
    ]

    env = env.copy()
    env["PYTHONUNBUFFERED"] = "1"

    print(f"[JOB][graphrag] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        # 세밀한 진행률 파싱 대신 단계 진행률만 갱신
        update_job(job_id, progress=30, message="GraphRAG 인덱싱 실행 중")

        # 파이썬 스크립트 실행
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


# 백그라운드: GraphRAG 업데이트 (증분)
def build_graphrag_update(job_id,paths, env):
    print(f"[JOB][graphrag-update] START job_id={job_id}")
    print(f"[JOB][graphrag-update] cwd={os.getcwd()}")
    print(f"[JOB][graphrag-update] sys.executable={sys.executable}")
    print(f"[JOB][graphrag-update] GRAPHRAG_ROOT={paths.GRAPHRAG_ROOT}")
    print(f"[JOB][graphrag-update] root_exists={os.path.exists(paths.GRAPHRAG_ROOT)}")

    update_job(job_id, progress=20, message="GraphRAG 업데이트 시작")
    append_job_log(job_id, "[START] build_graphrag_update")
    append_job_log(job_id, f"[INFO] cwd={os.getcwd()}")
    append_job_log(job_id, f"[INFO] sys.executable={sys.executable}")
    append_job_log(job_id, f"[INFO] GRAPHRAG_ROOT={paths.GRAPHRAG_ROOT}")
    append_job_log(job_id, f"[INFO] root_exists={os.path.exists(paths.GRAPHRAG_ROOT)}")
    

    # GraphRAG CLI 실행 명령어 구성
    cmd = [
        sys.executable,
        "-u",              # stdout/stderr 버퍼링 최소화
        "-X", "utf8",
        "-m", "graphrag",  # graphrag 모듈 실행
        "update",          # graphrag 모듈 update 명령
        "--root", paths.GRAPHRAG_ROOT
    ]

    env = env.copy()
    env["PYTHONUNBUFFERED"] = "1"

    print(f"[JOB][graphrag] CMD={cmd}")
    append_job_log(job_id, f"[CMD] {cmd}")

    try:
        update_job(job_id, progress=30, message="GraphRAG 업데이트 실행 중")

        subprocess.run(
            cmd,
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

        append_job_log(job_id, "[END] build_graphrag_update success")
        update_job(job_id, progress=90, message="GraphRAG 업데이트 완료")
        print(f"[JOB][graphrag-update] SUCCESS job_id={job_id}")

        # 새로운 데이터 graphml과 현재 존재하는 output graphml 병합
        output_graphml = os.path.join(paths.GRAPHRAG_ROOT, "output", "graph.graphml")
        update_output_dir = os.path.join(paths.GRAPHRAG_ROOT, "update_output")
        latest = sorted(os.listdir(update_output_dir))[-1]
        delta_graphml = os.path.join(update_output_dir, latest, "delta", "graph.graphml")

        if os.path.exists(delta_graphml):
            G_output = nx.read_graphml(output_graphml)  # 기존 graphml
            G_delta = nx.read_graphml(delta_graphml)    # 새로운 graphml
            G_merged = nx.compose(G_output, G_delta)    # 두 graphml 병합
            nx.write_graphml(G_merged, output_graphml)  # 병합 결과 기존 graphml에 덮어씀

    except Exception as e:
        print(f"[JOB][graphrag-update][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()
        append_job_log(job_id, f"[ERROR] build_graphrag_update failed: {e}")
        raise


# 전체 파이프라인 실행 (index 기준)


def run_graph_pipeline(job_id,paths, env, attachment_texts_by_mail=None):
    print(f"[JOB][pipeline] START job_id={job_id}")
    append_job_log(job_id, "[START] run_graph_pipeline")

    user_graphrag_init(paths)
    try:
        update_job(job_id, progress=0, status="running", message="작업 시작")

        # 첨부파일 요약 후 mail_latest.txt에 병합 (백그라운드에서 처리)
        if attachment_texts_by_mail:
            print(f"[JOB][summarize] START job_id={job_id}")
            update_job(job_id, progress=5, message="첨부파일 요약 중")

            # 각 첨부파일 텍스트를 요약으로 교체
            summarized_by_mail = {}
            for mail_id, items in attachment_texts_by_mail.items():
                summarized_by_mail[mail_id] = [
                    {
                        "name": item["name"],
                        "text": _summarize_attachment_text(item["text"],paths, item["name"])
                    }
                    for item in items
                ]

            # 요약된 첨부 내용을 mail_latest.txt 각 블록에 삽입
            _merge_summarized_attachments(paths.MAIL_LATEST_PATH, summarized_by_mail)
            print(f"[JOB][summarize] DONE job_id={job_id}")

        build_graphrag_index(job_id,paths, env)
        build_graph_json(job_id,paths, env)


        update_job(job_id, progress=100, status="done", message="인덱싱 완료")
        append_job_log(job_id, "[END] run_graph_pipeline success")
        print(f"[JOB][pipeline] SUCCESS job_id={job_id}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        update_job(job_id, status="failed", message=error_msg)
        append_job_log(job_id, f"[ERROR] run_graph_pipeline failed: {error_msg}")
        print(f"[JOB][pipeline][ERROR] job_id={job_id} error={error_msg}")
        traceback.print_exc()


# 업데이트 파이프라인 실행
def run_graph_update_pipeline(job_id, paths, env):
    print(f"[JOB][update-pipeline] START job_id={job_id}")
    append_job_log(job_id, "[START] run_graph_update_pipeline")

    user_graphrag_init(paths)
    try:
        update_job(job_id, progress=0, status="running", message="업데이트 작업 시작")

        # 1단계: graphrag 업데이트
        build_graphrag_update(job_id,paths, env)

        # 2단계: json 생성 
        build_graph_json(job_id,paths, env)


        update_job(job_id, progress=100, status="done", message="업데이트 완료")
        append_job_log(job_id, "[END] run_graph_update_pipeline success")
        print(f"[JOB][update-pipeline] SUCCESS job_id={job_id}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        update_job(job_id, status="failed", message=error_msg, error=error_msg)
        append_job_log(job_id, f"[ERROR] run_graph_update_pipeline failed: {error_msg}")
        print(f"[JOB][update-pipeline][ERROR] job_id={job_id} error={error_msg}")
        traceback.print_exc()


# 백그라운드 전체 파이프라인 실행 (index 기준)

def start_graph_pipeline_background(job_id,paths, env, attachment_texts_by_mail=None):
    print(f"[JOB][pipeline] BACKGROUND START job_id={job_id}")
    append_job_log(job_id, "[INFO] background thread starting")

    # 새로운 스레드 생성
    t = threading.Thread(

        target=run_graph_pipeline,  # 실행할 함수: 그래프라그 파이프라인 (인덱싱) 실행 함수
        args=(job_id, paths, env.copy(), attachment_texts_by_mail),
        daemon=True,                # app.py 종료 시 같이 종료
    )
    t.start()  # 스레드 실행 (비동기 시작)

    print(f"[JOB][pipeline] BACKGROUND THREAD STARTED job_id={job_id} thread={t.name}")
    append_job_log(job_id, f"[INFO] background thread started name={t.name}")
    return t


# 백그라운드 스레드 시작 - 업데이트
def start_graph_update_pipeline_background(job_id,paths, env):
    print(f"[JOB][update-pipeline] BACKGROUND START job_id={job_id}")
    append_job_log(job_id, "[INFO] update background thread starting")

    t = threading.Thread(
        target=run_graph_update_pipeline, # 실행할 함수 : 그래프라그 업데이트파이프라인 실행 함수
        args=(job_id,paths, env.copy()),
        daemon=True,                      # app.py 종료 시 같이 종료

    )
    t.start()  # 스레드 실행 (비동기 시작)

    print(f"[JOB][update-pipeline] BACKGROUND THREAD STARTED job_id={job_id} thread={t.name}")
    append_job_log(job_id, f"[INFO] update background thread started name={t.name}")
    return t