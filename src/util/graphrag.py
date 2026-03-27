import os
import time 
import traceback  

from util.jobs.job_store import update_job
from util.jobs.job_run import build_graph_json,build_graphrag_index,build_graphrag_update

# 메일데이터 그래프 데이터 JSON, GraphRAG 인덱싱을 수행하는 파이프라인 
def run_graph_pipeline(job_id):
    print(f"[PIPELINE] start job_id={job_id}")

    try:
        # 작업 상태를 running으로 변경
        update_job(job_id, status="running", progress=1, message="그래프 파이프라인 시작")
        print(f"[PIPELINE] job updated to running job_id={job_id}")

        env = os.environ.copy() # 현재 프로세스의 환경변수를 복사
        env["PYTHONUTF8"] = "1" # Python이 UTF-8 모드로 동작하도록 설정, 한글/특수문자 깨짐 방지 목적
        env["PYTHONIOENCODING"] = "utf-8" # 표준입출력 인코딩을 utf-8로 강제
        env["RICH_DISABLE"] = "1" # rich 라이브러리의 컬러/장식 출력 비활성화, 로그 파일이나 콘솔에서 ANSI escape 문자 깨짐 방지

        print(f"[PIPELINE][INDEX] env prepared job_id={job_id}")
        print(f"[PIPELINE][INDEX] cwd={os.getcwd()} job_id={job_id}")

        # GraphRAG 전체 인덱싱 시작
        print(f"[PIPELINE][INDEX] calling build_graphrag_index job_id={job_id}")
        build_graphrag_index(job_id, env)
        print(f"[PIPELINE][INDEX] build_graphrag_index DONE job_id={job_id}")

        # 인덱싱이 끝난 후 그래프 시각화용 JSON 생성
        print(f"[PIPELINE][INDEX] calling build_graph_json job_id={job_id}")
        build_graph_json(job_id, env)
        print(f"[PIPELINE][INDEX] build_graph_json DONE job_id={job_id}")

        # 모든 작업이 성공적으로 끝났으면 상태를 done으로 변경
        update_job(
            job_id,
            status="done",
            progress=100,
            message="JSON 변환, GraphRAG 인덱싱 완료",
            finished_at=time.time(),                    # 완료 시각 기록
        )
        print(f"[PIPELINE][INDEX] finished job_id={job_id}")

    except Exception as e:
        # 파이프라인 실행 중 예외 발생 시 로그 출력
        print(f"[PIPELINE][INDEX][ERROR] job_id={job_id} error={e}")
        traceback.print_exc()   # 자세한 에러 스택 출력

        try:
            # 작업 상태를 failed로 저장
            update_job(
                job_id,
                status="failed",
                progress=100,                   # 실패했더라도 작업 종료이므로 100
                message="그래프 파이프라인 실패",
                error=str(e),                   # 실제 에러 문자열 저장
                finished_at=time.time(),        # 실패 시각 기록
            )
        except Exception as inner_e:
            # 실패 상태 저장을 실패한 경우
            print(f"[PIPELINE][INDEX][ERROR] failed to save failed status job_id={job_id} error={inner_e}")
            traceback.print_exc()

# 메일데이터 그래프 데이터 JSON, GraphRAG 업데이트를 수행하는 파이프라인 
def run_graph_update_pipeline(job_id):
    print(f"[PIPELINE][UPDATE] start job_id={job_id}")

    try:
        # 작업 상태 running로 변경
        update_job(job_id, status="running", progress=1, message="그래프 업데이트 파이프라인 시작")
        print(f"[PIPELINE][UPDATE] job updated to running job_id={job_id}")

        env = os.environ.copy() # 현재 프로세스의 환경변수를 복사
        env["PYTHONUTF8"] = "1" # Python이 UTF-8 모드로 동작하도록 설정, 한글/특수문자 깨짐 방지 목적
        env["PYTHONIOENCODING"] = "utf-8" # 표준입출력 인코딩을 utf-8로 강제
        env["RICH_DISABLE"] = "1" # rich 라이브러리의 컬러/장식 출력 비활성화, 로그 파일이나 콘솔에서 ANSI escape 문자 깨짐 방지

        print(f"[PIPELINE][UPDATE] env prepared job_id={job_id}")
        print(f"[PIPELINE][UPDATE] cwd={os.getcwd()} job_id={job_id}")

        # 인덱싱이 끝난 후 그래프 시각화용 JSON 생성
        print(f"[PIPELINE][UPDATE] calling build_graph_json job_id={job_id}")
        build_graphrag_update(job_id, env) 
        print(f"[PIPELINE][UPDATE] build_graph_json DONE job_id={job_id}")
        # 그래프라그 업데이트 시작
        print(f"[PIPELINE][UPDATE] calling build_graphrag_update job_id={job_id}")
        build_graph_json(job_id, env)
        print(f"[PIPELINE][UPDATE] build_graphrag_update DONE job_id={job_id}")

        # 모든 작업이 성공적으로 끝났으면 상태를 done으로 변경
        update_job(
            job_id,
            status="done",
            progress=100,
            message="JSON 변환, GraphRAG 업데이트 완료",
            finished_at=time.time(), # 완료 시각 기록
        )
        print(f"[PIPELINE][UPDATE] finished job_id={job_id}")

    except Exception as e:
        # 파이프라인 실행 중 예외 발생 시 로그 출력
        print(f"[PIPELINE][UPDATE][ERROR] job_id={job_id} error={e}")
        traceback.print_exc() # 자세한 에러 스택 출력

        try:
            # 작업 상태를 failed로 저장
            update_job(
                job_id,
                status="failed",
                progress=100, # 실패했더라도 작업 종료이므로 100
                message="그래프 업데이트 파이프라인 실패",
                error=str(e), # 실제 에러 문자열 저장
                finished_at=time.time(), # 실패 시각 기록
            )
        except Exception as inner_e:
            # 실패 상태 저장을 실패한 경우
            print(f"[PIPELINE][UPDATE][ERROR] failed to save failed status job_id={job_id} error={inner_e}")
            traceback.print_exc()