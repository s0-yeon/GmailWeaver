import threading
import time

# 메모리 기반 Job 저장소 → 모든 작업 상태를 딕셔너리로 관리
# 서버 재시작 시 모든 Job 정보 소멸 (영속성 없음)
_jobs = {}
# 여려 쓰레드가 동시에 _jobs를 수정하는 걸 막기 위한 Lock
_jobs_lock = threading.Lock()

# Job 이용 공통함수
# JOb들의 형식을 통일하기 위함

# Job 생성 → 새로운 비동기 작업을 등록할 때 사용
def create_job(job_id, job_type="index"):
    with _jobs_lock:                      # lock → 다른 쓰레드 접근 차단
        _jobs[job_id] = {
            "job_id": job_id,             # 작업 고유 ID
            "job_type": job_type,         # 작업 종류, index / query / update 등
            "status": "queued",           # queued / running / done / failed
            "progress": 0,                # 0 ~ 100
            "message": "대기 중",          # 상태 메시지
            "result": None,               # 결과 데이터
            "error": None,
            "logs": [],                   # 로그 저장
            "started_at": time.time(),    # 생성 시각
            "finished_at": None,          # 완료 시각
        }

# Job 상태 업데이트 함수
def update_job(job_id, **kwargs):  # Job 수정
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)    # kwargs로 받은 값들을 기존 job dict에 덮어씀
        else:   # 존재하지 않는 job_id일 경우
            print(f"[JOB_STORE] update_job failed: unknown job_id={job_id}")

# Job 로그 추가 함수
def append_job_log(job_id, line):  # Job 로그 추가
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["logs"].append(line)
            _jobs[job_id]["logs"] = _jobs[job_id]["logs"][-100:]             # 로그가 너무 많아지는 걸 방지 → 최근 100개만 유지 (메모리 보호)
        else:
            print(f"[JOB_STORE] append_job_log failed: unknown job_id={job_id}")

# Job 조회 함수
def get_job(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        return dict(job)    # 복사본(dict(job))을 반환
    
# 전체 Job 조회 함수
def get_all_jobs():
    with _jobs_lock:
        return {job_id: dict(job) for job_id, job in _jobs.items()}