import threading
import time

# 메모리 기반 Job 저장소
# 서버 재시작 시 모든 Job 정보 소멸 (영속성 없음)
# 멀티 워커(gunicorn -w 2 이상) 환경에서는 워커 간 공유 불가 → 운영 시 Redis 등 외부 저장소로 대체 필요
# 현재 Job 청소(cleanup) 로직 없음. 장시간 운영 시 메모리 누수 가능성
_jobs = {}
# 여려 쓰레드가 동시에 Job을 건드리지 않도록 함
_jobs_lock = threading.Lock()

# Job 이용 공통함수
# JOb들의 형식을 통일하기 위함
def create_job(job_id, job_type="index"): # Job 생성
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "job_type": job_type,     # index / query
            "status": "queued",       # queued / running / done / failed
            "progress": 0,            # 0 ~ 100
            "message": "대기 중",
            "result": None,
            "error": None,
            "logs": [],
            "started_at": time.time(),
            "finished_at": None,
        }

def update_job(job_id, **kwargs):  # Job 수정
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)
        else:
            print(f"[JOB_STORE] update_job failed: unknown job_id={job_id}")

def append_job_log(job_id, line):  # Job 로그 추가
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["logs"].append(line)
            _jobs[job_id]["logs"] = _jobs[job_id]["logs"][-100:]
        else:
            print(f"[JOB_STORE] append_job_log failed: unknown job_id={job_id}")

def get_job(job_id):  # Job 조회
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        return dict(job)