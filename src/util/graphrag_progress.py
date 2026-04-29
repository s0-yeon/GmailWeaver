import re
import os

# 그래프라그 인덱싱 진행도를 표기하기 위한 현재 진행도 리턴 함수
def parse_graphrag_progress(line, current_progress):
    text = line.strip()

    stage_map = [
        ("create_base_text_units", 10, "텍스트 유닛 생성 중"),
        ("create_final_documents", 20, "최종 문서 생성 중"),
        ("extract_graph", 35, "그래프 추출 중"),
        ("finalize_graph", 45, "그래프 마무리 중"),
        ("create_communities", 60, "커뮤니티 생성 중"),
        ("create_final_text_units", 70, "최종 텍스트 유닛 정리 중"),
        ("create_community_reports", 80, "커뮤니티 리포트 생성 중"),
    ]

    for keyword, prog, msg in stage_map:
        if keyword in text:
            return max(current_progress, prog), msg

    m = re.search(r"generate_text_embeddings.*?(\d+)%", text)
    if m:
        emb = int(m.group(1))
        mapped = 80 + int(emb * 19 / 100)   # 80~99 구간으로 매핑
        return max(current_progress, mapped), f"임베딩 생성 중 ({emb}%)"

    return current_progress, None


# output 디렉토리를 스캔해서 현재 인덱싱 단계를 반환
# start_time: 인덱싱 시작 시각 (time.time()) — 이 시각 이후 생성·수정된 파일만 현재 실행 산출물로 인정
# GraphRAG는 단계별 폴더가 아니라 output/ 바로 아래에 parquet 파일을 순서대로 생성함
def get_stage_progress(output_dir, start_time):
    stage_map = [
        ("text_units.parquet",       35, "텍스트 유닛 생성 완료"),
        ("documents.parquet",        45, "문서 처리 완료"),
        ("entities.parquet",         55, "엔티티 추출 완료"),
        ("relationships.parquet",    65, "관계 추출 완료"),
        ("communities.parquet",      72, "커뮤니티 생성 완료"),
        ("community_reports.parquet",82, "커뮤니티 리포트 생성 완료"),
    ]

    if not os.path.isdir(output_dir):
        return 0, None

    try:
        entries = os.listdir(output_dir)
    except OSError:
        return 0, None

    best_progress = 0
    best_msg = None

    for filename, prog, msg in stage_map:
        if filename in entries:
            file_path = os.path.join(output_dir, filename)
            try:
                if os.path.getmtime(file_path) > start_time and prog > best_progress:
                    best_progress = prog
                    best_msg = msg
            except OSError:
                pass

    return best_progress, best_msg