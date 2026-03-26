import re
# [?]이거 쓸지 안쓸지 모름[?]
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