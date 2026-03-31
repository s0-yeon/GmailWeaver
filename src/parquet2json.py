# src/parquet2json.py

import pandas as pd
import json
import argparse
import os
from config.settings import *
from util.user_path import UserPaths

# pandas에서 읽은 값을 JSON으로 저장 가능한 타입으로 변환
def _convert(val):
    try:
        if pd.isna(val): # 값이 비어있으면
            return None
    
    except Exception:
        pass
 
    if isinstance(val, float): # 실수이면
        return round(val, 6) # 소수점 6자리 끊어서 반올림
 
    if isinstance(val, (list, dict)): # 리스트나 딕셔너리이면
        return val
 
    return val

# 노드 생성
def _build_nodes(entities_df: pd.DataFrame, communities_df: pd.DataFrame | None) -> list[dict]:
 
    print(f"entities 컬럼: {list(entities_df.columns)}")

    # { entity_id : community_id } 형태의 딕셔너리
    community_map = {}
 
    if communities_df is not None: # 커뮤니티가 있다면
        print(f"communities 컬럼: {list(communities_df.columns)}")
        # entity_ids = 해당 커뮤니티에 속한 entity id들의 리스트
        if "entity_ids" in communities_df.columns:
            # iterrows() : 표의 각 행을 (인덱스, 행데이터) 튜플로 순회, _ : 인덱스는 사용 안 하므로 무시
            for _, row in communities_df.iterrows():
                cid = str(row.get("community", row.get("id", ""))) # 커뮤니티 id 추출
                eids = row["entity_ids"] # 해당 커뮤니티 속 엔티티 id
                if isinstance(eids, list):    
                    for eid in eids:   
                        # { entity_id : community_id } 형태로 저장        
                        community_map[str(eid)] = cid
 
    nodes = [] # 최종 노드 리스트

    # entities.parquet의 각 행(= 엔티티 하나)을 순회
    for _, row in entities_df.iterrows():

        # 엣지의 source/target이 title(이름)을 사용하므로 노드 ID도 title로 통일
        nid = str(row.get("title", row.get("name", row.get("id", _))))

        node = {
            "id":                nid,
            "label":             nid, # 그래프 노드 안에 표시될 이름
            "entity_type":       _convert(row.get("entity_type", row.get("type", None))), # 엔티티 종류
            "description":       _convert(row.get("description", None)), # 엔티티 설명
            "human_readable_id": _convert(row.get("human_readable_id", None)), # GraphRAG가 부여한 숫자 형태의 ID
            "source_id":         _convert(row.get("source_id", None)), # 이 엔티티가 어느 원본 문서에서 추출됐는지 추적용 ID
            "degree":            _convert(row.get("degree", None)), # 이 노드에 연결된 엣지 수
            "weight":            _convert(row.get("weight", None)), # 노드 중요도 가중치
            "cluster":           _convert(row.get("cluster", community_map.get(nid, None))), # 소속 커뮤니티 ID
            "level":             _convert(row.get("level", None)), # GraphRAG 계층 레벨    
        }
        nodes.append(node)
 
    print(f"노드 {len(nodes)}개 생성")

    return nodes
 
# 엣지 생성
def _build_edges(rel_df: pd.DataFrame) -> list[dict]:
 
    print(f"relationships 컬럼: {list(rel_df.columns)}") # rel_df : relationships.parquet을 pandas로 읽은 표
 
    src_col = next((c for c in ["source", "src", "source_id"] if c in rel_df.columns), rel_df.columns[0]) # 출발 노드 이름이 들어있는 컬럼명을 찾음
    tgt_col = next((c for c in ["target", "tgt", "target_id"] if c in rel_df.columns), rel_df.columns[1]) # 도착 노드 이름이 들어있는 컬럼명을 찾음
 
    edges = []
 
    # relationships.parquet의 각 행(= 관계 하나)을 순회
    for i, row in rel_df.iterrows(): 
        edge = {
            "source":            str(row[src_col]), # 엣지 출발 노드
            "target":            str(row[tgt_col]), # 엣지 도착 노드
            "id":                _convert(row.get("id", str(i))), # 엣지 고유 식별자
            "human_readable_id": _convert(row.get("human_readable_id", None)), # GraphRAG가 부여한 숫자 형태의 ID
            "description":       _convert(row.get("description", None)), # 엣지 설명문
            "weight":            _convert(row.get("weight", 1.0)), # 엣지 가중치
            "source_id":         _convert(row.get("source_id", None)), # 관계가 어느 원본 문서에서 추출됐는지 추적용 ID
            "level":             _convert(row.get("level", None)), # GraphRAG 계층 레벨
        }
        edges.append(edge)
 
    print(f"엣지 {len(edges)}개 생성")

    return edges
 
# job_run.py에서 subprocess로 이 파일을 실행하면 여기서 시작
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", required=True)
    parser.add_argument("--gmail-id", required=True)
    args = parser.parse_args()

    paths = UserPaths(args.base_dir, args.gmail_id)
    print("parquet → JSON 변환 시작\n")
 
    # 인덱싱이 안된 경우
    if not os.path.exists(paths.ENTITIES_PATH):
        print(f"[ERROR] 파일 없음: {paths.ENTITIES_PATH}")
        return
 
    if not os.path.exists(paths.RELATIONSHIPS_PATH):
        print(f"[ERROR] 파일 없음: {paths.RELATIONSHIPS_PATH}")
        return
 
    entities_df = pd.read_parquet(paths.ENTITIES_PATH) # entities.parquet 에서 pandas DataFrame으로 메모리 로드
    rel_df = pd.read_parquet(paths.RELATIONSHIPS_PATH) # relationships.parquet 에서 pandas DataFrame으로 메모리 로드
    # 커뮤니티는 없을수도 있으니까 None로 둠
    communities_df = None # 커뮤니티는 없을수도 있음
    if os.path.exists(paths.COMMUNITIES_PATH):
        communities_df = pd.read_parquet(paths.COMMUNITIES_PATH) # communities.parquet 에서 pandas DataFrame으로 메모리 로드
 
    # 노드/엣지 생성
    print("노드 생성 중...")
    nodes = _build_nodes(entities_df, communities_df)    # entities → 노드 리스트
 
    print("\n엣지 생성 중...")
    edges = _build_edges(rel_df)     # relationships → 엣지 리스트

    os.makedirs(os.path.dirname(paths.GRAPH_JSON_PATH), exist_ok=True) # src/json안에 저장
 
    graph_data = {
        "nodes": nodes,             # 전체 노드
        "edges": edges,             # 전체 엣지
    }

    with open(paths.GRAPH_JSON_PATH, "w", encoding="utf-8") as f: # 쓰기 모드로 한글 깨짐 방지
        json.dump(graph_data, f, ensure_ascii=False, indent=2)

    print(f"\n완료")
    print(f"저장 경로 : {paths.GRAPH_JSON_PATH}")
    print(f"노드 수   : {len(nodes)}")
    print(f"엣지 수   : {len(edges)}")
 
if __name__ == "__main__":
    main()