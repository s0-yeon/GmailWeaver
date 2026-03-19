"""
src/util/neo4j_query.py
Production-grade GraphRAG: Neo4j + LanceDB Hybrid (CLI 완전 제거)
Microsoft GraphRAG LocalSearch 로직을 Neo4j 기반으로 재구현
Ref: https://microsoft.github.io/graphrag/query/local_search/
Ref: https://neo4j.com/docs/neo4j-graphrag-python/
"""
import os
import hashlib
import functools
import openai
import lancedb
import tiktoken
from concurrent.futures import ThreadPoolExecutor, as_completed
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv("src/parquet/.env")

# ── 설정 ───────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
LANCEDB_PATH   = os.getenv("GRAPHRAG_LANCEDB_URI",   "src/parquet/output/lancedb")
EMBED_MODEL    = os.getenv("GRAPHRAG_EMBEDDING_MODEL","text-embedding-3-small")
LLM_MODEL      = os.getenv("GRAPHRAG_LLM_MODEL",     "gpt-4o-mini")
API_KEY        = os.getenv("GRAPHRAG_API_KEY", "")

# 토큰 예산 (Microsoft GraphRAG 기본값 참조)
TOKEN_BUDGET       = 12_000   # GPT 컨텍스트 최대 토큰
ENTITY_TOP_K       = 10       # LanceDB 엔티티 검색 수
CHUNK_TOP_K        = 6        # LanceDB 청크 검색 수
COMMUNITY_TOP_K    = 3        # 커뮤니티 리포트 수
HOP2_LIMIT         = 5        # 2-hop 이웃 수

# ── 싱글턴 드라이버 (커넥션 풀링) ──────────────────────────────
_driver = None

def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            max_connection_pool_size=10,
        )
    return _driver


# ── 임베딩 캐시 (동일 쿼리 재호출 방지) ───────────────────────
_embed_cache = {}

def _embed(text):
    key = hashlib.md5(text.encode()).hexdigest()
    if key in _embed_cache:
        print("[Cache] 임베딩 캐시 히트")
        return _embed_cache[key]
    client = openai.OpenAI(api_key=API_KEY)
    res = client.embeddings.create(model=EMBED_MODEL, input=text)
    vec = res.data[0].embedding
    _embed_cache[key] = vec
    return vec


# ── 토큰 카운터 ────────────────────────────────────────────────
def _count_tokens(text):
    try:
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(str(text)))
    except Exception:
        return len(str(text)) // 4


# ── LanceDB 병렬 검색 ──────────────────────────────────────────
def _lancedb_search(table_keyword, query_vector, limit):
    try:
        db = lancedb.connect(LANCEDB_PATH)
        names = db.table_names()
        target = next((n for n in names if table_keyword in n.lower()), None)
        if not target:
            print("[LanceDB] 테이블 없음: " + table_keyword)
            return []
        return db.open_table(target).search(query_vector).limit(limit).to_list()
    except Exception as e:
        print("[LanceDB] 오류(" + table_keyword + "): " + str(e))
        return []


# ── Neo4j 배치 그래프 탐색 (2-hop + 관계 유형) ─────────────────
def _neo4j_graph_batch(entity_titles):
    if not entity_titles:
        return []
    try:
        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (e:Entity)
                WHERE e.title IN $titles
                OPTIONAL MATCH (e)-[r1]-(n1:Entity)
                OPTIONAL MATCH (n1)-[r2]-(n2:Entity)
                WHERE NOT n2.title IN $titles
                RETURN
                    e.title        AS title,
                    e.type         AS type,
                    e.description  AS description,
                    e.degree       AS degree,
                    collect(DISTINCT {
                        title:       n1.title,
                        type:        n1.type,
                        description: n1.description,
                        rel:         type(r1)
                    }) AS hop1,
                    collect(DISTINCT {
                        title: n2.title,
                        type:  n2.type
                    }) AS hop2
                ORDER BY e.degree DESC
                """,
                titles=entity_titles
            )
            return [dict(r) for r in result]
    except Exception as e:
        print("[Neo4j] 배치 그래프 오류: " + str(e))
        return []


# ── Neo4j 커뮤니티 리포트 ──────────────────────────────────────
def _neo4j_community_reports(entity_titles, limit=COMMUNITY_TOP_K):
    try:
        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (e:Entity)-[:IN_COMMUNITY]->(c:CommunityReport)
                WHERE e.title IN $titles
                RETURN DISTINCT c.title AS title, c.summary AS summary, c.rank AS rank
                ORDER BY rank DESC
                LIMIT $limit
                """,
                titles=entity_titles, limit=limit
            )
            rows = [dict(r) for r in result]
            if rows:
                return rows
            # 폴백: 연결 없어도 상위 리포트 반환
            result = session.run(
                """
                MATCH (c:CommunityReport)
                RETURN c.title AS title, c.summary AS summary, c.rank AS rank
                ORDER BY rank DESC
                LIMIT $limit
                """,
                limit=limit
            )
            return [dict(r) for r in result]
    except Exception as e:
        print("[Neo4j] 커뮤니티 리포트 오류: " + str(e))
        return []


# ── 컨텍스트 조립 (토큰 예산 관리) ────────────────────────────
def _build_context(entities, reports, chunks, budget=TOKEN_BUDGET):
    parts = []
    used = 0

    # 1. 커뮤니티 리포트 (전체 맥락 제공 - 최우선)
    if reports:
        header = "-----커뮤니티 요약-----\n"
        parts.append(header)
        used += _count_tokens(header)
        for rep in reports:
            line = "[" + str(rep.get("title","")) + "]\n" + str(rep.get("summary","")) + "\n\n"
            tok = _count_tokens(line)
            if used + tok > budget * 0.35:
                break
            parts.append(line)
            used += tok

    # 2. 엔티티 + 관계 (핵심 지식)
    if entities:
        header = "-----엔티티 및 관계-----\n"
        parts.append(header)
        used += _count_tokens(header)
        for ent in entities:
            title = str(ent.get("title",""))
            etype = str(ent.get("type",""))
            desc  = str(ent.get("description",""))
            hop1  = ent.get("hop1", []) or []
            hop2  = ent.get("hop2", []) or []

            block = "[엔티티] " + title + " (" + etype + ")\n"
            block += "  설명: " + desc + "\n"

            # 1-hop: 관계 유형 포함
            valid_hop1 = [n for n in hop1 if n.get("title")]
            if valid_hop1:
                block += "  직접 연관:\n"
                for n in valid_hop1[:4]:
                    rel  = n.get("rel", "RELATED") or "RELATED"
                    ndesc = (n.get("description") or "")[:100]
                    block += "    -[" + rel + "]-> " + str(n["title"]) + ": " + ndesc + "\n"

            # 2-hop: 확장 컨텍스트
            valid_hop2 = [n["title"] for n in hop2 if n.get("title")]
            if valid_hop2:
                block += "  2차 연관: " + ", ".join(valid_hop2[:HOP2_LIMIT]) + "\n"

            block += "\n"
            tok = _count_tokens(block)
            if used + tok > budget * 0.75:
                break
            parts.append(block)
            used += tok

    # 3. 원문 텍스트 청크 (나머지 예산 사용)
    if chunks:
        header = "-----원문 발췌-----\n"
        parts.append(header)
        used += _count_tokens(header)
        for chunk in chunks:
            text = chunk.get("text") or chunk.get("content") or chunk.get("chunk") or ""
            if not text:
                continue
            line = text[:800] + "\n\n"
            tok = _count_tokens(line)
            if used + tok > budget:
                break
            parts.append(line)
            used += tok

    print("[Context] 사용 토큰: " + str(used) + " / " + str(budget))
    return "".join(parts)


# ── 메인 쿼리 함수 ─────────────────────────────────────────────
def run_neo4j_query(message):
    import time
    t0 = time.time()

    # Step 1: 임베딩 생성
    query_vector = _embed(message)
    print("[Step1] 임베딩: " + str(round(time.time()-t0, 2)) + "초")

    # Step 2: LanceDB 병렬 검색
    entity_hits = []
    chunk_hits  = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_ent   = pool.submit(_lancedb_search, "entity", query_vector, ENTITY_TOP_K)
        f_chunk = pool.submit(_lancedb_search, "text",   query_vector, CHUNK_TOP_K)
        entity_hits = f_ent.result()
        chunk_hits  = f_chunk.result()
    print("[Step2] LanceDB 검색: " + str(round(time.time()-t0, 2)) + "초 / 엔티티: " + str(len(entity_hits)) + "개, 청크: " + str(len(chunk_hits)) + "개")

    # Step 3: 엔티티 타이틀 추출 (LanceDB score 기준 정렬 유지)
    entity_titles = []
    for hit in entity_hits:
        text = hit.get("text") or ""
        # "타이틀:설명" 형식에서 타이틀만 추출
        t = text.split(":")[0].strip() if ":" in text else text.strip()
        if t and t not in entity_titles:
            entity_titles.append(t)


    # Step 4: Neo4j 그래프 탐색 + 커뮤니티 리포트 (병렬)
    graph_data = []
    reports    = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_graph  = pool.submit(_neo4j_graph_batch, entity_titles)
        f_report = pool.submit(_neo4j_community_reports, entity_titles)
        graph_data = f_graph.result()
        reports    = f_report.result()
    print("[Step4] Neo4j 조회: " + str(round(time.time()-t0, 2)) + "초 / 엔티티: " + str(len(graph_data)) + "개, 리포트: " + str(len(reports)) + "개")

    # Step 5: 컨텍스트 조립 (토큰 예산 관리)
    context = _build_context(graph_data, reports, chunk_hits)
    print("[Step5] 컨텍스트 조립: " + str(round(time.time()-t0, 2)) + "초")

    # Step 6: GPT 최종 답변
    client = openai.OpenAI(api_key=API_KEY)
    system_prompt = (
        "당신은 사용자의 이메일을 분석하는 AI 어시스턴트입니다.\n"
        "아래 컨텍스트는 사용자의 실제 이메일에서 추출한 엔티티, 관계, 요약 정보입니다.\n"
        "컨텍스트를 바탕으로 이메일 내용을 자연스럽게 한국어로 설명해주세요.\n"
        "엔티티 이름, 날짜, 조직, 사람 정보를 최대한 활용해서 구체적으로 답변하세요.\n"
        "예를 들어 '최근 이메일'을 물어보면 컨텍스트의 엔티티들을 이메일 내용으로 재구성해서 설명하세요.\n\n"
        "=== 지식 그래프 컨텍스트 ===\n"
        + context
    )
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system",  "content": system_prompt},
            {"role": "user",    "content": message}
        ],
        temperature=0.0,
    )
    answer = response.choices[0].message.content.strip()
    print("[Step6] 완료: " + str(round(time.time()-t0, 2)) + "초")
    return answer
