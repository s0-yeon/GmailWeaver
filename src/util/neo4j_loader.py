import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv("src/parquet/.env")

NEO4J_URI      = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

OUTPUT_DIR = "src/parquet/output"

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def _setup_constraints(session):
    queries = [
        "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT community_id IF NOT EXISTS FOR (n:Community) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT report_id IF NOT EXISTS FOR (n:CommunityReport) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT textunit_id IF NOT EXISTS FOR (n:TextUnit) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (n:Document) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX entity_title IF NOT EXISTS FOR (n:Entity) ON (n.title)",
        "CREATE INDEX entity_type IF NOT EXISTS FOR (n:Entity) ON (n.type)",
        "CREATE INDEX community_level IF NOT EXISTS FOR (n:Community) ON (n.level)",
    ]
    for q in queries:
        try:
            session.run(q)
        except Exception:
            pass

def _clean(val):
    import math
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, list):
        return [str(v) for v in val if v is not None]
    return val

def _row_to_props(row, cols):
    return {c: _clean(row.get(c)) for c in cols if _clean(row.get(c)) is not None}

def _load_entities(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "entities.parquet"))
    cols = ["id", "title", "type", "description", "human_readable_id", "degree", "frequency"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        session.run("MERGE (e:Entity {id: $id}) SET e += $props", id=props["id"], props=props)
        count += 1
    print(f"[Neo4j] Entity {count}개 적재 완료")
    return count

def _load_relationships(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "relationships.parquet"))
    cols = ["id", "description", "weight", "human_readable_id"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        source = str(row.get("source", ""))
        target = str(row.get("target", ""))
        if not source or not target:
            continue
        session.run(
            """
            MATCH (s:Entity {title: $source})
            MATCH (t:Entity {title: $target})
            MERGE (s)-[r:RELATES_TO {id: $id}]->(t)
            SET r += $props
            """,
            source=source, target=target,
            id=props.get("id", f"{source}_{target}"), props=props
        )
        count += 1
    print(f"[Neo4j] Relationship {count}개 적재 완료")
    return count

def _load_communities(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "communities.parquet"))
    cols = ["id", "title", "level", "human_readable_id", "degree"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        session.run("MERGE (c:Community {id: $id}) SET c += $props", id=props["id"], props=props)
        count += 1
    print(f"[Neo4j] Community {count}개 적재 완료")
    return count

def _load_community_reports(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "community_reports.parquet"))
    cols = ["id", "title", "summary", "level", "rank", "human_readable_id"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        community_id = str(row.get("community", ""))
        session.run("MERGE (r:CommunityReport {id: $id}) SET r += $props", id=props["id"], props=props)
        if community_id:
            session.run(
                """
                MATCH (c:Community {id: $cid})
                MATCH (r:CommunityReport {id: $rid})
                MERGE (c)-[:HAS_REPORT]->(r)
                """,
                cid=community_id, rid=props["id"]
            )
        count += 1
    print(f"[Neo4j] CommunityReport {count}개 적재 완료")
    return count

def _load_text_units(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "text_units.parquet"))
    cols = ["id", "text", "n_tokens", "human_readable_id"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        session.run("MERGE (t:TextUnit {id: $id}) SET t += $props", id=props["id"], props=props)
        count += 1
    print(f"[Neo4j] TextUnit {count}개 적재 완료")
    return count

def _load_documents(session):
    df = pd.read_parquet(os.path.join(OUTPUT_DIR, "documents.parquet"))
    cols = ["id", "title", "human_readable_id"]
    count = 0
    for _, row in df.iterrows():
        props = _row_to_props(row, cols)
        session.run("MERGE (d:Document {id: $id}) SET d += $props", id=props["id"], props=props)
        count += 1
    print(f"[Neo4j] Document {count}개 적재 완료")
    return count

def load_all_to_neo4j(clear=True):
    driver = get_driver()
    result = {}
    with driver.session() as session:
        _setup_constraints(session)
        print("[Neo4j] 인덱스/제약 설정 완료")
        if clear:
            session.run("MATCH (n) DETACH DELETE n")
            print("[Neo4j] 기존 데이터 삭제 완료")
        result["entities"]          = _load_entities(session)
        result["relationships"]     = _load_relationships(session)
        result["communities"]       = _load_communities(session)
        result["community_reports"] = _load_community_reports(session)
        result["text_units"]        = _load_text_units(session)
        result["documents"]         = _load_documents(session)
    driver.close()
    print(f"\n[Neo4j] 전체 적재 완료: {result}")
    return result

if __name__ == "__main__":
    load_all_to_neo4j()
