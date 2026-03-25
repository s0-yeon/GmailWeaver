# check_entities.py
# 현재 GraphRAG 인덱싱 결과 상태 점검 (재인덱싱 필요 여부 판단용)

import os
import sys
import pandas as pd

PARQUET_DIR = "src/parquet/output"

def load(filename):
    path = os.path.join(PARQUET_DIR, filename)
    if not os.path.exists(path):
        print(f"[없음] {path}")
        return None
    return pd.read_parquet(path)


def check_entities():
    df = load("entities.parquet")
    if df is None: return

    print(f"\n{'='*60}")
    print(f"[ENTITIES] 총 {len(df)}개")
    print(f"{'='*60}")
    print(f"컬럼: {list(df.columns)}\n")

    # type 컬럼이 있으면 타입별 분포 출력
    type_col = next((c for c in df.columns if 'type' in c.lower()), None)
    if type_col:
        print("[타입별 분포]")
        print(df[type_col].value_counts().to_string())
    
    # 이메일 관련 엔티티 있는지 확인
    title_col = next((c for c in df.columns if 'title' in c.lower() or 'name' in c.lower()), None)
    if title_col:
        print(f"\n[전체 엔티티 목록 - '{title_col}' 기준]")
        for i, row in df.iterrows():
            t = str(row.get(type_col, ''))
            n = str(row.get(title_col, ''))
            d = str(row.get('description', ''))[:60]
            print(f"  [{i:3d}] {t:<20} | {n:<30} | {d}")


def check_text_units():
    df = load("text_units.parquet")
    if df is None: return

    print(f"\n{'='*60}")
    print(f"[TEXT UNITS] 총 {len(df)}개")
    print(f"{'='*60}")
    print(f"컬럼: {list(df.columns)}\n")

    # 첫 3개 텍스트 미리보기
    text_col = next((c for c in df.columns if 'text' in c.lower()), None)
    if text_col:
        print("[텍스트 미리보기 - 첫 3개]")
        for i, row in df.head(3).iterrows():
            print(f"\n--- TextUnit [{i}] ---")
            print(str(row[text_col])[:300])


def check_relationships():
    df = load("relationships.parquet")
    if df is None: return

    print(f"\n{'='*60}")
    print(f"[RELATIONSHIPS] 총 {len(df)}개")
    print(f"{'='*60}")
    print(f"컬럼: {list(df.columns)}\n")

    src = next((c for c in df.columns if 'source' in c.lower()), None)
    tgt = next((c for c in df.columns if 'target' in c.lower()), None)
    desc = next((c for c in df.columns if 'desc' in c.lower()), None)

    if src and tgt:
        print("[관계 목록 - 처음 20개]")
        for i, row in df.head(20).iterrows():
            s = str(row.get(src, ''))[:25]
            t = str(row.get(tgt, ''))[:25]
            d = str(row.get(desc, ''))[:50] if desc else ''
            print(f"  [{i:3d}] {s:<25} → {t:<25} | {d}")


def check_communities():
    df = load("communities.parquet")
    if df is None: return

    print(f"\n{'='*60}")
    print(f"[COMMUNITIES] 총 {len(df)}개")
    print(f"{'='*60}")

    title_col = next((c for c in df.columns if 'title' in c.lower()), None)
    size_col  = next((c for c in df.columns if 'size' in c.lower()), None)

    if title_col:
        print("[커뮤니티 목록]")
        for i, row in df.iterrows():
            t = str(row.get(title_col, ''))[:40]
            s = row.get(size_col, '?')
            print(f"  [{i:2d}] size={s:<4} | {t}")


def summary():
    print(f"\n{'='*60}")
    print("  [요약] EMAIL 관련 엔티티 체크")
    print(f"{'='*60}")

    df = load("entities.parquet")
    if df is None: return

    type_col  = next((c for c in df.columns if 'type' in c.lower()), None)
    title_col = next((c for c in df.columns if 'title' in c.lower() or 'name' in c.lower()), None)

    email_types = {"EMAIL_SENDER", "EMAIL_RECIPIENT", "EMAIL_SUBJECT",
                   "DEADLINE", "EVENT", "DATE", "TOPIC", "ATTACHMENT"}

    if type_col:
        found_types = set(df[type_col].dropna().unique())
        matched = email_types & found_types
        missing = email_types - found_types

        print(f"  존재하는 이메일 타입: {matched if matched else '없음 ❌'}")
        print(f"  누락된 이메일 타입:   {missing if missing else '없음 ✅'}")

        if not matched:
            print("\n  → 프롬프트 수정 후 graphrag index 재실행 필요!")
        else:
            print("\n  → 일부 이메일 엔티티 존재. 부분 재인덱싱 고려.")
    else:
        print("  type 컬럼 없음 — 컬럼명 확인 필요")


if __name__ == "__main__":
    print("GmailWeaver — GraphRAG 인덱스 상태 점검")
    print("실행 위치:", os.getcwd())

    check_entities()
    check_text_units()
    check_relationships()
    check_communities()
    summary()
