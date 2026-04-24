# src/util/graphrag_engine.py
# GraphRAG LocalSearch 엔진 유저별로 메모리에 캐싱해서 재사용하는 모듈

import os

import tiktoken
from pathlib import Path
import openai
import threading
_cache_lock = threading.Lock()
from graphrag.language_model.protocol.base import EmbeddingModel
from graphrag.config.load_config import load_config
from graphrag.query.context_builder.entity_extraction import EntityVectorStoreKey
from graphrag.query.indexer_adapters import (
    read_indexer_entities,       # parquet → Entity 객체 리스트로 변환
    read_indexer_relationships,  # parquet → Relationship 객체 리스트로 변환
    read_indexer_reports,        # parquet → CommunityReport 객체 리스트로 변환
    read_indexer_text_units,     # parquet → TextUnit 객체 리스트로 변환
)
from graphrag.query.structured_search.local_search.mixed_context import LocalSearchMixedContext  # 로컬 서치 컨텍스트 빌더
from graphrag.query.structured_search.local_search.search import LocalSearch   # 실제 로컬 서치 엔진
from graphrag.vector_stores.lancedb import LanceDBVectorStore                  # 임베딩 저장용 로컬 벡터 DB
from graphrag.language_model.protocol.base import ChatModel
from collections.abc import AsyncGenerator

class DirectOpenAIChatModel(ChatModel):
    """
    fnllm 우회용 Chat 모델.
    fnllm의 achat_stream()도 내부적으로 루프 충돌 발생.
    openai 직접 호출로 우회.
    """
    def __init__(self, api_key: str, model: str):
        self._client = openai.AsyncOpenAI(api_key=api_key)
        self._model = model

    async def achat_stream(
        self, prompt: str, history=None, model_parameters=None, **kwargs
    ) -> AsyncGenerator[str, None]:
        messages = list(history or [])
        messages.append({"role": "user", "content": prompt})
        params = model_parameters or {}

        stream = await self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            stream=True,
            **params
        )
        async for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta

class DirectOpenAIEmbedder(EmbeddingModel):
    """
    fnllm 우회용 임베더.
    fnllm의 embed()는 내부적으로 run_coroutine_sync를 써서
    이미 실행 중인 이벤트 루프와 충돌함.
    openai를 직접 동기 호출해서 이 문제를 우회함.
    """
    def __init__(self, api_key: str, model: str):
        self._client = openai.OpenAI(api_key=api_key)
        self._model = model

    def embed(self, text: str, **kwargs) -> list[float]:
        response = self._client.embeddings.create(
            input=text,
            model=self._model
        )
        return response.data[0].embedding

# 유저별 엔진 캐시 (유저마다 별도의 graphrag 인덱스 가지고 있어서 gmail_id를 키로 해서 캐시 가짐. 구조: { gmail_id: { "engine": LocalSearch객체, "mtime": float } })
_engine_cache: dict = {}

# entities.parquet 마지막 수정시간 (전체 갱신이나 update 하면 entities.parquet 파일이 수정되니 캐시된 mtime과 비교해서 인덱싱 갱신 여부 감지)
def _get_output_mtime(output_dir: str) -> float:
    p = os.path.join(output_dir, "entities.parquet")
    return os.path.getmtime(p) if os.path.exists(p) else 0.0

#LocalSearch엔진 처음부터 빌드. (유저별 첫 요청이나 인덱싱 갱신시에만 호출)
def _build_engine(output_dir: str, graphrag_root: str) -> LocalSearch:
    import pandas as pd
    lancedb_uri = os.path.join(output_dir, "lancedb")
    #setting.yaml에서 설정 가져옴
    config = load_config(Path(graphrag_root))

    # settings.yaml의 models.default_chat_model (gpt-4o-mini)
    llm_config = config.models["default_chat_model"]
    # settings.yaml의 models.default_embedding_model (text=embedding-3-small)
    emb_config = config.models["default_embedding_model"]
    # settings.yaml의 local_search
    ls_config = config.local_search

    # LLM: 최종 답변 생성용
    model = DirectOpenAIChatModel(
        api_key=os.environ["GRAPHRAG_API_KEY"],
        model=llm_config.model  # gpt-4o-mini
    )

    # 임베딩 모델 초기화 (쿼리를 벡터로 변환해서 유사 엔티티 검색에 사용함)
    api_key = os.environ["GRAPHRAG_API_KEY"]
    text_embedder = DirectOpenAIEmbedder(
        api_key=api_key,
        model=emb_config.model
    )

    # 토크나이저: settings.yaml의 encoding_model = o200k_base
    token_encoder = tiktoken.get_encoding(llm_config.encoding_model)

    # graphrag가 인덱싱 완료 후 output/ 폴더에 생성한 parquet 파일들을 DataFrame으로 로드
    entity_df    = pd.read_parquet(os.path.join(output_dir, "entities.parquet"))
    community_df = pd.read_parquet(os.path.join(output_dir, "communities.parquet"))
    relation_df  = pd.read_parquet(os.path.join(output_dir, "relationships.parquet"))
    report_df    = pd.read_parquet(os.path.join(output_dir, "community_reports.parquet"))
    text_unit_df = pd.read_parquet(os.path.join(output_dir, "text_units.parquet"))

    # DataFrame → graphrag 내부 객체로 변환. read_indexer_* 함수들이 DataFrame을 graphrag가 이해하는 데이터 클래스로 변환해줌
    entities      = read_indexer_entities(entity_df, community_df, community_level=2)
    relationships = read_indexer_relationships(relation_df)
    reports       = read_indexer_reports(report_df, community_df, community_level=2)
    text_units    = read_indexer_text_units(text_unit_df)

    # 벡터스토어 연결 및 엔티티 임베딩 로드 (graphrag 인덱싱 시 생성된 lancedb에 저장된 엔티티 임베딩을 불러옴)
    description_embedding_store = LanceDBVectorStore(
        collection_name="default-entity-description"
    )
    description_embedding_store.connect(db_uri=lancedb_uri)

    # 컨텍스트 빌더 생성 (LLM에 넘길 컨텍스트를 조립하는 역할)
    # settings.yaml의 local_search.embedding_vectorstore_key = EntityVectorStoreKey.ID
    context_builder = LocalSearchMixedContext(
        entities=entities,
        entity_text_embeddings=description_embedding_store,
        text_embedder=text_embedder,
        text_units=text_units,
        community_reports=reports,
        relationships=relationships,
        covariates=None,
        token_encoder=token_encoder,
        embedding_vectorstore_key=EntityVectorStoreKey.ID,
    )

    # LocalSearch 엔진 생성 (이 객체가 실제로 search(query)를 받아서 LLM 응답을 생성하는 핵심 객체) 재사용 가능.
    return LocalSearch(
        model=model,
        context_builder=context_builder,
        token_encoder=token_encoder,
        model_params={
            "max_tokens": ls_config.llm_max_tokens,  # 2000
            "temperature": ls_config.temperature,    # 0
        },
        context_builder_params={
            "text_unit_prop": ls_config.text_unit_prop,           # 0.5
            "community_prop": ls_config.community_prop,           # 0.1
            "top_k_mapped_entities": ls_config.top_k_entities,    # 10
            "top_k_relationships": ls_config.top_k_relationships, # 30
            "max_tokens": ls_config.max_tokens,                   # 12000
        },
        response_type="multiple paragraphs",
    )

# 유저별 캐시된 엔진 반환
def get_engine(gmail_id: str, output_dir: str, graphrag_root: str) -> LocalSearch:
    mtime = _get_output_mtime(output_dir)

    # 인덱스가 아직 생성되지 않은 상태._is_index_ready()로 이미 걸러지지만 방어적으로 한 번 더 체크
    if mtime == 0.0:
        raise RuntimeError(f"인덱스가 아직 생성되지 않았습니다: {output_dir}")

    with _cache_lock:  # 동시 접근 방지
        cached = _engine_cache.get(gmail_id)

        if cached and cached["mtime"] == mtime:
            return cached["engine"]  # 캐시 hit: 재사용

        # 캐시 miss 또는 인덱스 갱신 감지 (index/update 실행 후 mtime 변경): 새로 빌드
        print(f"[ENGINE] 빌드 시작: {gmail_id}")
        engine = _build_engine(output_dir, graphrag_root)
        _engine_cache[gmail_id] = {"engine": engine, "mtime": mtime}
        print(f"[ENGINE] 빌드 완료: {gmail_id}")
        return engine
