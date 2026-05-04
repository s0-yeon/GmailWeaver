# src/util/graphrag_engine.py
# GraphRAG LocalSearch 엔진 유저별로 메모리에 캐싱해서 재사용하는 모듈

import os
import tiktoken # 텍스트 토큰으로 변환하는 OpenAI 토크나이저
from pathlib import Path # 파일 경로를 객체로 다루기 위한 표준 라이브러리
import openai # OpenAI API 호출용
import threading
_cache_lock = threading.Lock() # 멀티스레드 환경에서 캐시 동시 접근하는 것을 방지하기 위한 락
from graphrag.language_model.protocol.base import EmbeddingModel # 임베딩 모델 인터페이스
from graphrag.config.load_config import load_config # settings.yaml 설정 로더
from graphrag.query.context_builder.entity_extraction import EntityVectorStoreKey
from graphrag.query.indexer_adapters import (
    read_indexer_entities,       # parquet → Entity 객체 리스트로 변환
    read_indexer_relationships,  # parquet → Relationship 객체 리스트로 변환
    read_indexer_reports,        # parquet → CommunityReport 객체 리스트로 변환
    read_indexer_text_units,     # parquet → TextUnit 객체 리스트로 변환
    read_indexer_communities,    # parquet → Community 객체 리스트로 변환
)
from graphrag.query.structured_search.local_search.mixed_context import LocalSearchMixedContext  # 로컬 서치 컨텍스트 빌더
from graphrag.query.structured_search.local_search.search import LocalSearch   # 실제 로컬 서치 엔진
from graphrag.vector_stores.lancedb import LanceDBVectorStore                  # 임베딩 저장용 로컬 벡터 DB
from graphrag.language_model.protocol.base import ChatModel
from collections.abc import AsyncGenerator
from graphrag.query.structured_search.global_search.community_context import GlobalCommunityContext
from graphrag.query.structured_search.global_search.search import GlobalSearch

class _ModelOutput:
    """ModelOutput 구현체: content 속성만 있으면 됨"""
    def __init__(self, content: str):
        self._content = content

    @property
    def content(self) -> str:
        return self._content


class _ModelResponse:
    """ModelResponse 구현체: GlobalSearch가 model_response.output.content로 접근함"""
    def __init__(self, content: str):
        self._output = _ModelOutput(content)

    @property
    def output(self) -> _ModelOutput:
        return self._output

    @property
    def parsed_response(self):
        return None

    @property
    def history(self) -> list:
        return []

# OpenAI API를 직접 호출하도록 만든 커스텀 클래스
class DirectOpenAIChatModel(ChatModel): 
    def __init__(self, api_key: str, model: str): 
        self._client = openai.AsyncOpenAI(api_key=api_key) # 비동기 OpenAI 클라이언트
        self._model = model # 사용할 모델명

    # GlobalSearch가 호출하는 메서드 (비스트리밍)
    async def achat(self, prompt: str, history=None, model_parameters=None, **kwargs):
        messages = list(history or [])
        messages.append({"role": "user", "content": prompt})
        params = model_parameters or {}

        # graphrag가 json=True를 넘기면 OpenAI JSON mode로 변환
        use_json = kwargs.get("json", False) or params.pop("json", False)
        if use_json:
            params["response_format"] = {"type": "json_object"}

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            stream=False,  # 스트리밍 아님
            **params
        )
        content = response.choices[0].message.content or ""
        return _ModelResponse(content)

    # graphrag 내부에서 LLM 응답을 스트리밍으로 받을 때 호출하는 메소드
    async def achat_stream(
        self, prompt: str, history=None, model_parameters=None, **kwargs
    ) -> AsyncGenerator[str, None]:
        messages = list(history or []) # 이전 대화 이력 
        messages.append({"role": "user", "content": prompt}) # 현재 질의 추가
        params = model_parameters or {} # 추가 파라미터 (temperature 등)

        # OpenAI 스트리밍 요청
        stream = await self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            stream=True,
            **params
        )
        # 토큰 단위로 청크를 받아서 내용이 있을 때만 yield
        async for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta

# OpenAI 임베딩 API를 직접 호출하도록 만든 커스텀 클래스
class DirectOpenAIEmbedder(EmbeddingModel):
    def __init__(self, api_key: str, model: str):
        self._client = openai.OpenAI(api_key=api_key) # 동기 OpenAI 클라이언트
        self._model = model # 사용할 임베딩 모델명

    def embed(self, text: str, **kwargs) -> list[float]: # 텍스트 한 건을 받아서 float 벡터로 변환해서 반환함
        response = self._client.embeddings.create(
            input=text,
            model=self._model
        )
        return response.data[0].embedding # 첫번째 결과의 임베딩 벡터 반환

# 유저별 엔진 캐시 (유저마다 별도의 graphrag 인덱스 가지고 있어서 gmail_id를 키로 해서 캐시 가짐. 구조: { gmail_id: { "local": LocalSearch객체, "global": GlobalSearch객체, "mtime": float } })
_engine_cache: dict = {}

# entities.parquet 마지막 수정시간 (전체 갱신이나 update 하면 entities.parquet 파일이 수정되니 캐시된 mtime과 비교해서 인덱싱 갱신 여부 감지)
def _get_output_mtime(output_dir: str) -> float:
    p = os.path.join(output_dir, "entities.parquet")
    return os.path.getmtime(p) if os.path.exists(p) else 0.0

#LocalSearch엔진 처음부터 빌드. (유저별 첫 요청이나 인덱싱 갱신시에만 호출)
def _build_local_engine(output_dir: str, graphrag_root: str) -> LocalSearch:
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
    text_embedder = DirectOpenAIEmbedder(
        api_key=os.environ["GRAPHRAG_API_KEY"],
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
    
    best_level = int(community_df['level'].value_counts().idxmax())
    # DataFrame → graphrag 내부 객체로 변환. read_indexer_* 함수들이 DataFrame을 graphrag가 이해하는 데이터 클래스로 변환해줌
    entities      = read_indexer_entities(entity_df, community_df, community_level=best_level)
    relationships = read_indexer_relationships(relation_df)
    reports       = read_indexer_reports(report_df, community_df, community_level=best_level)
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

    prompt_path = os.path.join(graphrag_root, "prompts", "local_search_system_prompt.txt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        system_prompt = f.read()

    # LocalSearch 엔진 생성 (이 객체가 실제로 search(query)를 받아서 LLM 응답을 생성하는 핵심 객체) 재사용 가능.
    return LocalSearch(
        model=model,
        context_builder=context_builder,
        token_encoder=token_encoder,
        system_prompt=system_prompt,
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

def _build_global_engine(output_dir: str, graphrag_root: str) -> GlobalSearch:
    import pandas as pd
    config = load_config(Path(graphrag_root))

    llm_config = config.models["default_chat_model"]
    gs_config  = config.global_search  # settings.yaml의 global_search 설정

    model = DirectOpenAIChatModel(
        api_key=os.environ["GRAPHRAG_API_KEY"],
        model=llm_config.model
    )
    token_encoder = tiktoken.get_encoding(llm_config.encoding_model)

    # LocalSearch와 달리 text_units, lancedb, 임베딩 모델 불필요 (커뮤니티 보고서와 엔티티만 필요함)
    entity_df    = pd.read_parquet(os.path.join(output_dir, "entities.parquet"))
    community_df = pd.read_parquet(os.path.join(output_dir, "communities.parquet"))
    report_df    = pd.read_parquet(os.path.join(output_dir, "community_reports.parquet"))

    # 글로벌 서치는 가장 낮은 레벨 선택 (전체 트랜드 파악하기 위함)
    best_level = 0 # int(community_df['level'].min())
    print(f"[ENGINE] global community_level 자동 선택: {best_level}")

    entities = read_indexer_entities(entity_df, community_df, community_level=best_level)
    reports  = read_indexer_reports(report_df, community_df, community_level=best_level)
    communities = read_indexer_communities(community_df, report_df)

    # GlobalCommunityContext: 커뮤니티 보고서를 계층적으로 조립해서 LLM에 넘기는 컨텍스트 빌더
    context_builder = GlobalCommunityContext(
        entities=entities,
        communities=communities,
        community_reports=reports,
        token_encoder=token_encoder,
    )

    prompts_dir = os.path.join(graphrag_root, "prompts")
    with open(os.path.join(prompts_dir, "global_search_map_system_prompt.txt"), "r", encoding="utf-8") as f:
        map_prompt = f.read()
    with open(os.path.join(prompts_dir, "global_search_reduce_system_prompt.txt"), "r", encoding="utf-8") as f:
        reduce_prompt = f.read()

    return GlobalSearch(
        model=model,
        context_builder=context_builder,
        token_encoder=token_encoder,
        map_system_prompt=map_prompt,         # 각 커뮤니티 보고서 개별 요약용
        reduce_system_prompt=reduce_prompt,   # 개별 요약 → 최종 답변 합산용
        json_mode=False,  # 추가: JSON 강제 파싱 비활성화
        map_llm_params={                          # map 단계 LLM 파라미터
            "max_tokens": gs_config.map_max_tokens,
            "temperature": gs_config.temperature,
        },
        reduce_llm_params={                       # reduce 단계 LLM 파라미터
            "max_tokens": gs_config.reduce_max_tokens,
            "temperature": gs_config.temperature,
        },
        context_builder_params={
            "max_tokens": gs_config.data_max_tokens, # 컨텐스트에 넣을 데이터 최대 토큰
            "community_level": best_level,
        },
    )

# 유저별 캐시된 엔진 반환
def get_engines(gmail_id: str, output_dir: str, graphrag_root: str) -> tuple[LocalSearch, GlobalSearch]:
    mtime = _get_output_mtime(output_dir)

    # 인덱스가 아직 생성되지 않은 상태._is_index_ready()로 이미 걸러지지만 방어적으로 한 번 더 체크
    if mtime == 0.0:
        raise RuntimeError(f"인덱스가 아직 생성되지 않았습니다: {output_dir}")

    with _cache_lock:  # 동시 접근 방지
        cached = _engine_cache.get(gmail_id)

        if cached and cached["mtime"] == mtime:
            return cached["local"], cached["global"]

        # 캐시 miss 또는 인덱스 갱신 감지 (index/update 실행 후 mtime 변경): 새로 빌드
        print(f"[ENGINE] 빌드 시작: {gmail_id}")
        local_engine  = _build_local_engine(output_dir, graphrag_root)
        global_engine = _build_global_engine(output_dir, graphrag_root)
        _engine_cache[gmail_id] = {
            "local":  local_engine,
            "global": global_engine,
            "mtime":  mtime
        }
        print(f"[ENGINE] 빌드 완료: {gmail_id}")
        return local_engine, global_engine
