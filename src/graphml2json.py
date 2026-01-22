import networkx as nx
import json

# .graphml 파일을 읽어들입니다. # 파일 경로 수정 필요
graph = nx.read_graphml("./graphml/clustered_graph.graphml")

# 그래프의 노드와 엣지 정보를 딕셔너리로 변환합니다.
graph_data = {
    "nodes": [],
    "edges": []
}

# 노드 정보 추가
for node, attributes in graph.nodes(data=True):
    node_data = {
        "id": node,
        "level": attributes.get("level", None),
        "human_readable_id": attributes.get("human_readable_id", None),
        "source_id": attributes.get("source_id", None),
        "description": attributes.get("description", None),
        "weight": attributes.get("weight", None),
        "cluster": attributes.get("cluster", None),
        "entity_type": attributes.get("entity_type", None),
        "degree": attributes.get("degree", None),
        "type": attributes.get("type", None)
    }
    graph_data["nodes"].append(node_data)

# 엣지 정보 추가
for source, target, attributes in graph.edges(data=True):
    edge_data = {
        "source": source,
        "target": target,
        "level": attributes.get("level", None),
        "human_readable_id": attributes.get("human_readable_id", None),
        "id": attributes.get("id", None),
        "source_id": attributes.get("source_id", None),
        "description": attributes.get("description", None),
        "weight": attributes.get("weight", None)
    }
    graph_data["edges"].append(edge_data)

# JSON 파일로 저장 # 파일 경로 수정 필요
with open("./json/graphml_data.json", "w") as f:
    json.dump(graph_data, f, indent=4)

print("GraphML 파일이 JSON 형식으로 변환되었습니다.")