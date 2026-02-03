// ========== D3.js 네트워크 차트 렌더링 ==========

function renderNetworkChart(data) {
  console.log('🎨 렌더링 시작:', data);
  
  const chartElement = document.getElementById('graph-container');
  if (!chartElement) {
    console.error("❌ graph-container를 찾을 수 없습니다");
    return;
  }

  // 데이터 검증
  if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
    console.error("❌ 잘못된 데이터 형식:", data);
    return;
  }

  if (data.nodes.length === 0) {
    console.warn("⚠️ 노드가 없습니다");
    return;
  }

  // SVG 크기
  const svg = d3.select(chartElement);
  const width = chartElement.clientWidth || 928;
  const height = chartElement.clientHeight || 600;

  // 기존 내용 제거
  svg.selectAll("*").remove();

  // SVG 속성 설정
  svg
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .style("max-width", "100%")
    .style("height", "auto");

  // 색상 팔레트
  const colorScale = d3.scaleOrdinal()
    .domain(data.nodes.map(d => d.type || 'default'))
    .range(["#667eea", "#764ba2", "#f093fb", "#4facfe", "#43e97b", "#fa709a", "#fee140"]);

  // 노드 크기 계산
  const maxDegree = d3.max(data.nodes, d => d.degree || 1) || 10;
  const sizeScale = d3.scaleLinear()
    .domain([1, maxDegree])
    .range([15, 40]);

  // D3 Force Simulation
  const simulation = d3.forceSimulation(data.nodes)
    .force("link", d3.forceLink(data.edges)
      .id(d => d.id)
      .distance(120)
      .strength(0.3))
    .force("charge", d3.forceManyBody()
      .strength(-300)
      .distanceMin(30)
      .distanceMax(800))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide()
      .radius(d => sizeScale(d.degree) + 10))
    .force("x", d3.forceX(width / 2).strength(0.05))
    .force("y", d3.forceY(height / 2).strength(0.05))
    .alphaDecay(0.02)
    .alphaTarget(0.1);

  // 그래프 컨테이너 그룹
  const g = svg.append("g");

  // 줌 기능
  const zoom = d3.zoom()
    .scaleExtent([0.5, 8])
    .on("zoom", (event) => {
      g.attr("transform", event.transform);
    });
  svg.call(zoom);

  // 링크 그리기
  const links = g.append("g")
    .attr("stroke", "#999")
    .attr("stroke-opacity", 0.5)
    .selectAll("line")
    .data(data.edges)
    .join("line")
    .attr("stroke-width", d => Math.sqrt(d.weight || 1) * 2)
    .attr("class", "link");

  // 링크 라벨
  const linkLabels = g.append("g")
    .selectAll("text")
    .data(data.edges)
    .join("text")
    .attr("font-size", "10px")
    .attr("fill", "#999")
    .attr("text-anchor", "middle")
    .attr("dy", "-5px")
    .attr("pointer-events", "none")
    .text(d => d.relationship || "");

  // 노드 그룹
  const nodes = g.append("g")
    .selectAll("g")
    .data(data.nodes)
    .join("g")
    .attr("class", "node")
    .call(drag(simulation));

  // 노드 원
  nodes.append("circle")
    .attr("r", d => sizeScale(d.degree || 1))
    .attr("fill", d => colorScale(d.type))
    .attr("stroke", "white")
    .attr("stroke-width", 2)
    .style("cursor", "pointer");

  // 노드 라벨
  nodes.append("text")
    .attr("text-anchor", "middle")
    .attr("dy", "0.3em")
    .attr("font-size", "11px")
    .attr("font-weight", "bold")
    .attr("fill", "white")
    .attr("pointer-events", "none")
    .text(d => {
      // 긴 텍스트는 줄임
      const text = d.id || "";
      return text.length > 10 ? text.substring(0, 10) + "..." : text;
    });

  // 툴팁
  const tooltip = d3.select("body").append("div")
    .attr("class", "d3-tooltip")
    .style("visibility", "hidden");

  nodes.on("mouseover", function(event, d) {
    tooltip.style("visibility", "visible")
      .html(`
        <strong>${d.id}</strong><br/>
        <small>${d.description || "엔티티"}</small><br/>
        <small>연결: ${d.degree || 0}</small>
      `)
      .style("top", (event.pageY + 10) + "px")
      .style("left", (event.pageX + 10) + "px");
  })
  .on("mousemove", function(event) {
    tooltip
      .style("top", (event.pageY + 10) + "px")
      .style("left", (event.pageX + 10) + "px");
  })
  .on("mouseout", function() {
    tooltip.style("visibility", "hidden");
  });

  // 시뮬레이션 업데이트
  simulation.on("tick", () => {
    links
      .attr("x1", d => d.source.x)
      .attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x)
      .attr("y2", d => d.target.y);

    linkLabels
      .attr("x", d => (d.source.x + d.target.x) / 2)
      .attr("y", d => (d.source.y + d.target.y) / 2);

    nodes.attr("transform", d => `translate(${d.x},${d.y})`);
  });

  // 통계 정보 표시
  document.getElementById('graphInfo').style.display = 'block';
  document.getElementById('nodeCount').textContent = `노드: ${data.nodes.length}`;
  document.getElementById('edgeCount').textContent = `엣지: ${data.edges.length}`;

  console.log(`✅ 렌더링 완료: ${data.nodes.length} 노드, ${data.edges.length} 엣지`);
}

// 드래그 기능
function drag(simulation) {
  function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }

  return d3.drag()
    .on("start", dragstarted)
    .on("drag", dragged)
    .on("end", dragended);
}
