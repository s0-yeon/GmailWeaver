/**
 * graph-render.js — 공유 그래프 렌더링 함수
 *
 * 사용 방법 두 가지:
 *   1) graph_view.html  : <script src="/graph-render.js"> 후 window.renderGraph(svgEl, data) 호출
 *   2) graph-viz.js     : 동적 스크립트 로드 후 동일하게 호출 (CDN d3 필요)
 *
 * 의존성: window.d3 (CDN 또는 먼저 로드된 npm d3)
 */
(function (global) {
  const COLORS = {
    PERSON:       '#60a5fa',
    ORGANIZATION: '#34d399',
    GEO:          '#fbbf24',
    EVENT:        '#f87171',
    unknown:      '#c9d1d9',
  };

  /**
   * @param {SVGElement} svgEl  - 그래프를 그릴 SVG 요소 (#graph)
   * @param {Object}     data   - { nodes: [...], edges: [...] }
   */
  function renderGraph(svgEl, data) {
    var d3 = global.d3;
    if (!d3) { console.error('[graph-render] window.d3 가 없습니다. D3를 먼저 로드하세요.'); return; }
    if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
      console.error('[graph-render] 데이터 형식이 올바르지 않습니다.', data);
      return;
    }

    function getNodeColor(d) {
      var type = (d.entity_type || d.type || '').toString().trim().toUpperCase();
      return COLORS[type] || COLORS.unknown;
    }

    var w = svgEl.clientWidth  || global.innerWidth;
    var h = svgEl.clientHeight || global.innerHeight;

    var svg = d3.select(svgEl);
    svg.selectAll('*').remove(); // 재호출 시 중복 방지

    var g = svg.append('g');

    // 줌/패닝
    var zoom = d3.zoom().on('zoom', function(e) { g.attr('transform', e.transform); });
    svg.call(zoom);
    svg.call(zoom.transform, d3.zoomIdentity.translate(w / 2, h / 2).scale(0.5));

    // Force 시뮬레이션
    var nodes = data.nodes.map(function(d) { return Object.assign({}, d); });
    var edges = data.edges.map(function(d) { return Object.assign({}, d); });

    var simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(edges).id(function(d) { return d.id; }).distance(80))
      .force('charge', d3.forceManyBody().strength(-150))
      .force('collide', d3.forceCollide(35))
      .force('center', d3.forceCenter(0, 0));

    // 엣지
    var link = g.append('g')
      .selectAll('line')
      .data(edges)
      .join('line')
      .attr('stroke', '#aaaaaa')
      .attr('stroke-width', 1.5)
      .attr('stroke-opacity', 0.6);

    // 노드
    var node = g.append('g')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .call(
        d3.drag()
          .on('start', function(e, d) {
            if (!e.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x; d.fy = d.y;
          })
          .on('drag', function(e, d) { d.fx = e.x; d.fy = e.y; })
          .on('end', function(e, d) {
            if (!e.active) simulation.alphaTarget(0);
            d.fx = null; d.fy = null;
          })
      );

    node.append('circle')
      .attr('r', 30)
      .attr('fill', function(d) { return getNodeColor(d); })
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5);

    node.append('text')
      .text(function(d) { return d.label || d.id; })
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'middle')
      .attr('font-size', 10)
      .attr('fill', '#333');

    // 틱마다 위치 업데이트
    simulation.on('tick', function() {
      link
        .attr('x1', function(d) { return d.source.x; })
        .attr('y1', function(d) { return d.source.y; })
        .attr('x2', function(d) { return d.target.x; })
        .attr('y2', function(d) { return d.target.y; });
      node.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')'; });
    });
  }

  global.renderGraph = renderGraph;
})(typeof window !== 'undefined' ? window : this);
