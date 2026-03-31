/**
 * graph-render.js — 고급 그래프 렌더링 함수
 * 의존성: window.d3 (v7)
 */
(function (global) {

  const PALETTE = {
    EMAIL:        { fill: "#f87171", glow: "#ff4444", dark: "#c0392b" },
    PERSON:       { fill: "#fb923c", glow: "#ff7700", dark: "#d35400" },
    TOPIC:        { fill: "#fbbf24", glow: "#ffcc00", dark: "#d4a017" },
    ORGANIZATION: { fill: "#34d399", glow: "#00ffaa", dark: "#1a9e6e" },
    LABEL:        { fill: "#60a5fa", glow: "#4488ff", dark: "#2563eb" },
    EVENT:        { fill: "#a78bfa", glow: "#9966ff", dark: "#7c3aed" },
    unknown:      { fill: "#94a3b8", glow: "#aabbcc", dark: "#64748b" },
  };

  function getPalette(d) {
    var key = (d.entity_type || d.type || "").toString().trim().toUpperCase();
    return PALETTE[key] || PALETTE.unknown;
  }

  function renderGraph(svgEl, data) {
    var d3 = global.d3;
    if (!d3) { console.error("[graph-render] d3 없음"); return; }
    if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
      console.error("[graph-render] 데이터 형식 오류", data); return;
    }

    var W = svgEl.clientWidth  || global.innerWidth;
    var H = svgEl.clientHeight || global.innerHeight;

    var svg = d3.select(svgEl);
    svg.selectAll("*").remove();

    // ── 다크 배경 ──────────────────────────────────────────────────
    svg.style("background", "linear-gradient(135deg,#0d1f16 0%,#0a1a10 60%,#060e08 100%)");

    // ── SVG defs (필터·그라디언트·마커) ───────────────────────────
    var defs = svg.append("defs");

    // 글로우 필터 (노드용)
    Object.entries(PALETTE).forEach(function(entry) {
      var key = entry[0], pal = entry[1];
      var f = defs.append("filter")
        .attr("id", "glow-" + key)
        .attr("x", "-60%").attr("y", "-60%")
        .attr("width", "220%").attr("height", "220%");
      f.append("feGaussianBlur")
        .attr("in", "SourceGraphic").attr("stdDeviation", "6").attr("result", "blur");
      var merge = f.append("feMerge");
      merge.append("feMergeNode").attr("in", "blur");
      merge.append("feMergeNode").attr("in", "blur");
      merge.append("feMergeNode").attr("in", "SourceGraphic");
    });

    // 강한 글로우 필터 (hover용)
    var hf = defs.append("filter").attr("id", "glow-hover")
      .attr("x", "-80%").attr("y", "-80%").attr("width", "260%").attr("height", "260%");
    hf.append("feGaussianBlur").attr("in", "SourceGraphic").attr("stdDeviation", "12").attr("result", "b");
    var hm = hf.append("feMerge");
    hm.append("feMergeNode").attr("in", "b");
    hm.append("feMergeNode").attr("in", "b");
    hm.append("feMergeNode").attr("in", "SourceGraphic");

    // 엣지 글로우
    var ef = defs.append("filter").attr("id", "glow-edge")
      .attr("x", "-20%").attr("y", "-200%").attr("width", "140%").attr("height", "500%");
    ef.append("feGaussianBlur").attr("in", "SourceGraphic").attr("stdDeviation", "2.5").attr("result", "b");
    var em = ef.append("feMerge");
    em.append("feMergeNode").attr("in", "b");
    em.append("feMergeNode").attr("in", "SourceGraphic");

    // 라디얼 그라디언트 (각 노드 타입)
    Object.entries(PALETTE).forEach(function(entry) {
      var key = entry[0];
      var rg = defs.append("radialGradient")
        .attr("id", "grad-" + key)
        .attr("cx", "35%").attr("cy", "30%").attr("r", "65%");
      rg.append("stop").attr("offset", "0%").attr("stop-color", "#ffffff").attr("stop-opacity", "0.35");
      rg.append("stop").attr("offset", "45%").attr("stop-color", pal.fill).attr("stop-opacity", "1");
      rg.append("stop").attr("offset", "100%").attr("stop-color", pal.dark).attr("stop-opacity", "1");
    });

    // 화살표 마커
    defs.append("marker")
      .attr("id", "arrow").attr("viewBox", "0 -4 10 8")
      .attr("refX", 28).attr("refY", 0)
      .attr("markerWidth", 6).attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path").attr("d", "M0,-4L10,0L0,4").attr("fill", "rgba(52,211,153,0.5)");

    // ── 배경 그리드 ───────────────────────────────────────────────
    var gridG = svg.append("g").attr("class", "grid").attr("pointer-events", "none");
    var gridSpacing = 60;
    for (var gx = 0; gx < W * 3; gx += gridSpacing) {
      gridG.append("line")
        .attr("x1", gx - W).attr("y1", -H).attr("x2", gx - W).attr("y2", H * 2)
        .attr("stroke", "rgba(52,211,153,0.04)").attr("stroke-width", 1);
    }
    for (var gy = 0; gy < H * 3; gy += gridSpacing) {
      gridG.append("line")
        .attr("x1", -W).attr("y1", gy - H).attr("x2", W * 2).attr("y2", gy - H)
        .attr("stroke", "rgba(52,211,153,0.04)").attr("stroke-width", 1);
    }

    // ── 메인 그룹 ─────────────────────────────────────────────────
    var g = svg.append("g");

    // 줌/패닝
    var zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on("zoom", function (e) {
        g.attr("transform", e.transform);
        gridG.attr("transform", e.transform);
      });
    svg.call(zoom);
    svg.call(zoom.transform, d3.zoomIdentity.translate(W / 2, H / 2).scale(0.6));

    // ── 데이터 준비 ───────────────────────────────────────────────
    var nodes = data.nodes.map(function (d) { return Object.assign({}, d); });
    var edges = data.edges.map(function (d) { return Object.assign({}, d); });

    // 연결 수로 노드 크기 결정
    var degreeMap = {};
    nodes.forEach(function(d) { degreeMap[d.id] = 0; });
    edges.forEach(function(e) {
      var s = typeof e.source === "object" ? e.source.id : e.source;
      var t = typeof e.target === "object" ? e.target.id : e.target;
      degreeMap[s] = (degreeMap[s] || 0) + 1;
      degreeMap[t] = (degreeMap[t] || 0) + 1;
    });
    var maxDeg = Math.max(1, d3.max(Object.values(degreeMap)));
    var rScale = d3.scaleSqrt().domain([0, maxDeg]).range([14, 38]);

    // ── 시뮬레이션 ────────────────────────────────────────────────
    var simulation = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(edges).id(function(d){ return d.id; }).distance(function(d){
        var s = typeof d.source === "object" ? d.source.id : d.source;
        var t = typeof d.target === "object" ? d.target.id : d.target;
        return 80 + rScale(degreeMap[s] || 0) + rScale(degreeMap[t] || 0);
      }))
      .force("charge", d3.forceManyBody().strength(-320))
      .force("collide", d3.forceCollide(function(d){ return rScale(degreeMap[d.id] || 0) + 18; }))
      .force("center", d3.forceCenter(0, 0))
      .alphaDecay(0.018);

    // ── 엣지 ─────────────────────────────────────────────────────
    var linkG = g.append("g").attr("class", "links");

    // 엣지 글로우 레이어
    var linkGlow = linkG.selectAll(".link-glow")
      .data(edges).join("line")
      .attr("class", "link-glow")
      .attr("stroke", "rgba(52,211,153,0.12)")
      .attr("stroke-width", 6)
      .attr("filter", "url(#glow-edge)");

    // 실제 엣지
    var link = linkG.selectAll(".link")
      .data(edges).join("line")
      .attr("class", "link")
      .attr("stroke", "rgba(52,211,153,0.28)")
      .attr("stroke-width", 1.5)
      .attr("marker-end", "url(#arrow)");

    // ── 노드 그룹 ─────────────────────────────────────────────────
    var nodeG = g.append("g").attr("class", "nodes");

    var node = nodeG.selectAll(".node")
      .data(nodes).join("g")
      .attr("class", "node")
      .style("cursor", "pointer")
      .call(d3.drag()
        .on("start", function(e, d) {
          if (!e.active) simulation.alphaTarget(0.3).restart();
          d.fx = d.x; d.fy = d.y;
        })
        .on("drag", function(e, d) { d.fx = e.x; d.fy = e.y; })
        .on("end", function(e, d) {
          if (!e.active) simulation.alphaTarget(0);
          d.fx = null; d.fy = null;
        })
      );

    // 외부 링 (pulse)
    node.append("circle")
      .attr("class", "node-ring")
      .attr("r", function(d) { return rScale(degreeMap[d.id] || 0) + 8; })
      .attr("fill", "none")
      .attr("stroke", function(d) { return getPalette(d).fill; })
      .attr("stroke-width", 1.5)
      .attr("stroke-opacity", 0.25);

    // 글로우 원
    node.append("circle")
      .attr("class", "node-glow")
      .attr("r", function(d) { return rScale(degreeMap[d.id] || 0) + 4; })
      .attr("fill", function(d) { return getPalette(d).glow; })
      .attr("fill-opacity", 0.18)
      .attr("filter", function(d) {
        var key = (d.entity_type || d.type || "unknown").toString().trim().toUpperCase();
        return "url(#glow-" + (PALETTE[key] ? key : "unknown") + ")";
      });

    // 메인 원
    node.append("circle")
      .attr("class", "node-circle")
      .attr("r", function(d) { return rScale(degreeMap[d.id] || 0); })
      .attr("fill", function(d) {
        var key = (d.entity_type || d.type || "unknown").toString().trim().toUpperCase();
        return "url(#grad-" + (PALETTE[key] ? key : "unknown") + ")";
      })
      .attr("stroke", function(d) { return getPalette(d).fill; })
      .attr("stroke-width", 1.8)
      .attr("stroke-opacity", 0.7);

    // 라벨
    node.append("text")
      .attr("class", "node-label")
      .text(function(d) {
        var label = d.label || d.id || "";
        return label.length > 12 ? label.slice(0, 11) + "…" : label;
      })
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("font-size", function(d) { return Math.max(9, Math.min(12, rScale(degreeMap[d.id]||0) * 0.4)); })
      .attr("font-family", "'Inter','Helvetica Neue',Arial,sans-serif")
      .attr("font-weight", "600")
      .attr("fill", "#ffffff")
      .attr("pointer-events", "none");

    // 타입 레이블 (노드 아래)
    node.append("text")
      .attr("class", "node-type")
      .text(function(d) { return (d.entity_type || d.type || "").toUpperCase(); })
      .attr("text-anchor", "middle")
      .attr("y", function(d) { return rScale(degreeMap[d.id] || 0) + 14; })
      .attr("font-size", 8)
      .attr("font-family", "'Inter','Helvetica Neue',Arial,sans-serif")
      .attr("font-weight", "700")
      .attr("letter-spacing", "0.06em")
      .attr("fill", function(d) { return getPalette(d).fill; })
      .attr("fill-opacity", 0.75)
      .attr("pointer-events", "none");

    // ── 툴팁 ─────────────────────────────────────────────────────
    var tooltip = d3.select(svgEl.parentNode || document.body)
      .append("div")
      .style("position", "absolute")
      .style("pointer-events", "none")
      .style("background", "rgba(10,26,16,0.92)")
      .style("border", "1px solid rgba(52,211,153,0.3)")
      .style("border-radius", "10px")
      .style("padding", "10px 14px")
      .style("font-family", "'Inter',sans-serif")
      .style("font-size", "12px")
      .style("color", "#e2f5ec")
      .style("backdrop-filter", "blur(8px)")
      .style("box-shadow", "0 8px 24px rgba(0,0,0,0.5)")
      .style("opacity", 0)
      .style("transition", "opacity 0.15s")
      .style("z-index", 9999)
      .style("max-width", "200px");

    // ── 호버 인터랙션 ─────────────────────────────────────────────
    var linkedSet = {};
    edges.forEach(function(e) {
      var s = typeof e.source === "object" ? e.source.id : e.source;
      var t = typeof e.target === "object" ? e.target.id : e.target;
      var key = s + "||" + t;
      linkedSet[key] = true; linkedSet[t + "||" + s] = true;
    });

    function isLinked(a, b) { return a === b || linkedSet[a + "||" + b]; }

    node
      .on("mouseover", function(event, d) {
        // 연결 안된 노드 dim
        node.style("opacity", function(o) { return isLinked(d.id, o.id) ? 1 : 0.15; });
        link.style("opacity", function(o) {
          var s = typeof o.source === "object" ? o.source.id : o.source;
          var t = typeof o.target === "object" ? o.target.id : o.target;
          return (s === d.id || t === d.id) ? 1 : 0.05;
        }).attr("stroke", function(o) {
          var s = typeof o.source === "object" ? o.source.id : o.source;
          var t = typeof o.target === "object" ? o.target.id : o.target;
          return (s === d.id || t === d.id) ? "rgba(52,211,153,0.9)" : "rgba(52,211,153,0.1)";
        }).attr("stroke-width", function(o) {
          var s = typeof o.source === "object" ? o.source.id : o.source;
          var t = typeof o.target === "object" ? o.target.id : o.target;
          return (s === d.id || t === d.id) ? 2.5 : 1.5;
        });
        linkGlow.style("opacity", function(o) {
          var s = typeof o.source === "object" ? o.source.id : o.source;
          var t = typeof o.target === "object" ? o.target.id : o.target;
          return (s === d.id || t === d.id) ? 1 : 0;
        });

        // 호버 노드 강조
        d3.select(this).select(".node-circle")
          .attr("filter", "url(#glow-hover)")
          .attr("stroke-width", 3)
          .attr("stroke-opacity", 1);
        d3.select(this).select(".node-ring")
          .attr("stroke-opacity", 0.6)
          .attr("r", function(d) { return rScale(degreeMap[d.id] || 0) + 14; });

        // 툴팁
        var deg = degreeMap[d.id] || 0;
        tooltip
          .html(
            "<div style='color:" + getPalette(d).fill + ";font-weight:700;font-size:13px;margin-bottom:6px'>" +
            (d.label || d.id) + "</div>" +
            "<div style='color:rgba(200,230,216,0.7);font-size:10px;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:4px'>" +
            (d.entity_type || d.type || "unknown") + "</div>" +
            "<div style='margin-top:6px;color:#94d4b4'>연결: <b>" + deg + "</b>개</div>"
          )
          .style("opacity", 1);
      })
      .on("mousemove", function(event) { // eslint-disable-line no-unused-vars
        var rect = svgEl.getBoundingClientRect();
        tooltip
          .style("left", (event.clientX - rect.left + 14) + "px")
          .style("top",  (event.clientY - rect.top  - 10) + "px");
      })
      .on("mouseout", function() {
        node.style("opacity", 1);
        link.style("opacity", 1)
          .attr("stroke", "rgba(52,211,153,0.28)")
          .attr("stroke-width", 1.5);
        linkGlow.style("opacity", 1);
        d3.select(this).select(".node-circle")
          .attr("filter", null)
          .attr("stroke-width", 1.8)
          .attr("stroke-opacity", 0.7);
        d3.select(this).select(".node-ring")
          .attr("stroke-opacity", 0.25)
          .attr("r", function(d) { return rScale(degreeMap[d.id] || 0) + 8; });
        tooltip.style("opacity", 0);
      });

    // ── Pulse 애니메이션 (CSS keyframes를 SVG animate로) ──────────
    node.select(".node-ring").each(function(d) {
      var delay = Math.random() * 2000;
      var el = d3.select(this);
      var baseR = rScale(degreeMap[d.id] || 0) + 8;
      (function pulse() {
        el.transition().duration(1200).delay(delay)
          .attr("r", baseR + 10).attr("stroke-opacity", 0)
          .transition().duration(0)
          .attr("r", baseR).attr("stroke-opacity", 0.25)
          .on("end", function() { delay = 0; pulse(); });
      })();
    });

    // ── 틱 ───────────────────────────────────────────────────────
    simulation.on("tick", function () {
      function lx(d) { return typeof d.source === "object" ? d.source.x : 0; }
      function ly(d) { return typeof d.source === "object" ? d.source.y : 0; }
      function tx(d) { return typeof d.target === "object" ? d.target.x : 0; }
      function ty(d) { return typeof d.target === "object" ? d.target.y : 0; }

      link.attr("x1", lx).attr("y1", ly).attr("x2", tx).attr("y2", ty);
      linkGlow.attr("x1", lx).attr("y1", ly).attr("x2", tx).attr("y2", ty);
      node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
    });
  }

  global.renderGraph = renderGraph;
})(typeof window !== "undefined" ? window : this);
