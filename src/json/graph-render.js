/**
 * graph-render.js — 고급 그래프 렌더링 함수
 * 의존성: window.d3 (v7)
 */
(function (global) {
  const COLORS = {
    EMAIL: "#f87171", // 빨강
    PERSON: "#ffa255", // 주황
    TOPIC: "#eef616", // 노랑
    ORGANIZATION: "#34d399", // 초록
    LABEL: "#60a5fa", // 파랑
    EVENT: "#a78bfa", // 보라
    ATTACHMENT:   "#7b8899", // 회청
    unknown: "#c9d1d9", // 회색
  };
  function renderGraph(svgEl, data) {
    // 툴팁 헬퍼
    const tip = document.getElementById("tooltip");

    function showTip(html, event) {
      // 툴팁 보여주는 함수
      tip.innerHTML = html;
      tip.classList.add("visible");
      moveTip(event);
    }

    function moveTip(event) {
      // 툴팁 위치 마우스 커서 근처로 이동
      const pad = 16;
      let x = event.clientX + pad; // 기본: 커서 오른쪽
      let y = event.clientY + pad; // 기본: 커서 아래쪽
      if (x + 280 > window.innerWidth) x = event.clientX - 280 - pad; // 화면 오른쪽 끝에 걸리면 커서 왼쪽으로 방향 전환
      if (y + 160 > window.innerHeight) y = event.clientY - 160 - pad; // 화면 아래쪽 끝에 걸리면 커서 위쪽으로 방향 전환
      tip.style.left = x + "px";
      tip.style.top = y + "px";
    }

    function hideTip() {
      tip.classList.remove("visible");
    } // 툴팁 숨기기

    // description 분량 제한
    function shortDesc(text, maxLen = 300) {  // 300자로 수정
      if (!text) return "";
      return text.length > maxLen
        ? text.slice(0, maxLen).trimEnd() + "…"
        : text; // 300자 초과분은 숨김
    }

    function edgeWidth(weight) {
      // 엣지 두께 계산
      if (weight == null) return 1.5;
      return Math.min(6, 1 + weight * 0.2);
    }

    const svgRect = svgEl.getBoundingClientRect();
    const w = svgRect.width  || window.innerWidth;
    const h = svgRect.height || window.innerHeight;

    /* 1440px 기준으로 노드/링크 크기 비례 스케일 */
    const viewScale = Math.max(0.5, Math.min(2.0, w / 1440));

    // degree 기준으로 노드 반지름 계산 (뷰포트 비례)
    const _rScale = d3.scaleSqrt()
      .domain([0, 30])
      .range([20 * viewScale, 55 * viewScale])
      .clamp(true);
    function nodeRadius(d) {
      return _rScale(d.degree ?? 1);
    }

    const svg = d3.select("#graph"); // svg 요소
    const g = svg.append("g"); // 그래프 담을 태그

    const zoom = d3.zoom()
      .scaleExtent([0.05, 4])
      .on("zoom", (e) => g.attr("transform", e.transform));
    svg.call(zoom);

    // 노드들 간의 힘 설정 (움직임)
    const simulation = d3
      .forceSimulation(data.nodes)
      .force(
        "link",
        d3
          .forceLink(data.edges)
          .id((d) => d.label)
          .distance(140 * viewScale),
      )
      .force("charge", d3.forceManyBody().strength(-800 * viewScale))
      .force("collide", d3.forceCollide(d => nodeRadius(d) + 8))
      .force("center", d3.forceCenter(0, 0));

    // 엣지 그리기
    const link = g
      .append("g")
      .selectAll("line")
      .data(data.edges) // 엣지 데이터 바인딩
      .join("line") // 선 생성
      .attr("stroke", "#aaaaaa") // 엣지 색상
      .attr("stroke-width", (d) =>
        d.weight == null ? 1.5 : Math.min(6, 1 + d.weight * 0.2),
      ) // 두께
      .attr("stroke-opacity", 0.6) // 투명도
      .style("cursor", "pointer")
      .on("mouseover", (event, d) => {
        // 엣지 위에 마우스 올렸을 때 엣지 불투명하게 강조
        d3.select(event.currentTarget)
          .attr("stroke", "#aaaaaa")
          .attr(
            "stroke-width",
            d.weight == null ? 1.5 : Math.min(6, 1 + d.weight * 0.2),
          )
          .attr("stroke-opacity", 1);

        // 툴팁에 표시할 데이터
        const src = d.source.label ?? d.source;
        const tgt = d.target.label ?? d.target;
        const desc = shortDesc(d.description);
        const wt = d.weight != null ? `가중치 ${d.weight}` : "";

        showTip(
          `
                <div class="tt-type">엣지</div>
                <div class="tt-arrow">${src} → ${tgt}</div>
                ${desc ? `<hr class="tt-divider"><div class="tt-desc">${desc}</div>` : ""}
                ${wt ? `<div class="tt-meta">${wt}</div>` : ""}
              `,
          event,
        );
      })

      .on("mousemove", moveTip) // 마우스가 엣지 위에서 움직일 때 툴팁도 이동
      .on("mouseout", (event, d) => {
        // 마우스가 엣지 위에서 벗어날 때
        d3.select(event.currentTarget).attr("stroke-opacity", 0.6); // 투명도 복원
        hideTip();
      });

    // 노드 그리기
    const node = g
      .append("g")
      .selectAll("g")
      .data(data.nodes) // 노드 데이터 바인딩
      .join("g") // 각 노드마다 g 요소 생성
      .call(
        d3
          .drag() // 드래그 이벤트 등록
          .on("start", (e, d) => {
            // 드래그하면 simulation 재시작
            if (!e.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y; // 고정된 노드 위치
          })
          .on("drag", (e, d) => {
            d.fx = e.x;
            d.fy = e.y;
          }) // 노드 위치 마우스 위치로 업데이트
          .on("end", (e, d) => {
            // 드래그 끝나면 노드 고정 끝. 자유롭게 움직임.
            if (!e.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
          }),
      );
    // 노드
    node
      .append("circle")
      .attr("r", d => nodeRadius(d)) // 원 크기 (반지름)
      .attr("fill", (d) => COLORS[d.entity_type] || COLORS.unknown) // 타입별 색상
      .attr("stroke", "#fff") // 테두리 = 흰색
      .attr("stroke-width", 1.5) // 테두리 두께
      .style("cursor", "pointer")
      .on("mouseover", (event, d) => {
        // 노드 위에 마우스 올렸을 때
        d3.select(event.currentTarget) // 노드 강조: 원 확대 + 밝기 증가
          .attr("r", nodeRadius(d) + 8)
          .attr("filter", "brightness(1.25)");

        const desc = shortDesc(d.description);
        const degree = d.degree != null ? `연결 수 ${d.degree}` : "";
        const id =
          d.human_readable_id != null
            ? `#${d.human_readable_id}`
            : (d.id ?? "");

        showTip(
          `
              <div class="tt-type">${d.entity_type ?? "unknown"}</div>
              <div class="tt-label">${d.label ?? d.id}</div>
              ${desc ? `<hr class="tt-divider"><div class="tt-desc">${desc}</div>` : ""}
              <div class="tt-meta">${[id, degree].filter(Boolean).join(" · ")}</div>
            `,
          event,
        );
      })
      .on("mousemove", moveTip) // 노드 위에서 마우스 움직일 때 툴팁도 이동
      .on("mouseout", (event, d) => {
        // 마우스가 노드 벗어나면 원래 크기로 복구 + 툴팁 숨김
        d3.select(event.currentTarget).attr("r", nodeRadius(d)).attr("filter", null);
        hideTip();
      });

    // 노드 안에 텍스트 표시
    node
      .append("text")
      .text((d) => {
          const label = d.label || d.id;
          const maxLen = Math.floor(nodeRadius(d) / 4.5);
          return label.length > maxLen ? label.slice(0, maxLen) + "…" : label;
      }) // label 없으면 id 표시
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("font-size", d => Math.max(11, Math.min(15, nodeRadius(d) * 0.52)))
      .attr("font-weight", "700")
      .attr("fill", "#1a1a1a")
      .style("pointer-events", "none");

    let _fitted = false;

    function fitToView() {
      const nodes = data.nodes;
      if (!nodes.length) return;
      const pad = 60;
      let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
      nodes.forEach(d => {
        const r = nodeRadius(d);
        x0 = Math.min(x0, d.x - r);
        y0 = Math.min(y0, d.y - r);
        x1 = Math.max(x1, d.x + r);
        y1 = Math.max(y1, d.y + r);
      });
      const bw = x1 - x0, bh = y1 - y0;
      if (bw <= 0 || bh <= 0) return;
      const scale = Math.min(0.9, (w - pad * 2) / bw, (h - pad * 2) / bh);
      const tx = w / 2 - scale * ((x0 + x1) / 2);
      const ty = h / 2 - scale * ((y0 + y1) / 2);
      svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
    }

    // 시뮬레이션 틱마다 노드와 엣지 위치 업데이트
    simulation.on("tick", () => {
      link
        .attr("x1", (d) => d.source.x)
        .attr("y1", (d) => d.source.y)
        .attr("x2", (d) => d.target.x)
        .attr("y2", (d) => d.target.y);
      node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });

    simulation.on("end", () => {
      if (!_fitted) { _fitted = true; fitToView(); }
    });

    // 시뮬레이션이 오래 걸릴 경우 300틱 후에도 fit 시도
    let _tickCount = 0;
    const _origTick = simulation.on("tick");
    simulation.on("tick.fit", () => {
      if (!_fitted && ++_tickCount >= 300) {
        _fitted = true;
        fitToView();
        simulation.on("tick.fit", null);
      }
    });

    // 전체 보기 버튼 연결
    const fitBtn = document.getElementById('mp-graph-fit-btn');
    if (fitBtn) fitBtn.onclick = fitToView;
  }

  global.renderGraph = renderGraph;
})(typeof window !== "undefined" ? window : this);