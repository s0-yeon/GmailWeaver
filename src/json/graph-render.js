/**
 * graph-render.js — 고급 그래프 렌더링 함수
 * 의존성: window.d3 (v7)
 */
(function (global) {
  const COLORS = {
    EMAIL: "#f87171", // 빨강
    PERSON: "#fb923c", // 주황
    TOPIC: "#fbbf24", // 노랑
    ORGANIZATION: "#34d399", // 초록
    LABEL: "#60a5fa", // 파랑
    EVENT: "#a78bfa", // 보라
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

    // description을 3줄 분량(120자)으로 자르기
    function shortDesc(text, maxLen = 120) {
      if (!text) return "";
      return text.length > maxLen
        ? text.slice(0, maxLen).trimEnd() + "…"
        : text; // 120자 초과분은 숨김
    }

    function edgeWidth(weight) {
      // 엣지 두께 계산
      if (weight == null) return 1.5;
      return Math.min(6, 1 + weight * 0.2);
    }

    const w = window.innerWidth;
    const h = window.innerHeight;

    const svg = d3.select("#graph"); // svg 요소
    const g = svg.append("g"); // 그래프 담을 태그

    const zoom = d3.zoom().on("zoom", (e) => g.attr("transform", e.transform));
    svg.call(zoom); // svg에 줌 이벤트 등록
    svg.call(
      zoom.transform,
      d3.zoomIdentity.translate(w / 2, h / 2).scale(0.5),
    ); // 초기 줌 레벨 = 0.5 (그래프 전체가 보이도록)

    // 노드들 간의 힘 설정 (움직임)
    const simulation = d3
      .forceSimulation(data.nodes)
      .force(
        "link",
        d3
          .forceLink(data.edges)
          .id((d) => d.label) // 엣지의 source와 target가 label 기준으로 연결
          .distance(80),
      )
      .force("charge", d3.forceManyBody().strength(-200)) // 노드들 간의 밀어내는 힘
      .force("collide", d3.forceCollide(30)); // 노드들 겹치지 않도록 함

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
      .attr("r", 30) // 원 크기 (반지름)
      .attr("fill", (d) => COLORS[d.entity_type] || COLORS.unknown) // 타입별 색상
      .attr("stroke", "#fff") // 테두리 = 흰색
      .attr("stroke-width", 1.5) // 테두리 두께
      .style("cursor", "pointer")
      .on("mouseover", (event, d) => {
        // 노드 위에 마우스 올렸을 때
        d3.select(event.currentTarget) // 노드 강조: 원 확대 + 밝기 증가
          .attr("r", 36)
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
      .on("mouseout", (event) => {
        // 마우스가 노드 벗어나면 원래 크기로 복구 + 툴팁 숨김
        d3.select(event.currentTarget).attr("r", 30).attr("filter", null);
        hideTip();
      });

    // 노드 안에 텍스트 표시
    node
      .append("text")
      .text((d) => d.label || d.id) // label 없으면 id 표시
      .attr("text-anchor", "middle") // 가로 중앙 정렬
      .attr("dominant-baseline", "middle") // 세로 중앙 정렬
      .attr("font-size", 10) // 글자 크기
      .attr("fill", "#333") // 글자 색상
      .style("pointer-events", "none");

    // 시뮬레이션 틱마다 노드와 엣지 위치 업데이트
    simulation.on("tick", () => {
      // 엣지 양 끝점 위치 업데이트
      link
        .attr("x1", (d) => d.source.x) // 처음 노드 x좌표
        .attr("y1", (d) => d.source.y) // 처음 노드 y좌표
        .attr("x2", (d) => d.target.x) // 나중 노드 x좌표
        .attr("y2", (d) => d.target.y); // 나중 노드 y좌표
      // 노드위치 업데이트
      node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });
  }

  global.renderGraph = renderGraph;
})(typeof window !== "undefined" ? window : this);
