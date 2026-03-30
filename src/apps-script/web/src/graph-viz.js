/**
 * graph-viz.js — dashboard용 그래프 탭 로더
 *
 * 렌더링 로직은 /graph-render.js (공유 파일)에서 관리.
 * CDN D3 + graph-render.js를 동적으로 로드한 뒤 renderGraph()를 호출.
 */

const D3_CDN = 'https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js';
const GRAPH_RENDER_URL = '/graph-render.js';

function loadScript(src) {
  return new Promise((resolve, reject) => {
    if (document.querySelector(`script[src="${src}"]`)) { resolve(); return; }
    const s = document.createElement('script');
    s.src = src;
    s.onload = resolve;
    s.onerror = () => reject(new Error(`스크립트 로드 실패: ${src}`));
    document.head.appendChild(s);
  });
}

export async function loadGraphViz() {
  const svgEl = document.getElementById('graph');
  if (!svgEl) { console.error('[graph-viz] #graph 요소가 없습니다.'); return; }

  // 로딩 표시
  const section = document.getElementById('graph-section');
  let loader = null;
  if (section) {
    loader = section.querySelector('.gv-loader');
    if (!loader) {
      loader = document.createElement('div');
      loader.className = 'gv-loader';
      loader.style.cssText = [
        'position:absolute', 'inset:0', 'display:flex',
        'align-items:center', 'justify-content:center',
        'font-size:1rem', 'color:#73879C',
        'background:rgba(255,255,255,0.85)', 'z-index:10'
      ].join(';');
      loader.textContent = '그래프 불러오는 중...';
      section.appendChild(loader);
    } else {
      loader.style.display = 'flex';
    }
  }

  function removeLoader() {
    if (loader) loader.style.display = 'none';
  }

  function showError(msg) {
    removeLoader();
    svgEl.innerHTML = '';
    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    text.setAttribute('x', '50%');
    text.setAttribute('y', '50%');
    text.setAttribute('text-anchor', 'middle');
    text.setAttribute('fill', '#e74c3c');
    text.setAttribute('font-size', '16');
    text.textContent = msg;
    svgEl.appendChild(text);
  }

  try {
    // 1) D3 CDN → 2) 공유 렌더러 순서로 로드
    await loadScript(D3_CDN);
    await loadScript(GRAPH_RENDER_URL);

    // 2) 데이터 fetch
    const res = await fetch('/graph-data');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    // 3) 렌더링 (graph-render.js의 window.renderGraph 사용)
    removeLoader();
    window.renderGraph(svgEl, data);

  } catch (err) {
    showError('그래프 로드 실패: Flask 서버가 실행 중인지 확인하세요.');
    console.error('[graph-viz]', err);
  }
}
