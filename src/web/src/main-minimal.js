// Modern jQuery-Free Minimal Bundle
// Complete replacement for jQuery dependencies

// Native DOM utilities (jQuery replacement) - LOAD FIRST
const DOM = {
  ready: callback => {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', callback);
    } else {
      callback();
    }
  },
  select: selector => document.querySelector(selector),
  selectAll: selector => [...document.querySelectorAll(selector)],
  addClass: (element, className) => element?.classList.add(className),
  removeClass: (element, className) => element?.classList.remove(className),
  toggleClass: (element, className) => element?.classList.toggle(className),
  hasClass: (element, className) => element?.classList.contains(className),
  closest: (element, selector) => element?.closest(selector),
  find: (element, selector) => element?.querySelector(selector),
  findAll: (element, selector) => [...(element?.querySelectorAll(selector) || [])],
  animate: (element, properties, duration = 1000, easing = 'ease') => {
    return new Promise(resolve => {
      const transitions = [];
      Object.keys(properties).forEach(prop => {
        const camelProp = prop.replace(/-([a-z])/g, g => g[1].toUpperCase());
        element.style.setProperty('transition', `${prop} ${duration}ms ${easing}`);
        element.style[camelProp] = properties[prop];
        transitions.push(`${prop} ${duration}ms ${easing}`);
      });

      element.style.transition = transitions.join(', ');

      setTimeout(() => {
        element.style.transition = '';
        resolve();
      }, duration);
    });
  }
};

// Make DOM utilities available globally immediately
window.DOM = DOM;

// Import security utilities for XSS protection
import './utils/security.js';

// Native easing functions (jQuery-free)
const EasingFunctions = {
  easeOutElastic: function (t, b, c, d) {
    let s = 1.70158;
    let p = 0;
    let a = c;
    if (t === 0) {
      return b;
    }
    if ((t /= d) === 1) {
      return b + c;
    }
    if (!p) {
      p = d * 0.3;
    }
    if (a < Math.abs(c)) {
      a = c;
      s = p / 4;
    } else {
      s = (p / (2 * Math.PI)) * Math.asin(c / a);
    }
    return a * Math.pow(2, -10 * t) * Math.sin(((t * d - s) * (2 * Math.PI)) / p) + c + b;
  },
  easeInOutQuart: function (t, b, c, d) {
    if ((t /= d / 2) < 1) {
      return (c / 2) * t * t * t * t + b;
    }
    return (-c / 2) * ((t -= 2) * t * t * t - 2) + b;
  }
};

window.EasingFunctions = EasingFunctions;

// Import jQuery-free vendor libraries

// Toggle switches now use Bootstrap 5 native form-switch component

// Choices.js (Select2 replacement)
import Choices from 'choices.js';
window.Choices = Choices;

// NoUiSlider (Ion Range Slider replacement)
import noUiSlider from 'nouislider';
window.noUiSlider = noUiSlider;

// Bootstrap 5 - No jQuery dependency needed
import * as bootstrap from 'bootstrap';
window.bootstrap = bootstrap;
globalThis.bootstrap = bootstrap;

// TempusDominus DateTimePicker (Bootstrap 5 compatible)
import { TempusDominus } from '@eonasdan/tempus-dominus';
window.TempusDominus = TempusDominus;
globalThis.TempusDominus = TempusDominus;

// Chart.js v4 - No jQuery dependency
import { Chart, registerables } from 'chart.js';
try {
  Chart.register(...registerables);
  window.Chart = Chart;
  globalThis.Chart = Chart;
} catch (error) {
  window.Chart = Chart;
  globalThis.Chart = Chart;
}

// ECharts - Apache ECharts library
import * as echarts from 'echarts';
window.echarts = echarts;
globalThis.echarts = echarts;

// Skycons (Animated weather icons)
import SkyconsFactory from 'skycons';
try {
  const Skycons = SkyconsFactory(typeof window !== 'undefined' ? window : globalThis);
  window.Skycons = Skycons;
  globalThis.Skycons = Skycons;
} catch (error) {}

// Leaflet (for maps)
import * as L from 'leaflet';
window.L = L;
globalThis.L = L;

// Global styles (Bootstrap 5 + custom)
import './main.scss';

// Add global error handlers to prevent uncaught promise rejections
window.addEventListener('unhandledrejection', event => {
  event.preventDefault();
});

window.addEventListener('error', event => {});

// CSS imports for libraries
import 'leaflet/dist/leaflet.css';
import '@eonasdan/tempus-dominus/dist/css/tempus-dominus.min.css';
import 'nouislider/dist/nouislider.css';
import 'choices.js/public/assets/styles/choices.min.css';
import '@simonwep/pickr/dist/themes/classic.min.css';

// Input Mask
import Inputmask from 'inputmask';
window.Inputmask = Inputmask;
globalThis.Inputmask = Inputmask;

// Modern Color Picker (Pickr)
// Pickr uses UMD format - import as namespace and get the class
import * as PickrModule from '@simonwep/pickr';
const Pickr = PickrModule.default || PickrModule;
window.Pickr = Pickr;
globalThis.Pickr = Pickr;

// Cropper.js for image cropping
import Cropper from 'cropperjs';
window.Cropper = Cropper;
globalThis.Cropper = Cropper;

// DataTables (Bootstrap 5 styling) - Modern vanilla JS usage
import DataTable from 'datatables.net-bs5';
import 'datatables.net-responsive-bs5';
import 'datatables.net-buttons-bs5';
import 'datatables.net-buttons/js/buttons.html5.js';
import 'datatables.net-buttons/js/buttons.print.js';
import 'datatables.net-fixedheader';
import 'datatables.net-keytable';

// Required for export functionality
import JSZip from 'jszip';
window.JSZip = JSZip;

// Make DataTable globally available for chart initializer
window.DataTable = DataTable;
globalThis.DataTable = DataTable;

// Modern DataTable initialization
document.addEventListener('DOMContentLoaded', () => {
  const advancedTableEl = document.getElementById('advancedDataTable');
  if (advancedTableEl && !advancedTableEl.dataTableInstance) {
    try {
      const dataTable = new DataTable(advancedTableEl, {
        responsive: true,
        pageLength: 10,
        lengthChange: true,
        lengthMenu: [
          [10, 25, 50, -1],
          [10, 25, 50, 'All']
        ],
        searching: true,
        ordering: true,
        info: true,
        paging: true,
        columnDefs: [
          { orderable: false, targets: [5] },
          { className: 'text-center', targets: [3, 5] }
        ],
        language: {
          search: 'Search invoices:',
          lengthMenu: 'Show _MENU_ invoices per page',
          info: 'Showing _START_ to _END_ of _TOTAL_ invoices',
          paginate: {
            first: 'First',
            last: 'Last',
            next: 'Next',
            previous: 'Previous'
          }
        }
      });
      advancedTableEl.dataTableInstance = dataTable;
    } catch (error) {}
  }
});

// Import table performance optimizer
import './utils/table-optimizer.js';

// Initialize DataTables for other pages
document.addEventListener('DOMContentLoaded', () => {
  if (window.location.pathname.includes('tables.html')) {
    const advancedTable = document.getElementById('advancedDataTable');
    if (advancedTable && !advancedTable.dataTableInstance) {
      try {
        const dataTable = new DataTable(advancedTable, {
          responsive: true,
          pageLength: 10,
          lengthMenu: [
            [5, 10, 25, 50],
            [5, 10, 25, 50]
          ],
          order: [[0, 'asc']],
          language: {
            search: 'Search employees:',
            lengthMenu: 'Show _MENU_ employees per page',
            info: 'Showing _START_ to _END_ of _TOTAL_ employees',
            paginate: {
              first: 'First',
              last: 'Last',
              next: 'Next',
              previous: 'Previous'
            }
          },
          columnDefs: [
            {
              orderable: false,
              targets: [6]
            }
          ]
        });
        advancedTable.dataTableInstance = dataTable;
      } catch (error) {}
    }
  }
});

// DOM utilities are already defined at the top of the file

// Import comprehensive chart initializer
import './chart-initializer.js';

// Widget-specific initialization (jQuery-free)
DOM.ready(() => {
  // The chart initializer handles all chart initialization
  // No need for manual chart initialization here anymore

  // Initialize progress bars (vanilla JS) - keep this as it's not chart-related
  function initProgressBars() {
    const progressBars = DOM.selectAll('.progress .progress-bar');

    progressBars.forEach(bar => {
      if (bar.getAttribute('data-transitiongoal')) {
        return;
      }

      const goal = parseInt(bar.dataset.transitiongoal) || 0;

      if (goal > 0) {
        bar.style.width = '0%';
        bar.style.transition = 'width 1s ease-in-out';

        setTimeout(() => {
          bar.style.width = goal + '%';
        }, 100);
      }
    });
  }

  // Initialize non-chart elements
  initProgressBars();
});

// Universal Progress Bars Initialization (vanilla JS)
function initUniversalProgressBars() {
  const allProgressBars = DOM.selectAll('.progress-bar');

  if (allProgressBars.length > 0) {
    allProgressBars.forEach((bar, index) => {
      if (bar.classList.contains('animation-complete')) {
        return;
      }

      // Skip animation for progress bars inside sales-progress - they already have width set
      if (bar.closest('.sales-progress')) {
        bar.classList.add('animation-complete');
        return;
      }

      let targetWidth = null;
      const transitionGoal = bar.getAttribute('data-transitiongoal');

      if (transitionGoal) {
        targetWidth = transitionGoal + '%';
      } else {
        const inlineWidth = bar.style.width;
        const computedStyle = window.getComputedStyle(bar);
        const currentWidth = inlineWidth || computedStyle.width;

        if (
          currentWidth &&
          currentWidth !== '0px' &&
          currentWidth !== '0%' &&
          currentWidth !== 'auto'
        ) {
          targetWidth = currentWidth;
        }
      }

      if (targetWidth) {
        bar.setAttribute('data-target-width', targetWidth);
        bar.style.setProperty('--bar-width', targetWidth);
        bar.style.width = '0%';
        bar.style.transition = 'width 0.8s ease-out';

        setTimeout(
          () => {
            bar.style.width = targetWidth;
            setTimeout(() => {
              bar.style.transition = 'none';
              bar.style.width = targetWidth;
              bar.classList.add('animation-complete');
            }, 1000);
          },
          index * 100 + 300
        );
      }
    });
  }
}

// '메일 분석 현황' 창: 로그 패널 HTML 동적 주입 (SSE)
function injectLogPanel() {
  if (document.getElementById('logFab')) {
    initLogPanel();
    return;
  }

  const style = document.createElement('style');
  style.textContent = `
    .log-fab { display:inline-flex; align-items:center; gap:8px; padding:7px 16px; border-radius:999px; background:#1a9e6e; color:#fff; font-size:13px; font-weight:500; cursor:pointer; border:none; transition:background .18s,transform .15s; box-shadow:0 2px 10px rgba(26,158,110,0.35); }
    .log-fab:hover { background:#0f7a52; transform:translateY(-1px); }
    .log-fab:active { transform:scale(.97); }
    .fab-dot { width:8px; height:8px; border-radius:50%; background:#fff; opacity:.85; display:inline-block; }
    .fab-dot.running { background:#6effc5; animation:dot-pulse 1.2s ease-in-out infinite; }
    @keyframes dot-pulse { 0%,100%{opacity:.85;transform:scale(1);}50%{opacity:1;transform:scale(1.35);} }
    .fab-badge { position:absolute; top:-5px; right:-5px; background:#e24b4a; color:#fff; font-size:10px; font-weight:500; border-radius:999px; padding:1px 5px; min-width:16px; text-align:center; display:none; }
    .fab-badge.show { display:block; }
    .panel-wrap { position:absolute; top:calc(100% + 10px); right:0; width:480px; border:1px solid #d8ebe3; border-radius:12px; overflow:hidden; background:#fff; z-index:9999; box-shadow:0 8px 32px rgba(26,158,110,0.12); max-height:0; opacity:0; pointer-events:none; transition:max-height .35s cubic-bezier(.22,1,.36,1),opacity .3s; max-width:calc(100vw - 24px); }
    .panel-wrap.open { max-height:480px; opacity:1; pointer-events:all; }
    .panel-header { display:flex; align-items:center; justify-content:space-between; padding:10px 14px; background:#f0f7f3; border-bottom:1px solid #d8ebe3; }
    .panel-title { display:flex; align-items:center; gap:7px; font-size:13px; font-weight:500; color:#1b2e22; }
    .panel-status { font-size:11px; padding:3px 9px; border-radius:999px; font-weight:500; background:rgba(26,158,110,0.12); color:#1a9e6e; display:flex; align-items:center; gap:5px; }
    .panel-status.idle { background:#f4f4f4; color:#888; }
    .panel-status.done { background:#eaf3de; color:#3b6d11; }
    .panel-status.failed { background:#fcebeb; color:#a32d2d; }
    .status-indicator { width:6px; height:6px; border-radius:50%; background:currentColor; display:inline-block; }
    .status-indicator.running { animation:dot-pulse 1.1s ease-in-out infinite; }
    .panel-actions { display:flex; gap:6px; }
    .panel-btn { background:none; border:1px solid #d8ebe3; color:#1a9e6e; border-radius:6px; padding:3px 9px; font-size:11px; cursor:pointer; transition:background .15s; }
    .panel-btn:hover { background:rgba(26,158,110,0.08); }
    .progress-bar-wrap { height:3px; background:#d8ebe3; }
    .progress-bar-fill { height:100%; background:linear-gradient(90deg,#1a9e6e,#34d399); width:0%; transition:width .5s ease; border-radius:0 2px 2px 0; }
    .log-body { height:260px; overflow-y:auto; padding:10px 14px; font-size:12px; line-height:1.7; scrollbar-width:thin; scrollbar-color:#d8ebe3 transparent; }
    .log-body::-webkit-scrollbar { width:4px; }
    .log-body::-webkit-scrollbar-thumb { background:#d8ebe3; border-radius:4px; }
    .log-line { display:flex; gap:6px; align-items:flex-start; padding:2px 0; animation:line-in .2s ease; min-width:0; width:100%; }
    @keyframes line-in { from{opacity:0;transform:translateY(4px);}to{opacity:1;transform:none;} }
    .log-ts { color:#b0c4bc; flex-shrink:0; font-size:11px; padding-top:1px; }
    .log-tag { flex-shrink:0; font-size:10px; font-weight:500; padding:1px 6px; border-radius:4px; min-width:36px; text-align:center; white-space:nowrap; display:inline-block; overflow:visible; height:auto; }
    .log-tag.running { background:#e1f5ee; color:#0f6e56; }
    .log-tag.done { background:#eaf3de; color:#3b6d11; }
    .log-tag.failed { background:#fcebeb; color:#a32d2d; }
    .log-msg { color:#333; word-break:break-word; min-width:0; overflow-wrap:anywhere; }
    .log-pct { color:#1a9e6e; font-weight:500; margin-left:4px; }
    .log-empty { height:100%; display:flex; align-items:center; justify-content:center; color:#aaa; font-size:12px; flex-direction:column; gap:6px; }
    .log-empty svg { opacity:.35; }
    .panel-footer { padding:8px 14px; border-top:1px solid #d8ebe3; display:flex; align-items:center; justify-content:space-between; background:#f9fcfb; }
    .footer-count { font-size:11px; color:#888; }
    .footer-scroll-btn { font-size:11px; color:#1a9e6e; cursor:pointer; background:none; border:none; display:flex; align-items:center; gap:4px; }
    .footer-scroll-btn:hover { text-decoration:underline; }
  `;
  document.head.appendChild(style);

  const targetUl = document.querySelector('ul.navbar-right');
  if (!targetUl) return;

  const li = document.createElement('li');
  li.id = 'log-fab-wrap';
  li.style.position = 'relative';
  li.innerHTML = `
    <button class="log-fab" id="logFab">
      <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 16 16">
        <rect x="2" y="3" width="12" height="10" rx="2"/>
        <path d="M5 6h6M5 9h4"/>
      </svg>
      메일 분석 현황
      <span class="fab-dot" id="fabDot"></span>
    </button>
    <span class="fab-badge" id="fabBadge">0</span>
    <div class="panel-wrap" id="logPanel">
      <div class="panel-header">
        <div class="panel-title">
          <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 16 16">
            <rect x="2" y="3" width="12" height="10" rx="2"/>
            <path d="M5 6h6M5 9h4"/>
          </svg>
          메일 분석 현황
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="panel-status idle" id="panelStatus">
            <span class="status-indicator" id="statusDot"></span>
            <span id="statusText">대기 중</span>
          </span>
          <div class="panel-actions">
            <button class="panel-btn" id="clearBtn">지우기</button>
            <button class="panel-btn" id="closeBtn">닫기</button>
          </div>
        </div>
      </div>
      <div class="progress-bar-wrap">
        <div class="progress-bar-fill" id="progressFill"></div>
      </div>
      <div class="log-body" id="logBody">
        <div class="log-empty" id="logEmpty">
          <svg width="28" height="28" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
            <rect x="3" y="5" width="18" height="14" rx="2"/>
            <path d="M7 9h10M7 13h6"/>
          </svg>
          분석 시작 시 로그가 표시됩니다.
        </div>
      </div>
      <div class="panel-footer">
        <span class="footer-count" id="footerCount">0 줄</span>
        <button class="footer-scroll-btn" id="scrollBottomBtn">
          <svg width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 12 12">
            <path d="M2 4l4 4 4-4"/>
          </svg>
          맨 아래로
        </button>
      </div>
    </div>
  `;

  const langLi = targetUl.querySelector('.nav-item.dropdown');
  targetUl.insertBefore(li, langLi);

  initLogPanel();
}

function initLogPanel() {
  const logBody = document.getElementById('logBody');
  const logEmpty = document.getElementById('logEmpty');
  const progressFill = document.getElementById('progressFill');
  const panelStatus = document.getElementById('panelStatus');
  const statusDot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  const footerCount = document.getElementById('footerCount');
  const fabDot = document.getElementById('fabDot');
  const fabBadge = document.getElementById('fabBadge');
  const logPanel = document.getElementById('logPanel');

  let lineCount = 0;
  let unreadCount = 0;
  let isPanelOpen = false;
  let autoScroll = true;
  let currentView = 'user'; // 'user' | 'dev'

  // ── 사용자 메시지 매핑 ──────────────────────────────────
  const STEPS = [
    { key: '메일 데이터 불러오기', match: ['청크 분할', '데이터 로드'] },
    { key: '텍스트 분석', match: ['텍스트 정제', '문서 처리'] },
    { key: '핵심 내용 추출', match: ['임베딩', '엔티티'] },
    { key: '관계 파악', match: ['그래프', '커뮤니티', '관계'] },
    { key: '검색 준비 완료', match: ['인덱스 저장', '완료'] }
  ];

  function toUserMessage(msg) {
    const map = [
      { match: '청크 분할', text: '메일 데이터를 불러오고 있어요', stepIdx: 0 },
      { match: '데이터 로드', text: '메일 데이터를 불러오고 있어요', stepIdx: 0 },
      { match: '텍스트 정제', text: '내용을 정리하고 있어요', stepIdx: 1 },
      { match: '문서 처리', text: '내용을 정리하고 있어요', stepIdx: 1 },
      { match: '임베딩', text: '핵심 내용을 분석하고 있어요', stepIdx: 2 },
      { match: '엔티티', text: '핵심 내용을 분석하고 있어요', stepIdx: 2 },
      { match: '그래프', text: '메일 간 관계를 파악하고 있어요', stepIdx: 3 },
      { match: '커뮤니티', text: '연관 메일을 그룹으로 묶고 있어요', stepIdx: 3 },
      { match: '관계', text: '메일 간 관계를 파악하고 있어요', stepIdx: 3 },
      { match: '인덱스 저장', text: '검색을 준비하고 있어요', stepIdx: 4 },
      { match: '완료', text: '분석이 완료됐어요!', stepIdx: 4 },
      { match: '오류', text: '문제가 발생했어요. 다시 시도해주세요', stepIdx: -1 }
    ];
    const found = map.find(m => msg.includes(m.match));
    return found || { text: msg, stepIdx: -1 };
  }

  // ── 사용자 뷰 HTML 주입 ─────────────────────────────────
  function buildUserView() {
    const cards = [
      { id: 0, title: '메일 읽기', sub: '받은 메일을 모두 가져왔어요' },
      { id: 1, title: '내용 이해하기', sub: '메일의 핵심 내용을 파악해요' },
      { id: 2, title: '연결 관계 분석', sub: '메일들이 어떻게 연결됐는지 봐요' },
      { id: 3, title: '검색 기능 켜기', sub: '곧 검색할 수 있게 준비해요' }
    ];

    return `
    <div id="userView" style="background:#fff;border-radius:12px;overflow:hidden;">
      <div style="background:#f0f7f3;padding:16px 18px 14px;border-bottom:0.5px solid #d8ebe3;display:flex;flex-direction:column;gap:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;">
          <div style="display:flex;align-items:center;gap:5px;background:#d8ebe3;border-radius:999px;padding:3px 10px;">
            <div id="uvPillDot" style="width:6px;height:6px;border-radius:50%;background:#1a9e6e;"></div>
            <span id="uvPillText" style="font-size:11px;font-weight:500;color:#0f6e56;">메일 분석 중</span>
          </div>
          <button id="uvClose" style="font-size:11px;color:#b0c4bc;background:none;border:none;cursor:pointer;padding:0;">닫기</button>
        </div>
        <div style="display:flex;align-items:flex-end;gap:10px;">
          <span id="uvPct" style="font-size:52px;font-weight:500;color:#1b2e22;line-height:1;">0%</span>
          <div style="display:flex;flex-direction:column;justify-content:flex-end;padding-bottom:5px;gap:3px;">
            <span id="uvMsg" style="font-size:13px;font-weight:500;color:#1b2e22;">분석 준비 중</span>
            <span id="uvSub" style="font-size:11px;color:#73879C;">잠시만 기다려주세요</span>
          </div>
        </div>
      </div>

      <div style="height:4px;background:#d8ebe3;">
        <div id="uvBar" style="height:100%;background:#1a9e6e;width:0%;transition:width .5s ease;"></div>
      </div>

      <div style="padding:12px 14px;display:flex;flex-direction:column;gap:7px;">
        ${cards
          .map(
            c => `
          <div id="uvCard${c.id}" style="background:#f9fcfb;border:0.5px solid #e1f5ee;border-radius:10px;padding:11px 13px;display:flex;align-items:center;gap:11px;">
            <div id="uvCardIcon${c.id}" style="width:32px;height:32px;border-radius:8px;background:#f4f4f4;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:background .3s;">
              <svg width="14" height="14" fill="none" stroke="#ccc" stroke-width="2" viewBox="0 0 14 14" id="uvCardIconSvg${c.id}"><circle cx="7" cy="7" r="5"/></svg>
            </div>
            <div style="flex:1;min-width:0;">
              <div id="uvCardTitle${c.id}" style="font-size:12px;font-weight:500;color:#aaa;margin-bottom:1px;transition:color .3s;">${c.title}</div>
              <div id="uvCardSub${c.id}" style="font-size:10px;color:#ccc;transition:color .3s;">${c.sub}</div>
            </div>
            <span id="uvCardBadge${c.id}" style="font-size:10px;padding:2px 7px;border-radius:999px;font-weight:500;white-space:nowrap;background:#f4f4f4;color:#bbb;transition:all .3s;">대기 중</span>
          </div>
        `
          )
          .join('')}
      </div>
    </div>
  `;
  }

  // ── 사용자 뷰 업데이트 ──────────────────────────────────
  function updateUserView(pct, stepIdx, msg, type) {
    const bar = document.getElementById('uvBar');
    const pctEl = document.getElementById('uvPct');
    const msgEl = document.getElementById('uvMsg');
    const subEl = document.getElementById('uvSub');
    const pillDot = document.getElementById('uvPillDot');
    const pillText = document.getElementById('uvPillText');
    if (!bar) return;

    bar.style.width = Math.round(pct) + '%';
    pctEl.textContent = Math.round(pct) + '%';

    const svgDone = `<svg width="14" height="14" fill="none" stroke="#fff" stroke-width="2.5" viewBox="0 0 14 14"><path d="M2 7l3.5 3.5L12 3"/></svg>`;
    const svgActive = `<svg width="14" height="14" fill="none" stroke="#1a9e6e" stroke-width="2" viewBox="0 0 14 14"><circle cx="7" cy="7" r="5"/><path d="M7 4v3l2 1.5"/></svg>`;
    const svgWait = `<svg width="14" height="14" fill="none" stroke="#ccc" stroke-width="2" viewBox="0 0 14 14"><circle cx="7" cy="7" r="5"/></svg>`;

    // 카드 매핑 (stepIdx → 카드 인덱스)
    // progress stepIdx: 0~1 → card 0, 2 → card 1, 3 → card 2, 4 → card 3
    const stepToCard = [0, 0, 1, 2, 3];
    const cardIdx = stepIdx >= 0 ? (stepToCard[stepIdx] ?? stepIdx) : -1;

    const msgs = [
      '시작됐어요! 메일을 읽고 있어요',
      '내용을 이해하고 있어요',
      '연결 관계를 분석하고 있어요',
      '검색 기능을 켜고 있어요'
    ];
    const subs = [
      '받은 메일을 하나씩 가져오고 있어요',
      '중요한 내용을 뽑아내고 있어요',
      '메일들이 어떻게 이어지는지 봐요',
      '곧 검색할 수 있어요'
    ];

    if (type === 'done') {
      pctEl.style.color = '#1a9e6e';
      bar.style.background = '#1a9e6e';
      msgEl.textContent = '분석 완료!';
      subEl.textContent = '이제 메일을 검색할 수 있어요';
      pillDot.style.background = '#1a9e6e';
      pillText.textContent = '분석 완료';
      pillText.style.color = '#0f6e56';
      for (let i = 0; i < 4; i++) {
        const icon = document.getElementById(`uvCardIcon${i}`);
        const title = document.getElementById(`uvCardTitle${i}`);
        const sub = document.getElementById(`uvCardSub${i}`);
        const badge = document.getElementById(`uvCardBadge${i}`);
        if (!icon) continue;
        icon.style.background = '#1a9e6e';
        icon.innerHTML = svgDone;
        title.style.color = '#1b2e22';
        sub.style.color = '#73879C';
        badge.textContent = '완료';
        badge.style.background = '#eaf3de';
        badge.style.color = '#3b6d11';
      }
    } else if (type === 'failed') {
      pctEl.style.color = '#e24b4a';
      bar.style.background = '#e24b4a';
      msgEl.textContent = '문제가 발생했어요';
      subEl.textContent = '잠시 후 다시 시도해주세요';
      pillDot.style.background = '#e24b4a';
      pillText.textContent = '오류 발생';
      pillText.style.color = '#a32d2d';
    } else {
      msgEl.textContent = cardIdx >= 0 ? msgs[cardIdx] : msg;
      subEl.textContent = cardIdx >= 0 ? subs[cardIdx] : '';

      for (let i = 0; i < 4; i++) {
        const icon = document.getElementById(`uvCardIcon${i}`);
        const title = document.getElementById(`uvCardTitle${i}`);
        const sub = document.getElementById(`uvCardSub${i}`);
        const badge = document.getElementById(`uvCardBadge${i}`);
        if (!icon) continue;
        if (i < cardIdx) {
          icon.style.background = '#1a9e6e';
          icon.innerHTML = svgDone;
          title.style.color = '#1b2e22';
          sub.style.color = '#73879C';
          badge.textContent = '완료';
          badge.style.background = '#eaf3de';
          badge.style.color = '#3b6d11';
        } else if (i === cardIdx) {
          icon.style.background = '#e1f5ee';
          icon.innerHTML = svgActive;
          title.style.color = '#1b2e22';
          title.style.fontWeight = '500';
          sub.style.color = '#73879C';
          badge.textContent = '진행 중';
          badge.style.background = '#e1f5ee';
          badge.style.color = '#0f6e56';
        } else {
          icon.style.background = '#f4f4f4';
          icon.innerHTML = svgWait;
          title.style.color = '#aaa';
          title.style.fontWeight = '400';
          sub.style.color = '#ccc';
          badge.textContent = '대기 중';
          badge.style.background = '#f4f4f4';
          badge.style.color = '#bbb';
        }
      }
    }
  }

  // ── pulse 애니메이션 CSS 주입 ───────────────────────────
  if (!document.getElementById('uvPulseStyle')) {
    const s = document.createElement('style');
    s.id = 'uvPulseStyle';
    s.textContent = `@keyframes uvPulse{0%,100%{opacity:1;}50%{opacity:.3;}}`;
    document.head.appendChild(s);
  }

  // ── 드롭다운 HTML 주입 ──────────────────────────────────
  function buildDropdown() {
    const dd = document.createElement('div');
    dd.id = 'logDropdown';
    dd.style.cssText = `
      position:absolute; top:calc(100% + 6px); right:0;
      background:#fff; border:1px solid #d8ebe3; border-radius:10px;
      overflow:hidden; z-index:10000; min-width:150px;
      box-shadow:0 4px 16px rgba(26,158,110,0.12);
    `;
    dd.innerHTML = `
      <div id="ddUser" style="display:flex;align-items:center;gap:9px;padding:10px 14px;cursor:pointer;font-size:12px;color:#1b2e22;border-bottom:1px solid #f0f7f3;transition:background .15s;">
        <div style="width:22px;height:22px;border-radius:6px;background:#e1f5ee;display:flex;align-items:center;justify-content:center;">
          <svg width="12" height="12" fill="none" stroke="#0f6e56" stroke-width="2" viewBox="0 0 16 16"><circle cx="8" cy="6" r="3"/><path d="M2 14c0-3 2.7-5 6-5s6 2 6 5"/></svg>
        </div>
        사용자 보기
      </div>
      <div id="ddDev" style="display:flex;align-items:center;gap:9px;padding:10px 14px;cursor:pointer;font-size:12px;color:#1b2e22;transition:background .15s;">
        <div style="width:22px;height:22px;border-radius:6px;background:#e8f0fe;display:flex;align-items:center;justify-content:center;">
          <svg width="12" height="12" fill="none" stroke="#185fa5" stroke-width="2" viewBox="0 0 16 16"><rect x="2" y="3" width="12" height="10" rx="2"/><path d="M5 6h6M5 9h4"/></svg>
        </div>
        개발자 로그
      </div>
    `;
    return dd;
  }

  // ── 뷰 전환 ─────────────────────────────────────────────
  function switchView(view) {
    currentView = view;
    const panelHeader = logPanel.querySelector('.panel-header');
    const progressWrap = logPanel.querySelector('.progress-bar-wrap');
    const panelFooter = document.getElementById('panel-footer-wrap');
    let userView = document.getElementById('userView');

    if (view === 'user') {
      if (!userView) {
        const wrap = document.createElement('div');
        wrap.innerHTML = buildUserView();
        logPanel.insertBefore(wrap.firstElementChild, logBody);
        document.getElementById('uvClose').addEventListener('click', () => {
          isPanelOpen = false;
          logPanel.classList.remove('open');
        });
      }
      document.getElementById('userView').style.display = '';
      panelHeader.style.display = 'none';
      progressWrap.style.display = 'none';
      logBody.style.display = 'none';
      if (panelFooter) panelFooter.style.display = 'none';
    } else {
      if (userView) userView.style.display = 'none';
      panelHeader.style.display = '';
      progressWrap.style.display = '';
      logBody.style.display = '';
      if (panelFooter) panelFooter.style.display = '';
    }
  }

  // panel-footer 에 id 추가
  const panelFooter = logPanel.querySelector('.panel-footer');
  if (panelFooter) panelFooter.id = 'panel-footer-wrap';

  // ── logFab 클릭 → 드롭다운 토글 ────────────────────────
  const fabWrap = document.getElementById('log-fab-wrap');
  document.getElementById('logFab').addEventListener('click', e => {
    e.stopPropagation();
    const existing = document.getElementById('logDropdown');
    if (existing) {
      existing.remove();
      return;
    }
    // 패널 열려있으면 닫기
    if (isPanelOpen) {
      isPanelOpen = false;
      logPanel.classList.remove('open');
    }
    const dd = buildDropdown();
    fabWrap.appendChild(dd);

    dd.querySelector('#ddUser').addEventListener('click', () => {
      dd.remove();
      isPanelOpen = true;
      logPanel.classList.add('open');
      unreadCount = 0;
      fabBadge.classList.remove('show');
      switchView('user');
    });
    dd.querySelector('#ddDev').addEventListener('click', () => {
      dd.remove();
      isPanelOpen = true;
      logPanel.classList.add('open');
      unreadCount = 0;
      fabBadge.classList.remove('show');
      switchView('dev');
    });

    // 바깥 클릭 시 드롭다운 닫기
    setTimeout(() => {
      document.addEventListener('click', function closeDd() {
        document.getElementById('logDropdown')?.remove();
        document.removeEventListener('click', closeDd);
      });
    }, 0);
  });

  document.getElementById('closeBtn').addEventListener('click', () => {
    isPanelOpen = false;
    logPanel.classList.remove('open');
  });
  document.getElementById('clearBtn').addEventListener('click', () => {
    logBody.innerHTML = '';
    logBody.appendChild(logEmpty);
    logEmpty.style.display = '';
    lineCount = 0;
    footerCount.textContent = '0 줄';
    progressFill.style.width = '0%';
    setStatus('idle');
    // 사용자 뷰 초기화
    document.getElementById('uvPct') && (document.getElementById('uvPct').textContent = '0%');
    const bar = document.getElementById('uvBar');
    if (bar) {
      bar.style.width = '0%';
      bar.style.background = '#1a9e6e';
    }
    const pctEl2 = document.getElementById('uvPct');
    if (pctEl2) {
      pctEl2.textContent = '0%';
      pctEl2.style.color = '#1a9e6e';
    }
  });
  document.getElementById('scrollBottomBtn').addEventListener('click', () => {
    logBody.scrollTop = logBody.scrollHeight;
  });
  logBody.addEventListener('scroll', () => {
    autoScroll = logBody.scrollHeight - logBody.scrollTop - logBody.clientHeight < 40;
  });

  // ── 공통 함수 ───────────────────────────────────────────
  function ts() {
    const n = new Date();
    return [n.getHours(), n.getMinutes(), n.getSeconds()]
      .map(v => String(v).padStart(2, '0'))
      .join(':');
  }

  function addLogLine(type, msg, pct) {
    if (logEmpty) logEmpty.style.display = 'none';
    const line = document.createElement('div');
    line.className = 'log-line';
    const tagClass =
      type === 'progress'
        ? 'running'
        : type === 'done'
          ? 'done'
          : type === 'failed'
            ? 'failed'
            : 'info';
    const tagLabel =
      type === 'progress' ? '진행' : type === 'done' ? '완료' : type === 'failed' ? '실패' : '정보';
    const pctText = pct != null ? `<span class="log-pct">(${pct}%)</span>` : '';
    line.innerHTML = `<span class="log-ts">${ts()}</span><span class="log-tag ${tagClass}">${tagLabel}</span><span class="log-msg">${msg}${pctText}</span>`;
    logBody.appendChild(line);
    lineCount++;
    footerCount.textContent = lineCount + ' 줄';
    if (!isPanelOpen) {
      unreadCount++;
      fabBadge.textContent = unreadCount;
      fabBadge.classList.add('show');
    }
    if (autoScroll) logBody.scrollTop = logBody.scrollHeight;
  }

  function setStatus(type) {
    panelStatus.className = 'panel-status ' + type;
    statusDot.className = 'status-indicator' + (type === 'running' ? ' running' : '');
    const labels = { idle: '대기 중', running: '분석 중', done: '완료', failed: '실패' };
    statusText.textContent = labels[type] || type;
    fabDot.classList.toggle('running', type === 'running');
  }

  // ── SSE ─────────────────────────────────────────────────
  fetch('/indexing-history')
    .then(r => r.json())
    .then(history => {
      history.forEach(data => handleData(data));
    })
    .catch(() => {});

  const es = new EventSource('/indexing-stream');
  es.onmessage = function (e) {
    try {
      handleData(JSON.parse(e.data));
    } catch (_) {}
  };
  es.onerror = function () {
    addLogLine('failed', 'SSE 연결 끊김 - 서버를 확인해 주세요.', null);
    setStatus('failed');
  };

  function handleData(data) {
    if (data.type === 'progress') {
      addLogLine('progress', data.message, data.progress);
      progressFill.style.width = (data.progress ?? 0) + '%';
      setStatus('running');
      const mapped = toUserMessage(data.message);
      updateUserView(data.progress ?? 0, mapped.stepIdx, mapped.text, 'progress');
    } else if (data.type === 'done') {
      addLogLine('done', data.message, null);
      progressFill.style.width = '100%';
      setStatus('done');
      updateUserView(100, 4, '분석이 완료됐어요!', 'done');
    } else if (data.type === 'failed') {
      addLogLine('failed', data.message, null);
      setStatus('failed');
      updateUserView(0, -1, '문제가 발생했어요', 'failed');
    }
  }

  // ── 초기 사용자 뷰 삽입 (기본값) ────────────────────────
  switchView('user');
}

document.addEventListener('DOMContentLoaded', injectLogPanel);
// Initialize universal progress bars on DOM ready
DOM.ready(() => {
  setTimeout(initUniversalProgressBars, 200);
});

// Import essential JavaScript functionality - modern versions
import './js/helpers/smartresize.js';
import './js/sidebar.js';
import './js/init.js';

// Import weather and maps modules for index.html
import './modules/weather.js';
import './modules/maps.js';

// Import echarts module for echarts.html
import './modules/echarts.js';

// 다국어 지원 (i18n)
import './utils/i18n.js';
