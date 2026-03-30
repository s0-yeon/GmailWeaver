import i18next from 'i18next';
import ko from '../i18n/ko.json';
import en from '../i18n/en.json';
import ja from '../i18n/ja.json';

const LANG_KEY = 'gw_lang';
const SUPPORTED = ['ko', 'en', 'ja'];

// 저장된 언어 or 브라우저 기본 언어 감지
function detectLang() {
  const saved = localStorage.getItem(LANG_KEY);
  if (saved && SUPPORTED.includes(saved)) return saved;
  const browser = (navigator.language || 'ko').split('-')[0];
  return SUPPORTED.includes(browser) ? browser : 'ko';
}

// i18next 초기화
await i18next.init({
  lng: detectLang(),
  fallbackLng: 'ko',
  resources: {
    ko: { translation: ko },
    en: { translation: en },
    ja: { translation: ja },
  },
  interpolation: { escapeValue: false },
});

// 단축 번역 함수 (전역 노출)
export function t(key, opts) {
  return i18next.t(key, opts);
}
window.t = t;

// DOM 요소에 번역 적용
// data-i18n="key"              → textContent
// data-i18n-html="key"         → innerHTML (DOMPurify로 보호)
// data-i18n-placeholder="key"  → placeholder 속성
// data-i18n-title="key"        → title 속성
// data-i18n-aria-label="key"   → aria-label 속성
export function applyTranslations(root = document) {
  root.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    const val = i18next.t(key);
    if (val && val !== key) el.textContent = val;
  });

  root.querySelectorAll('[data-i18n-html]').forEach(el => {
    const key = el.getAttribute('data-i18n-html');
    const val = i18next.t(key);
    if (val && val !== key) {
      // DOMPurify가 있으면 사용, 없으면 textContent fallback
      if (typeof window.DOMPurify !== 'undefined') {
        el.innerHTML = window.DOMPurify.sanitize(val);
      } else {
        el.textContent = val;
      }
    }
  });

  root.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    const key = el.getAttribute('data-i18n-placeholder');
    const val = i18next.t(key);
    if (val && val !== key) el.setAttribute('placeholder', val);
  });

  root.querySelectorAll('[data-i18n-title]').forEach(el => {
    const key = el.getAttribute('data-i18n-title');
    const val = i18next.t(key);
    if (val && val !== key) el.setAttribute('title', val);
  });

  root.querySelectorAll('[data-i18n-aria-label]').forEach(el => {
    const key = el.getAttribute('data-i18n-aria-label');
    const val = i18next.t(key);
    if (val && val !== key) el.setAttribute('aria-label', val);
  });

  // html lang 속성도 업데이트
  document.documentElement.lang = i18next.language;
}

// 언어 전환 (페이지 새로고침 없이 즉시 적용)
export async function changeLanguage(lang) {
  if (!SUPPORTED.includes(lang)) return;
  localStorage.setItem(LANG_KEY, lang);
  await i18next.changeLanguage(lang);
  applyTranslations();
  updateLangSwitcher(lang);
  // greeting 동적 업데이트 (이름 포함)
  updateGreeting();
}
window.changeLanguage = changeLanguage;

// 언어 스위처 UI 현재 언어 표시 업데이트
function updateLangSwitcher(lang) {
  const labels = { ko: 'KO', en: 'EN', ja: 'JA' };
  const el = document.getElementById('current-lang');
  if (el) el.textContent = labels[lang] || lang.toUpperCase();
}

// 홈 인사말 동적 업데이트 (이름 포함)
function updateGreeting() {
  const greetingEl = document.getElementById('home-greeting');
  if (!greetingEl) return;
  const name = sessionStorage.getItem('gw_user_name');
  if (name && name !== '-') {
    greetingEl.textContent = name + (i18next.language === 'ja' ? 'さん、' : i18next.language === 'en' ? ', ' : '님, ') + i18next.t('page.index.greeting');
  } else {
    greetingEl.textContent = i18next.t('page.index.greeting');
  }
}

// DOM 준비 시 자동 번역 적용
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    applyTranslations();
    updateLangSwitcher(i18next.language);
    updateGreeting();
    initLangSwitcher();
  });
} else {
  applyTranslations();
  updateLangSwitcher(i18next.language);
  updateGreeting();
  initLangSwitcher();
}

// 언어 스위처 클릭 이벤트 초기화
function initLangSwitcher() {
  document.querySelectorAll('.lang-option').forEach(el => {
    el.addEventListener('click', e => {
      e.preventDefault();
      changeLanguage(e.currentTarget.dataset.lang);
    });
  });
}
