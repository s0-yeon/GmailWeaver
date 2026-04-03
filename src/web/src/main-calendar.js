// apps-script/web/main-calendar.js

import './utils/i18n.js';
import DOMPurify from 'dompurify';
import * as bootstrap from 'bootstrap';
window.bootstrap = bootstrap;
globalThis.bootstrap = bootstrap;

import './main.scss';
import './js/helpers/smartresize.js';
import './js/sidebar.js';
import './js/init.js';

// FullCalendar 핵심 + 플러그인 import
import { Calendar } from '@fullcalendar/core';
import dayGridPlugin from '@fullcalendar/daygrid';
import interactionPlugin from '@fullcalendar/interaction';
import timeGridPlugin from '@fullcalendar/timegrid';

// 전역 노출 (HTML 인라인 스크립트에서 접근 가능하도록)
window.FullCalendar = { Calendar, dayGridPlugin, interactionPlugin, timeGridPlugin };
globalThis.FullCalendar = { Calendar, dayGridPlugin, interactionPlugin, timeGridPlugin };

// 현재 캘린더 인스턴스 / 선택된 이벤트 전역 상태
let currentCalendar = null;
let selectedEvent = null;

// Date 객체 → datetime-local input 형식 (YYYY-MM-DDTHH:mm) 변환
function formatDateForInput(date) {
  if (!date) return '';
  const d = new Date(date);
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const hours = String(d.getHours()).padStart(2, '0');
  const minutes = String(d.getMinutes()).padStart(2, '0');
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

// 이벤트 고유 ID 생성 (타임스탬프 + 랜덤 문자열)
function generateEventId() {
  return 'event_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

// FullCalendar 초기화 및 렌더링
function initCalendar() {
  const calendarEl = document.getElementById('calendar');

  if (calendarEl) {
    currentCalendar = new Calendar(calendarEl, {
      plugins: [dayGridPlugin, timeGridPlugin, interactionPlugin],
      initialView: 'dayGridMonth',
      headerToolbar: {
        left: 'prev,next today',
        center: 'title',
        right: 'dayGridMonth,timeGridWeek,timeGridDay'
      },
      selectable: true,
      selectMirror: true,
      dayMaxEvents: true,
      weekends: true,
      editable: true,
      droppable: true,
      height: 'auto',

      // 이벤트 데이터 fetch (세션 캐시 우선, 없으면 Flask API 호출)
      events: function(fetchInfo, successCallback, failureCallback) {
        const cacheKey = `gw_cal_${fetchInfo.startStr}_${fetchInfo.endStr}`;
        const cached = sessionStorage.getItem(cacheKey);
        
        // 캐시 있으면 즉시 반환
        if (cached) {
          successCallback(JSON.parse(cached));
          return;
        }

        // 캐시 없으면 API 호출
        fetch('/calendar-events', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'getEvents',
            start: fetchInfo.startStr,
            end: fetchInfo.endStr
          })
        })
        .then(res => res.json())
        .then(data => {
          const events = data.events || [];
          // 캐시 저장
          sessionStorage.setItem(cacheKey, JSON.stringify(events));
          successCallback(events);
        })
        .catch(err => failureCallback(err));
      },

      // 날짜 범위 선택 시 새 이벤트 모달 오픈
      select: function(selectInfo) {
        openNewEventModal(selectInfo);
      },

      // 이벤트 클릭 시 상세 보기 모달 오픈
      eventClick: function(eventClickInfo) {
        selectedEvent = eventClickInfo.event;
        showEventDetails(eventClickInfo.event);
      },

      // 이벤트 마운트 시 툴팁 등록(마우스를 올렸을 때 뜨는 작은 설명 박스)
      eventDidMount: function(info) {
        info.el.setAttribute('title', info.event.title);
        if (info.event.extendedProps.description) {
          info.el.setAttribute('data-bs-toggle', 'tooltip');
          info.el.setAttribute('data-bs-title', info.event.extendedProps.description);
        }
      }
    });

    currentCalendar.render();
    // 외부 접근용 전역 노출
    window.calendar = currentCalendar;
    globalThis.calendar = currentCalendar;

    // 동적으로 추가된 툴팁 초기화
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    [...tooltipTriggerList].map(el => new bootstrap.Tooltip(el));
  }

  setupModalHandlers();
}

// DOM 준비 여부에 따라 initCalendar 실행 시점 결정
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initCalendar);
} else {
  initCalendar();
}

// 새 이벤트 생성 모달 오픈 (선택 범위 있으면 날짜 자동 입력)
function openNewEventModal(selectInfo) {
  const modal = new bootstrap.Modal(document.getElementById('CalenderModalNew'));
  if (selectInfo) {
    document.getElementById('eventStartDate').value = formatDateForInput(selectInfo.start);
    if (selectInfo.end) {
      document.getElementById('eventEndDate').value = formatDateForInput(selectInfo.end);
    }
    document.getElementById('allDayEvent').checked = selectInfo.allDay;
  }
  document.getElementById('newEventForm').reset();
  document.getElementById('eventColor').value = '#26B99A';
  modal.show();
}

// 이벤트 상세 정보 모달 표시 (DOMPurify로 XSS 방지)
function showEventDetails(event) {
  const modal = new bootstrap.Modal(document.getElementById('EventDetailsModal'));
  const contentEl = document.getElementById('eventDetailsContent');

  const startDate = event.start ? event.start.toLocaleDateString() : 'Not specified';
  const startTime = event.start && !event.allDay ? event.start.toLocaleTimeString() : '';
  const endDate = event.end ? event.end.toLocaleDateString() : '';
  const endTime = event.end && !event.allDay ? event.end.toLocaleTimeString() : '';

  // 사용자 입력값 살균 처리(위험한 입력값 제거)
  const safeTitle = DOMPurify.sanitize(event.title || '');
  const safeDescription = event.extendedProps.description ? DOMPurify.sanitize(event.extendedProps.description) : '';
  const safeLocation = event.extendedProps.location ? DOMPurify.sanitize(event.extendedProps.location) : '';
  const safeCategory = event.extendedProps.category ? DOMPurify.sanitize(event.extendedProps.category) : '';

  const eventDetailsHtml = `
    <div class="row mb-3"><div class="col-md-3"><strong>Title:</strong></div><div class="col-md-9">${safeTitle}</div></div>
    <div class="row mb-3"><div class="col-md-3"><strong>Start:</strong></div><div class="col-md-9">${startDate} ${startTime}</div></div>
    ${event.end ? `<div class="row mb-3"><div class="col-md-3"><strong>End:</strong></div><div class="col-md-9">${endDate} ${endTime}</div></div>` : ''}
    ${safeDescription ? `<div class="row mb-3"><div class="col-md-3"><strong>Description:</strong></div><div class="col-md-9">${safeDescription}</div></div>` : ''}
    ${safeLocation ? `<div class="row mb-3"><div class="col-md-3"><strong>Location:</strong></div><div class="col-md-9">${safeLocation}</div></div>` : ''}
    ${safeCategory ? `<div class="row mb-3"><div class="col-md-3"><strong>Category:</strong></div><div class="col-md-9"><span class="badge bg-secondary">${safeCategory}</span></div></div>` : ''}
  `;

  contentEl.innerHTML = DOMPurify.sanitize(eventDetailsHtml);
  modal.show();
}

// 이벤트 수정 모달 오픈 (기존 값 자동 입력)
function openEditEventModal(event) {
  const modal = new bootstrap.Modal(document.getElementById('CalenderModalEdit'));
  document.getElementById('editEventTitle').value = event.title || '';
  document.getElementById('editEventColor').value = event.backgroundColor || '#26B99A';
  document.getElementById('editEventStartDate').value = formatDateForInput(event.start);
  document.getElementById('editEventEndDate').value = formatDateForInput(event.end);
  document.getElementById('editAllDayEvent').checked = event.allDay || false;
  document.getElementById('editEventDescription').value = event.extendedProps.description || '';
  document.getElementById('editEventLocation').value = event.extendedProps.location || '';
  document.getElementById('editEventCategory').value = event.extendedProps.category || '';
  modal.show();
}

// 모달 버튼 이벤트 핸들러 일괄 등록
function setupModalHandlers() {
  // 새 이벤트 저장: 캘린더에 즉시 반영 후 Flask → Google Calendar로 전송
  document.getElementById('saveNewEvent').addEventListener('click', function() {
    const form = document.getElementById('newEventForm');
    if (form.checkValidity()) {
      const formData = new FormData(form);
      const eventData = {
        id: generateEventId(),
        title: formData.get('title'),
        start: formData.get('start'),
        end: formData.get('end'),
        allDay: formData.has('allDay'),
        backgroundColor: formData.get('color'),
        borderColor: formData.get('color'),
        description: formData.get('description'),
        location: formData.get('location'),
        category: formData.get('category')
      };

      currentCalendar.addEvent(eventData);

      fetch('/calendar-events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'addEvent',
          title: eventData.title,
          start: eventData.start,
          end: eventData.end,
          description: eventData.description || ''
        })
      })
      .then(res => res.json())
      .then(data => { if (!data.ok) showToast('구글 캘린더 저장 실패', 'error'); });

      bootstrap.Modal.getInstance(document.getElementById('CalenderModalNew')).hide();
      showToast('Event created successfully!', 'success');
    } else {
      form.classList.add('was-validated');
    }
  });

  // 이벤트 수정 저장: FullCalendar 인스턴스 직접 업데이트
  document.getElementById('saveEditEvent').addEventListener('click', function() {
    if (selectedEvent) {
      const form = document.getElementById('editEventForm');
      if (form.checkValidity()) {
        const formData = new FormData(form);
        selectedEvent.setProp('title', formData.get('title'));
        selectedEvent.setProp('backgroundColor', formData.get('color'));
        selectedEvent.setProp('borderColor', formData.get('color'));
        selectedEvent.setStart(formData.get('start'));
        selectedEvent.setEnd(formData.get('end'));
        selectedEvent.setAllDay(formData.has('allDay'));
        selectedEvent.setExtendedProp('description', formData.get('description'));
        selectedEvent.setExtendedProp('location', formData.get('location'));
        selectedEvent.setExtendedProp('category', formData.get('category'));
        bootstrap.Modal.getInstance(document.getElementById('CalenderModalEdit')).hide();
        showToast('Event updated successfully!', 'success');
      } else {
        form.classList.add('was-validated');
      }
    }
  });

  // 이벤트 삭제: 확인 후 캘린더에서 제거 및 Google Calendar 동기화
  document.getElementById('deleteEvent').addEventListener('click', function() {
    if (selectedEvent && confirm('Are you sure you want to delete this event?')) {
      const eventId = selectedEvent.id;
      selectedEvent.remove();

      fetch('/calendar-events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'deleteEvent', id: eventId })
      })
      .then(res => res.json())
      .then(data => { if (!data.ok) showToast('구글 캘린더 삭제 실패', 'error'); });

      bootstrap.Modal.getInstance(document.getElementById('CalenderModalEdit')).hide();
      showToast('Event deleted successfully!', 'success');
    }
  });
  // 상세 모달 → 수정 모달 전환 (모달 중첩 방지를 위해 300ms 딜레이)
  document.getElementById('editEventBtn').addEventListener('click', function() {
    if (selectedEvent) {
      bootstrap.Modal.getInstance(document.getElementById('EventDetailsModal')).hide();
      setTimeout(() => openEditEventModal(selectedEvent), 300);
    }
  });

   // 새 이벤트 모달 닫힐 때 폼 초기화
  document.getElementById('CalenderModalNew').addEventListener('hidden.bs.modal', function() {
    document.getElementById('newEventForm').classList.remove('was-validated');
    document.getElementById('newEventForm').reset();
  });

  // 수정 모달 닫힐 때 폼 초기화 및 선택 이벤트 해제
  document.getElementById('CalenderModalEdit').addEventListener('hidden.bs.modal', function() {
    document.getElementById('editEventForm').classList.remove('was-validated');
    selectedEvent = null;
  });
}

// 우측 상단 토스트 알림 표시 (success / error / info)
function showToast(message, type = 'info') {
  const toastContainer = document.querySelector('.toast-container') || createToastContainer();
  const toastId = 'toast_' + Date.now();
  const bgClass = type === 'success' ? 'bg-success' : type === 'error' ? 'bg-danger' : 'bg-primary';
  const toastHtml = `
    <div id="${toastId}" class="toast align-items-center text-white ${bgClass} border-0" role="alert" aria-live="assertive" aria-atomic="true">
      <div class="d-flex">
        <div class="toast-body"><i class="fas fa-check-circle me-2"></i>${message}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
      </div>
    </div>
  `;
  toastContainer.insertAdjacentHTML('beforeend', toastHtml);
  const toastElement = document.getElementById(toastId);
  const toast = new bootstrap.Toast(toastElement, { delay: 3000 });
  toast.show();
   // 토스트 숨겨지면 DOM에서 제거 (메모리 누수 방지)
  toastElement.addEventListener('hidden.bs.toast', function() { toastElement.remove(); });
}

// 토스트 컨테이너가 없을 때 동적 생성
function createToastContainer() {
  const container = document.createElement('div');
  container.className = 'toast-container position-fixed top-0 end-0 p-3';
  container.style.zIndex = '9999';
  document.body.appendChild(container);
  return container;
}