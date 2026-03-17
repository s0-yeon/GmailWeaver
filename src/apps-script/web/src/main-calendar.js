// apps-script/web/main-calendar.js
import DOMPurify from 'dompurify';
import * as bootstrap from 'bootstrap';
window.bootstrap = bootstrap;
globalThis.bootstrap = bootstrap;

import './main.scss';
import './js/helpers/smartresize.js';
import './js/sidebar.js';
import './js/init.js';

import { Calendar } from '@fullcalendar/core';
import dayGridPlugin from '@fullcalendar/daygrid';
import interactionPlugin from '@fullcalendar/interaction';
import timeGridPlugin from '@fullcalendar/timegrid';

window.FullCalendar = { Calendar, dayGridPlugin, interactionPlugin, timeGridPlugin };
globalThis.FullCalendar = { Calendar, dayGridPlugin, interactionPlugin, timeGridPlugin };

let currentCalendar = null;
let selectedEvent = null;

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

function generateEventId() {
  return 'event_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

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

      events: function(fetchInfo, successCallback, failureCallback) {
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
        .then(data => successCallback(data.events || []))
        .catch(err => failureCallback(err));
      },

      select: function(selectInfo) {
        openNewEventModal(selectInfo);
      },

      eventClick: function(eventClickInfo) {
        selectedEvent = eventClickInfo.event;
        showEventDetails(eventClickInfo.event);
      },

      eventDidMount: function(info) {
        info.el.setAttribute('title', info.event.title);
        if (info.event.extendedProps.description) {
          info.el.setAttribute('data-bs-toggle', 'tooltip');
          info.el.setAttribute('data-bs-title', info.event.extendedProps.description);
        }
      }
    });

    currentCalendar.render();
    window.calendar = currentCalendar;
    globalThis.calendar = currentCalendar;

    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    [...tooltipTriggerList].map(el => new bootstrap.Tooltip(el));
  }

  setupModalHandlers();
}

// DOM 준비 여부에 따라 실행
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initCalendar);
} else {
  initCalendar();
}

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

function showEventDetails(event) {
  const modal = new bootstrap.Modal(document.getElementById('EventDetailsModal'));
  const contentEl = document.getElementById('eventDetailsContent');

  const startDate = event.start ? event.start.toLocaleDateString() : 'Not specified';
  const startTime = event.start && !event.allDay ? event.start.toLocaleTimeString() : '';
  const endDate = event.end ? event.end.toLocaleDateString() : '';
  const endTime = event.end && !event.allDay ? event.end.toLocaleTimeString() : '';

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

function setupModalHandlers() {
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

  document.getElementById('editEventBtn').addEventListener('click', function() {
    if (selectedEvent) {
      bootstrap.Modal.getInstance(document.getElementById('EventDetailsModal')).hide();
      setTimeout(() => openEditEventModal(selectedEvent), 300);
    }
  });

  document.getElementById('CalenderModalNew').addEventListener('hidden.bs.modal', function() {
    document.getElementById('newEventForm').classList.remove('was-validated');
    document.getElementById('newEventForm').reset();
  });

  document.getElementById('CalenderModalEdit').addEventListener('hidden.bs.modal', function() {
    document.getElementById('editEventForm').classList.remove('was-validated');
    selectedEvent = null;
  });
}

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
  toastElement.addEventListener('hidden.bs.toast', function() { toastElement.remove(); });
}

function createToastContainer() {
  const container = document.createElement('div');
  container.className = 'toast-container position-fixed top-0 end-0 p-3';
  container.style.zIndex = '9999';
  document.body.appendChild(container);
  return container;
}