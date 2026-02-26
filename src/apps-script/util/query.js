// ============================================================
// src/apps-script/util/query.js  (비동기 패턴)
// ============================================================

function onSmartSearch_(e) {
  var inputs = (e && e.commonEventObject && e.commonEventObject.formInputs) || {};
  var query  = (inputs.searchQuery && inputs.searchQuery.stringInputs)
    ? inputs.searchQuery.stringInputs.value[0].trim() : "";
  if (!query) return _toast_("메시지를 입력해주세요.");

  var jobId;
  try {
    var res = UrlFetchApp.fetch(TunnelURL + "/run-query-async", {
      method: "post",
      contentType: "application/json",
      headers: { "ngrok-skip-browser-warning": "1" },
      payload: JSON.stringify({ message: query, resMethod: "local", resType: "structured" })
    });
    jobId = JSON.parse(res.getContentText()).jobId;
  } catch (err) { return _toast_("⚠️ 서버 연결 실패: " + err.message); }

  if (!jobId) return _toast_("⚠️ 서버 응답 오류");
  return _pendingCard_(query, jobId);
}

// ── 캘린더 비동기 요청 ─────────────────────────────────────

function requestCalendarAsync_(message, messageId) {
  var jobId;
  try {
    var res = UrlFetchApp.fetch(TunnelURL + "/run-query-async", {
      method: "post",
      contentType: "application/json",
      headers: { "ngrok-skip-browser-warning": "1" },
      payload: JSON.stringify({ message: message, resMethod: "local", resType: "calendar" })
    });
    jobId = JSON.parse(res.getContentText()).jobId;
  } catch (err) { return _toast_("⚠️ 서버 연결 실패: " + err.message); }

  if (!jobId) return _toast_("⚠️ 서버 응답 오류");
  return _pendingCard_("일정 분석 중...", jobId, "calendar", messageId);
}

// ── 결과 확인 버튼 핸들러 ──────────────────────────────────

function onCheckJobResult_(e) {
  var params    = (e && e.commonEventObject && e.commonEventObject.parameters) || {};
  var jobId     = params.jobId     || "";
  var query     = params.query     || "";
  var jobType   = params.jobType   || "query";
  var messageId = params.messageId || "";

  if (!jobId) return _toast_("jobId를 찾을 수 없습니다.");

  var data;
  try {
    var res = UrlFetchApp.fetch(TunnelURL + "/job-status/" + jobId, {
      method: "get",
      headers: { "ngrok-skip-browser-warning": "1" }
    });
    data = JSON.parse(res.getContentText());
  } catch (err) { return _toast_("⚠️ 상태 확인 실패: " + err.message); }

  var status = data.status || "";

  if (status === "pending") {
    return _pendingCard_(query, jobId, jobType, messageId, "⏳ 아직 처리 중입니다.\n잠시 후 다시 확인해주세요.");
  }

  if (status === "error") {
    return _toast_("⚠️ 오류: " + (data.result || "알 수 없는 오류"));
  }

  if (jobType === "calendar") {
    return _handleCalendarResult_(data.data || {});
  }

  var result = data.result || "";
  var parsed = null;
  try { parsed = JSON.parse(result); } catch(_) {}

  if (parsed) {
    var intent = (parsed.intent || "").toLowerCase();
    if (intent === "label"  && parsed.actions) return _toast_(_executeLabelActions_(parsed.actions));
    if (intent === "delete" && parsed.actions) return _toast_(_executeDeleteActions_(parsed.actions));
    result = parsed.result || result;
  }

  return _answerCard_(query, result);
}

// ── 캘린더 결과 → 토스트 ──────────────────────────────────

function _handleCalendarResult_(calData) {
  var events = calData.events || [];
  if (!events.length) return _toast_("📅 날짜/일정 정보를 찾지 못했습니다.");

  var cal   = CalendarApp.getDefaultCalendar();
  var added = 0;

  events.forEach(function(ev) {
    try {
      var start = new Date(ev.startTime);
      var end   = ev.endTime ? new Date(ev.endTime) : new Date(start.getTime() + 3600000);
      cal.createEvent(ev.title || "일정", start, end, { description: ev.description || "" });
      added++;
    } catch(_) {}
  });

  return _toast_(added > 0 ? "📅 " + added + "개 일정이 캘린더에 등록되었습니다." : "⚠️ 일정 등록에 실패했습니다.");
}

// ── "처리 중" 카드 ─────────────────────────────────────────

function _pendingCard_(query, jobId, jobType, messageId, notice) {
  notice    = notice    || "분석 중입니다.\n완료 후 아래 버튼을 눌러 결과를 확인하세요.";
  jobType   = jobType   || "query";
  messageId = messageId || "";

  var card = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader()
      .setTitle("🔍 검색 중...")
      .setSubtitle(_truncate_(query, 60)))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newDecoratedText()
        .setText(notice)
        .setStartIcon(CardService.newIconImage()
          .setIconUrl("https://www.gstatic.com/images/icons/material/system/1x/hourglass_empty_grey600_24dp.png")))
      .addWidget(CardService.newDivider())
      .addWidget(CardService.newTextButton()
        .setText("🔄 결과 확인")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(CardService.newAction()
          .setFunctionName("onCheckJobResult_")
          .setParameters({
            jobId:     jobId,
            query:     _truncate_(query, 200),
            jobType:   jobType,
            messageId: messageId
          })))
      .addWidget(CardService.newTextButton()
        .setText("← 홈으로")
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOnClickAction(CardService.newAction().setFunctionName("onBackHome_"))))
    .build();

  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
}

// ── 라벨 / 삭제 ────────────────────────────────────────────

function _executeLabelActions_(actions) {
  var total = 0;
  actions.forEach(function(action) {
    var lname = action.labelName || "";
    if (!lname) return;
    var label = GmailApp.getUserLabelByName(lname) || GmailApp.createLabel(lname);
    (action.threadIds || []).forEach(function(id) {
      try { var t = GmailApp.getThreadById(id); if (t) { t.addLabel(label); total++; } } catch(_) {}
    });
  });
  return total > 0 ? "✅ " + total + "개 스레드에 라벨 적용 완료" : "⚠️ 적용할 스레드를 찾지 못했습니다.";
}

function _executeDeleteActions_(actions) {
  var deleted = 0;
  (actions.threadIds || []).forEach(function(id) {
    try { var t = GmailApp.getThreadById(id); if (t) { t.moveToTrash(); deleted++; } } catch(_) {}
  });
  return deleted > 0 ? "🗑️ " + deleted + "개 스레드 삭제 완료" : "⚠️ 삭제할 스레드를 찾지 못했습니다.";
}

// ── 답변 카드 ──────────────────────────────────────────────

function _answerCard_(query, answer) {
  var card = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader()
      .setTitle("💬 검색 결과")
      .setSubtitle(_truncate_(query, 60)))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newTextParagraph().setText(answer))
      .addWidget(CardService.newDivider())
      .addWidget(CardService.newTextButton()
        .setText("🔎 자세히 검색하기")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOpenLink(CardService.newOpenLink()
          .setUrl(WEBAPP_URL)
          .setOpenAs(CardService.OpenAs.FULL_SIZE)))
      .addWidget(CardService.newTextButton()
        .setText("← 홈으로 돌아가기")
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOnClickAction(CardService.newAction().setFunctionName("onBackHome_"))))
    .build();

  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
}

// ── 공통 유틸 ──────────────────────────────────────────────

function onBackHome_(e) {
  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().popToRoot())
    .build();
}

function _toast_(msg) {
  return CardService.newActionResponseBuilder()
    .setNotification(CardService.newNotification().setText(msg))
    .build();
}

function _truncate_(str, max) {
  if (!str) return "";
  return str.length > max ? str.slice(0, max) + "…" : str;
}