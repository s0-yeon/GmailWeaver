// src/apps-script/gmail.js
// Gmail 동기화, 라벨, 캘린더, 단일 메일 업로드

// 동기화 버튼 핸들러  
function onSyncNewOnly(e) {
  return _runSync(false);
}

function onSyncAll(e) {
  return _runSync(true);
}

function _runSync(includeAll) {
  try {
    var query   = includeAll ? "in:inbox OR in:sent" : "in:inbox";
    var threads = GmailApp.search(query, 0, 50);
    var myEmail = Session.getActiveUser().getEmail();
    var allText = "";
    var count   = 0;

    threads.forEach(function(thread) {
      thread.getMessages().forEach(function(msg) {
        count++;
        allText += _buildMessageText(msg, myEmail) + "\n";
      });
    });

    var filename = "gmail_sync_" + (includeAll ? "all" : "inbox") + "_" + _dateToYmdHms(new Date()) + ".txt";

    UrlFetchApp.fetch(TunnelURL + "/upload", {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({ filename: filename, content: allText })
    });

    return _toast("✅ " + count + "개 메일을 서버로 전송했습니다.");
  } catch (err) {
    return _toast("⚠️ 동기화 실패: " + err.message);
  }
}

// 라벨 적용 (선택된 메일)
function onApplyLabelToMessage(e) {
  var inputs     = (e && e.commonEventObject && e.commonEventObject.formInputs) || {};
  var parameters = (e && e.commonEventObject && e.commonEventObject.parameters) || {};

  var labelName = (inputs.labelName && inputs.labelName.stringInputs)
    ? inputs.labelName.stringInputs.value[0].trim()
    : "";

  var messageId = parameters.messageId || "";

  if (!labelName) return _toast("라벨 이름을 입력해주세요.");
  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  try {
    var msg    = GmailApp.getMessageById(messageId);
    var thread = msg.getThread();

    var label  = GmailApp.getUserLabelByName(labelName);
    if (!label) label = GmailApp.createLabel(labelName);

    thread.addLabel(label);
    return _toast("✅ \"" + labelName + "\" 라벨이 적용되었습니다.");
  } catch (err) {
    return _toast("⚠️ 라벨 적용 실패: " + err.message);
  }
}

// 추출 및 캘린더 등록
function onExtractAndAddCalendar(e) {
  var parameters = (e && e.commonEventObject && e.commonEventObject.parameters) || {};
  var messageId  = parameters.messageId || "";

  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  var msg;
  try {
    msg = GmailApp.getMessageById(messageId);
  } catch (err) {
    return _toast("⚠️ 메일을 불러오지 못했습니다: " + err.message);
  }

  var subject = msg.getSubject() || "(제목 없음)";
  var body = msg.getPlainBody() || "";

  // OpenAI 직접 호출 엔드포인트로 변경
  var data;
  try {
    var res = UrlFetchApp.fetch(TunnelURL + "/extract-calendar", {
      method: "post",
      contentType: "application/json",
      headers: { "ngrok-skip-browser-warning": "1" },
      payload: JSON.stringify({ subject: subject, body: body })
    });
    data = JSON.parse(res.getContentText());
  } catch (err) {
    return _toast("⚠️ 서버 오류: " + err.message);
  }

  var events = data.events || [];
  if (!events.length) return _toast("📅 날짜/일정 정보를 찾지 못했습니다.");

  // messageId 별로 추출결과 임시 저장 (수동 제목 저장 버튼에서 재사용)
  _saveExtractedEvents(messageId, events, subject); 

  // 입력칸 + 저장 버튼 카드로 보여주기
  return _buildCalendarConfirmCard(messageId, events, subject);
}

function _saveExtractedEvents(messageId, events, subject) { // 추출 결과 userProperties에 저장 (messageId별)
  var key = "GW_CAL_" + messageId;
  var payload = {
    savedAt: new Date().toISOString(),
    subject: subject || "",
    events: events || []
  };
  PropertiesService.getUserProperties().setProperty(key, JSON.stringify(payload));
}

// 제목 입력 + 이 제목으로 저장 카드
function _buildCalendarConfirmCard(messageId, events, subject) { 
  var first = events[0] || {};

  // 추출 미리보기 텍스트 (첫 이벤트 중심으로)
  var previewLines = [];
  previewLines.push("<b>추출된 일정(1개 기준 미리보기)</b>");
  if (first.startTime) previewLines.push("• 시작: " + first.startTime);
  if (first.endTime)   previewLines.push("• 종료: " + first.endTime);
  if (first.title)     previewLines.push("• 제목: " + first.title);
  if (first.description) previewLines.push("• 설명: " + first.description);

  var preview = CardService.newTextParagraph()
    .setText(previewLines.join("<br/>"));

  var titleInput = CardService.newTextInput()
    .setFieldName("manualTitle")
    .setTitle("일정 제목")
    .setHint("제목을 입력하지 않으면 자동 생성되는 제목으로 저장됩니다")
    .setValue("");

  var saveBtn = CardService.newTextButton()
    .setText("일정 저장")
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setOnClickAction(
      CardService.newAction()
        .setFunctionName("onSaveCalendarWithManualTitle")
        .setParameters({ messageId: messageId })
    );

  var section = CardService.newCardSection()
    .setHeader("📅 일정 저장")
    .addWidget(preview)
    .addWidget(titleInput)
    .addWidget(saveBtn);

  return CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle("GmailWeaver"))
    .addSection(section)
    .build();
}

function onSaveCalendarWithManualTitle(e) { // 입력한 제목으로 캘린더 저장
  var inputs     = (e && e.commonEventObject && e.commonEventObject.formInputs) || {};
  var parameters = (e && e.commonEventObject && e.commonEventObject.parameters) || {};

  var messageId = parameters.messageId || "";
  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  var manualTitle = (inputs.manualTitle && inputs.manualTitle.stringInputs)
    ? String(inputs.manualTitle.stringInputs.value[0] || "").trim()
    : "";

  // 저장해둔 추출결과 로드
  var key = "GW_CAL_" + messageId;
  var raw = PropertiesService.getUserProperties().getProperty(key);
  if (!raw) return _toast("⚠️ 추출 결과를 찾을 수 없습니다. 다시 '일정 분석'을 눌러주세요.");

  var payload;
  try {
    payload = JSON.parse(raw);
  } catch (err) {
    return _toast("⚠️ 저장된 데이터가 손상되었습니다. 다시 시도해주세요.");
  }

  var events = (payload && payload.events) ? payload.events : [];
  if (!events.length) return _toast("📅 저장할 일정이 없습니다.");

  var cal = CalendarApp.getDefaultCalendar();
  var added = 0;

  events.forEach(function(ev, idx) {
    try {
      var start = new Date(ev.startTime);
      var end   = ev.endTime ? new Date(ev.endTime) : new Date(start.getTime() + 3600000);

      // ✅ 첫 이벤트는 입력 제목 적용, 나머지는 원래 제목 유지
      // (전부 같은 제목으로 저장하고 싶으면: var titleToUse = manualTitle; 로 바꾸면 됨)
      var titleToUse = manualTitle || ev.title || "(제목 없음)";

      // ✅ description에 표기 추가
      var baseDesc = ev.description || "";
      var stamp = "GmailWeaver에서 저장됨";
      var desc = baseDesc ? (stamp + "\n\n" + baseDesc) : stamp;

      cal.createEvent(titleToUse, start, end, { description: desc });
      added++;
    } catch (err) {
      Logger.log("calendar save error: " + err);
    }
  });

  return _toast(added > 0 ? "📅 " + added + "개 일정이 저장되었습니다." : "⚠️ 일정 저장 실패");
}

// 단일 메일 서버 업로드  
function onUploadSingleMessage(e) {
  var parameters = (e && e.commonEventObject && e.commonEventObject.parameters) || {};
  var messageId  = parameters.messageId || "";

  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  try {
    var msg     = GmailApp.getMessageById(messageId);
    var myEmail = Session.getActiveUser().getEmail();
    var content  = _buildMessageText(msg, myEmail);
    var filename = "gmail_single_" + messageId + ".txt";

    UrlFetchApp.fetch(TunnelURL + "/upload", {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({ filename: filename, content: content })
    });

    return _toast("☁️ 서버로 전송 완료");
  } catch (err) {
    return _toast("⚠️ 전송 실패: " + err.message);
  }
}

// 유틸 
function _buildMessageText(msg, myEmail) {
  var direction = msg.getFrom().includes(myEmail) ? "발신" : "수신";
  var atts = msg.getAttachments({ includeInlineImages: false });
  var attInfo = atts.length === 0
    ? "없음"
    : atts.map(function(a, i) {
        return (i + 1) + ". " + a.getName() + " | " + a.getContentType() + " | " + a.getSize() + " bytes";
      }).join("\n  ");

  return [
    "============================================================",
    "ID: "        + msg.getId(),
    "구분: "      + direction,
    "제목: "      + (msg.getSubject() || "(제목 없음)"),
    "보낸 사람: " + msg.getFrom(),
    "받는 사람: " + msg.getTo(),
    "날짜: "      + msg.getDate(),
    "",
    "[첨부파일]",
    attInfo,
    "",
    "[본문]",
    msg.getPlainBody() || "",
    "============================================================"
  ].join("\n");
}

function _dateToYmdHms(d) {
  var pad = function(n) { return String(n).padStart(2, "0"); };
  return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
    "_" + pad(d.getHours()) + pad(d.getMinutes()) + pad(d.getSeconds());
}