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
    var threads = GmailApp.search(query, 0, 500);
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
  var raw, data;
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

  var cal   = CalendarApp.getDefaultCalendar();
  var added = 0;
  events.forEach(function(ev) {
    try {
      var start = new Date(ev.startTime);
      var end   = ev.endTime ? new Date(ev.endTime) : new Date(start.getTime() + 3600000);
      cal.createEvent(ev.title || subject, start, end, { description: ev.description || "" });
      added++;
    } catch(_) {}
  });

  return _toast(added > 0 ? "📅 " + added + "개 일정이 등록되었습니다." : "⚠️ 일정 등록 실패");
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