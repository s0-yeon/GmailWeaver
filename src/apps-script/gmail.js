// src/apps-script/gmail.js
// Gmail 동기화, 라벨, 캘린더, 단일 메일 업로드

//배치 시스템 상수
var BATCH_TRIGGER_MINUTES = 5;
var BATCH_SIZE = 50; // GmailApp.search() 1회 최대 반환 수 (스레드 단위)
var ATT_BATCH_SIZE = 10; //첨부파일 배치 크기
// 동기화 버튼 핸들러
// 서버에 없는 메일 추가
function onSyncNewOnly(e) {
  return _startBatchSync("append");
}

// 전체 갱신 (보낸 메일 포함)
function onSyncAll(e) {
  return _startBatchSync("rewrite");
}

// 배치 시스템
// 버튼 클릭 시 호출. 전체 메일 수만 세고 즉시 반환
// 실제 전송은 _runBatchSync() 트리거가 BATCH_TRIGGER_MINUTES마다 담당
function _startBatchSync(mode) {
  try {
    var props = PropertiesService.getUserProperties();
    var query = "in:inbox OR in:sent";

    // 1) append 모드: 최근 배치만 확인해서 새 메일 있는지 체크
    // 카운트 루프 제거 → 버튼 누르자마자 즉시 토스트 반환
    if (mode === "append") {
      var lastSyncMs = Number(props.getProperty("GW_LAST_SYNC_MS") || "0");
      var recentThreads = GmailApp.search(query, 0, BATCH_SIZE);
      var hasNew = recentThreads.some(function(thread) {
        return thread.getLastMessageDate().getTime() > lastSyncMs;
      });
      if (!hasNew) {
        return _toast("📭 새로 추가할 메일이 없습니다.");
      }
      props.setProperty("GW_SYNC_MODE", mode);
      props.setProperty("GW_BATCH_OFFSET", "0");
      return _toast(
        "✅ 새 메일 추가 인덱싱을 시작합니다.\n" +
        "백그라운드에서 " + BATCH_TRIGGER_MINUTES + "분 간격으로 처리됩니다."
      );
    }

    // 2) rewrite: PropertiesService에 배치 상태 저장 후 즉시 토스트 반환
    props.setProperty("GW_SYNC_MODE", mode);
    props.setProperty("GW_BATCH_OFFSET", "0");
    return _toast(
      "✅ 전체 갱신 인덱싱을 시작합니다.\n" +
      "백그라운드에서 " + BATCH_TRIGGER_MINUTES + "분 간격으로 처리됩니다."
    );

  } catch (err) {
    return _toast("⚠️ 동기화 실패: " + err.message);
  }
}

// 배치 트리거 등록 - Apps Script 편집기에서 최초 1회 수동 실행
// registerAttachmentTrigger()와 동일한 패턴
// 애드온 컨텍스트에서는 1시간 미만 트리거 등록 불가하므로
// 반드시 Apps Script 편집기에서 직접 실행해야 함
function registerBatchTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (trigger.getHandlerFunction() === "_runBatchSync") {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  ScriptApp.newTrigger("_runBatchSync")
    .timeBased()
    .everyMinutes(BATCH_TRIGGER_MINUTES)
    .create();
  Logger.log("[BatchTrigger] _runBatchSync " + BATCH_TRIGGER_MINUTES + "분 트리거 등록 완료");
}

// 배치 실행 트리거 함수 - Apps Script가 자동 호출
// PropertiesService에서 offset/mode를 읽어 배치 1개(스레드 200개) 처리
// - 완료(스레드 0개) 시: 트리거 삭제, 상태 초기화
// - 미완료 시: offset 갱신 후 다음 트리거 대기
// - 전송 실패 시: offset 갱신 안 함 → 다음 트리거에서 재시도
function _runBatchSync() {
  var props      = PropertiesService.getUserProperties();
  var mode       = props.getProperty("GW_SYNC_MODE");
 
  // GW_SYNC_MODE 없으면 할 일 없음 → 조기 리턴
  // 버튼을 누르지 않은 상태에서 트리거가 깨어났을 때
  if (!mode) {
    return;
  }
 
  var offset     = Number(props.getProperty("GW_BATCH_OFFSET") || "0");
  var myEmail    = Session.getActiveUser().getEmail();
  var query      = "in:inbox OR in:sent";
 
  try {
    // 1) 현재 오프셋 기준으로 스레드 BATCH_SIZE개 가져오기
    var threads = GmailApp.search(query, offset, BATCH_SIZE);
 
    // 2) 더 이상 스레드 없으면 배치 완료 처리
    if (threads.length === 0) {
      _finishBatchSync(props, mode);
      return;
    }
 
    // 3) 메일 텍스트 & 첨부 메타데이터 수집
    var allText        = "";
    var allAttachments = [];
    var count          = 0;
 
    if (mode === "append") {
      // append: 마지막 동기화 이후 메일만 필터링
      var lastSyncMs = Number(props.getProperty("GW_LAST_SYNC_MS") || "0");
      threads.forEach(function(thread) {
        thread.getMessages().forEach(function(msg) {
          if (msg.getDate().getTime() > lastSyncMs) {
            count++;
            allText += _buildMessageText(msg, myEmail, count) + "\n\n";
            allAttachments = allAttachments.concat(_buildAttachmentPayload(msg));
          }
        });
      });
    } else {
      // rewrite: 해당 배치 내 전체 메일 포함
      threads.forEach(function(thread) {
        thread.getMessages().forEach(function(msg) {
          count++;
          allText += _buildMessageText(msg, myEmail, count) + "\n\n";
          allAttachments = allAttachments.concat(_buildAttachmentPayload(msg));
        });
      });
    }
 
    // 4) 다음 배치 오프셋 및 마지막 배치 여부 계산
    var nextOffset = offset + threads.length;
    // threads.length < BATCH_SIZE 이면 더 이상 스레드 없음 → 마지막 배치
    var nextThreads = GmailApp.search(query, nextOffset, 1);
    var isLast;
    if (mode === "append") {
      // append: 다음 배치에 새 메일이 있는지 확인
      var lastSyncMs2 = Number(props.getProperty("GW_LAST_SYNC_MS") || "0");
      var hasNextNew = nextThreads.some(function(thread) {
        return thread.getLastMessageDate().getTime() > lastSyncMs2;
      });
      isLast = !hasNextNew;
    } else {
      // rewrite: 다음 배치에 스레드가 있는지만 확인
      isLast = (nextThreads.length === 0);
    }
    var filename = mode === "rewrite"
      ? "mail_latest.txt"
      : "inc_" + _dateToYmdHms(new Date()) + ".txt";
 
    // 5) 이번 배치에 보낼 메일이 없어도(append에서 새 메일 0개)
    //    오프셋은 갱신하고 다음 배치로 이동
    if (count === 0) {
      props.setProperty("GW_BATCH_OFFSET", String(nextOffset));
      if (isLast) {
        _finishBatchSync(props, mode);
      }
      return;
    }
 
    // 6) 서버로 배치 전송
    // is_last=true 일 때만 서버가 GraphRAG 파이프라인 실행
    // (마지막 배치 전까지는 mail_latest.txt에 누적만 하고 인덱싱 안 함)
    var res = UrlFetchApp.fetch(TunnelURL + "/upload", {
      method: "post",
      contentType: "application/json",
      headers: { "ngrok-skip-browser-warning": "1" },
      payload: JSON.stringify({
        filename:     filename,
        content:      allText,
        attachment:   allAttachments,
        syncmode:     mode,
        gmail_id:     myEmail,
        is_last:      isLast,       // 마지막 배치 여부 → 서버 GraphRAG 실행 타이밍 결정
        batch_offset: offset,       // 디버깅용: 현재 배치 시작 위치
      }),
      muteHttpExceptions: true,
    });
  
    var code = res.getResponseCode();
    var text = res.getContentText();
 
    if (code < 200 || code >= 300) {
      // 전송 실패 시 오프셋 갱신하지 않음 → 다음 트리거에서 동일 배치 재시도
      Logger.log("[BatchSync] 전송 실패 (재시도 예정): " + code + " / " + text);
      return;
    }
 
    // 7) 전송 성공 시 오프셋 갱신
    props.setProperty("GW_BATCH_OFFSET", String(nextOffset));
    Logger.log(
      "[BatchSync] 배치 완료: offset=" + offset + " → " + nextOffset +
      ", 메일=" + count + "개, isLast=" + isLast
    );
 
    // 8) 마지막 배치면 정리 처리
    if (isLast) {
      _finishBatchSync(props, mode);
    }
 
  } catch (err) {
    // 예외 발생 시 오프셋 갱신 안 함 → 다음 트리거에서 동일 배치 재시도
    Logger.log("[BatchSync] 오류 (재시도 예정): " + err.message);
  }
}

// 배치 완료 처리
// 마지막 배치 전송 후 호출: 배치 상태 키 초기화, 동기화 시간 저장
// 트리거는 삭제하지 않음 → 상시 돌면서 다음 버튼 클릭을 대기
function _finishBatchSync(props, mode) {
  // 동기화 완료 시각 저장 (append 모드의 다음 기준점으로 사용)
  props.setProperty("GW_LAST_SYNC_MS", String(Date.now()));
 
  // 배치 상태 키 초기화
  // GW_SYNC_MODE 없으면 _runBatchSync가 할 일 없다고 판단하고 리턴
  props.deleteProperty("GW_SYNC_MODE");
  props.deleteProperty("GW_BATCH_OFFSET");
 
  Logger.log("[BatchSync] 전체 배치 완료. mode=" + mode);
}

// 라벨 적용 (선택된 메일)
function onApplyLabelToMessage(e) {
  var inputs =
    (e && e.commonEventObject && e.commonEventObject.formInputs) || {};
  var parameters =
    (e && e.commonEventObject && e.commonEventObject.parameters) || {};

  var labelName =
    inputs.labelName && inputs.labelName.stringInputs
      ? inputs.labelName.stringInputs.value[0].trim()
      : "";

  var messageId = parameters.messageId || "";

  if (!labelName) return _toast("라벨 이름을 입력해주세요.");
  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  try {
    var msg = GmailApp.getMessageById(messageId);
    var thread = msg.getThread();

    var label = GmailApp.getUserLabelByName(labelName);
    if (!label) label = GmailApp.createLabel(labelName);

    thread.addLabel(label);
    return _toast('✅ "' + labelName + '" 라벨이 적용되었습니다.');
  } catch (err) {
    return _toast("⚠️ 라벨 적용 실패: " + err.message);
  }
}

// 일정 추출 및 캘린더 등록
function onExtractAndAddCalendar(e) {
  var parameters =
    (e && e.commonEventObject && e.commonEventObject.parameters) || {};
  var messageId = parameters.messageId || "";

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
      payload: JSON.stringify({ subject: subject, body: body }),
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

// 추출 결과를 userProperties에 저장 (messageId별)
function _saveExtractedEvents(messageId, events, subject) {
  var key = "GW_CAL_" + messageId;
  var payload = {
    savedAt: new Date().toISOString(),
    subject: subject || "",
    events: events || [],
  };
  PropertiesService.getUserProperties().setProperty(
    key,
    JSON.stringify(payload),
  );
}

// 사용자 입력 제목 & 추출된 데이터 기반으로 구글 캘린더에 일정 등록
function _buildCalendarConfirmCard(messageId, events, subject) {
  var first = events[0] || {};

  // 추출 미리보기 텍스트 (첫 이벤트 중심으로)
  var previewLines = [];
  previewLines.push("<b>추출된 일정(1개 기준 미리보기)</b>");
  if (first.startTime) previewLines.push("• 시작: " + first.startTime);
  if (first.endTime) previewLines.push("• 종료: " + first.endTime);
  if (first.title) previewLines.push("• 제목: " + first.title);
  if (first.description) previewLines.push("• 설명: " + first.description);

  var preview = CardService.newTextParagraph().setText(
    previewLines.join("<br/>"),
  );

  // 제목 입력란
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
        .setParameters({ messageId: messageId }),
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

// 입력한 제목으로 캘린더 저장
function onSaveCalendarWithManualTitle(e) {
  var inputs =
    (e && e.commonEventObject && e.commonEventObject.formInputs) || {}; // 제목 입력창에 입력한 값
  var parameters =
    (e && e.commonEventObject && e.commonEventObject.parameters) || {};

  var messageId = parameters.messageId || "";
  if (!messageId) return _toast("메시지 ID를 찾을 수 없습니다.");

  // 입력란이 비어있으면 빈 문자열로 처리
  var manualTitle =
    inputs.manualTitle && inputs.manualTitle.stringInputs
      ? String(inputs.manualTitle.stringInputs.value[0] || "").trim()
      : "";

  // 저장해둔 추출결과 로드
  var key = "GW_CAL_" + messageId;
  var raw = PropertiesService.getUserProperties().getProperty(key);
  if (!raw)
    return _toast(
      "⚠️ 추출 결과를 찾을 수 없습니다. 다시 '일정 분석'을 눌러주세요.",
    );

  var payload;
  try {
    payload = JSON.parse(raw);
  } catch (err) {
    return _toast("⚠️ 저장된 데이터가 손상되었습니다. 다시 시도해주세요.");
  }

  var events = payload && payload.events ? payload.events : [];
  if (!events.length) return _toast("📅 저장할 일정이 없습니다.");

  var cal = CalendarApp.getDefaultCalendar(); // 기본 캘린더
  var added = 0;

  events.forEach(function (ev, idx) {
    try {
      var start = new Date(ev.startTime);
      // end 타임 없으면 시작 시간 +1로 설정
      var end = ev.endTime
        ? new Date(ev.endTime)
        : new Date(start.getTime() + 3600000);

      // 첫 이벤트는 입력 제목 적용, 나머지는 원래 제목 유지
      // (전부 같은 제목으로 저장하고 싶으면: var titleToUse = manualTitle; 로 바꾸면 됨)
      var titleToUse = manualTitle || ev.title || "(제목 없음)";

      // description에 표기 추가

      var baseDesc = ev.description || "";
      var stamp = "GmailWeaver에서 저장됨";
      var desc = baseDesc ? stamp + "\n\n" + baseDesc : stamp;

      cal.createEvent(titleToUse, start, end, { description: desc });
      added++;
    } catch (err) {
      Logger.log("calendar save error: " + err);
    }
  });

  return _toast(
    added > 0
      ? "📅 " + added + "개 일정이 저장되었습니다."
      : "⚠️ 일정 저장 실패",
  );
}

// 공통 유틸
// 발신인, 수신인, 참조인 등 Person type 정규화
function _parsePerson(raw) {
  if (!raw || !raw.trim()) return null;
  var match = raw.match(/^"?([^"<]+?)"?\s*<([^>]+)>$/);
  if (match) {
    var name = match[1]   
      .replace(/\(.*?\)/g, "")
      .replace(/\s+/g, " ")
      .trim();
    var account = match[2].trim().toLowerCase();
    return { name: name, account: account };
  } else {               
    var account = raw.trim().toLowerCase();
    return { name: account, account: account };
  }
}

// 복수 CC 처리
function _parsePersonList(raw) {
  if (!raw || !raw.trim()) return [];
  return raw.split(',')
    .map(function(s) { return _parsePerson(s.trim()); })
    .filter(function(p) { return p !== null; });
}

// Person type 포맷팅
function _formatPerson(p) {
  if (!p) return "없음";  
  if (p.name !== p.account) return p.name + " <" + p.account + ">";   // 이름 + 계정 형태 
  return "<" + p.account + ">";   // 계정만 있는 형태
} 

// 메일 1개의 TXT 블록 생성
function _buildMessageText(msg, myEmail, mailIndex) {
  // 1) 기본 정보
  var id = msg.getId();
  var fromRaw = msg.getFrom();
  var from = _parsePerson(fromRaw);
  var direction = (from && from.account === myEmail.toLowerCase()) ? "발신" : "수신";
  var to = _parsePersonList(msg.getTo());
  var ccRaw = msg.getCc();
  var cc = _parsePersonList(ccRaw);
  var subject = msg.getSubject() || "(제목 없음)";
  var date = Utilities.formatDate(
    msg.getDate(),
    Session.getScriptTimeZone(),
    "yyyy-MM-dd HH:mm:ss"
  );

  // 3) 라벨 정보 
  var thread     = msg.getThread();
  var userLabels = thread.getLabels().map(function(l) { return l.getName(); });
  var labelInfo  = userLabels.length > 0 ? userLabels.join(", ") : "없음";

  // 4) 첨부파일
  var atts = msg.getAttachments({ includeInlineImages: false });
  var attachmentInfo;

  if (atts.length === 0) {
    attachmentInfo = "없음";
  } else {
    attachmentInfo = atts.map(function(a, i) {
      var name  = a.getName() || "attachment_" + (i + 1);
      var size  = a.getSize();
      var lower = name.toLowerCase();
      var supported = [".pdf",".docx",".hwp",".pptx",".xlsx",".csv",".txt"]
        .some(function(ext) { return lower.endsWith(ext); });
      var status = !supported           ? "제외: 형식 미지원"
                 : size > 10*1024*1024  ? "제외: 용량 초과"
                 :                        "포함";
      return "- " + name + " (" + (size/1024).toFixed(1) + " KB) [" + status + "]";
    }).join("\n");
  }

  // 본문 input txt 필요없는 요소 줄이기
  var body = (msg.getPlainBody() || "")
    .replace(/\r\n/g, "\n")
    .replace(/\[image:[^\]]*\]/gi, "")
    .replace(/<https?:\/\/[^>]+>/g, "")    // <URL> 형태 제거
    .replace(/https?:\/\/\S+/g, "")        // 일반 URL 제거
    .replace(/unsubscribe[\s\S]*$/i, "")   // footer 제거
    .replace(/opt.out[\s\S]*$/i, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/ \n/g, "\n")
    .replace(/\n /g, "\n")
    .trim();

  var ccStr = cc.length > 0 ? cc.map(_formatPerson).join(", ") : "없음";

  return [
    "============================================================",
    "[메일 " + mailIndex + "]",
    "",
    "ID: " + id,
    "제목: " + subject,
    "구분: " + direction,
    "날짜: " + date,
    "",
    "발신인: " + _formatPerson(from),
    "수신인: " + (to.length > 0 ? to.map(_formatPerson).join(", ") : "없음"),
    "참조(CC): " + ccStr,
    "",
    "[라벨 정보]",
    labelInfo,
    "",
    "[첨부파일 정보]",
    attachmentInfo,
    "",
    "[메일 본문]",
    body,
  ].join("\n");
}

// 서버 전송용 첨부 payload 생성
function _buildAttachmentPayload(msg) {
  var atts = msg.getAttachments({ includeInlineImages: false }); // 본문에 인라인 이미지로 삽입된 경우 제외
  var id = msg.getId();
  var payload = [];
  var MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024; // 10MB 크기 제한

  atts.forEach(function (att, i) {
    var name = att.getName() || "attachment_" + (i + 1);
    var mime = att.getContentType() || "application/octet-stream";
    var size = att.getSize();
    var lowerName = name.toLowerCase();

    var isPdf =
      lowerName.endsWith(".pdf") ||
      mime === "application/pdf" ||
      mime === "application/haansoftpdf";
    var isDocx =
      lowerName.endsWith(".docx") ||
      mime ===
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    var isHwp =
      lowerName.endsWith(".hwp") ||
      mime === "application/x-hwp" ||
      mime === "application/haansofthwp";
    var isPptx =
      lowerName.endsWith(".pptx") ||
      mime ===
        "application/vnd.openxmlformats-officedocument.presentationml.presentation";
    var isXlsx =
      lowerName.endsWith(".xlsx") ||
      mime ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    var isCsv =
      lowerName.endsWith(".csv") ||
      mime === "text/csv" ||
      mime === "application/csv";
    var isTxt = lowerName.endsWith(".txt") || mime === "text/plain";

    var isSupported =
      isPdf || isDocx || isHwp || isPptx || isXlsx || isCsv || isTxt;

    // base64 인코딩 후 payload push
    if (isSupported && size <= MAX_ATTACHMENT_SIZE) {
      payload.push({
        mail_id: id,
        name: name,
        mime: mime,
        // data_base64 없음 - 원본은 _runAttachmentSync()가 별도 전송
      });
    }
  });

  return payload;
}

// Date 객체 YYYY-MM-DD_HHmmss 형식으로 변환
function _dateToYmdHms(d) {
  var pad = function (n) {
    return String(n).padStart(2, "0");
  };
  return (
    d.getFullYear() +
    "-" +
    pad(d.getMonth() + 1) +
    "-" +
    pad(d.getDate()) +
    "_" +
    pad(d.getHours()) +
    pad(d.getMinutes()) +
    pad(d.getSeconds())
  );
}

// 10분 트리거: 첨부파일 원본을 서버로 전송
// Apps Script 트리거에서 자동 실행됨 (사용자 인터랙션 없음)
function _runAttachmentSync() {
  try {
    var props = PropertiesService.getUserProperties();
    var myEmail = Session.getActiveUser().getEmail();

    var lastSyncMs = Number(props.getProperty("GW_LAST_SYNC_MS") || "0");
    var lastSyncDate = _msToDateStr(lastSyncMs);
    var queryNew = "in:inbox OR in:sent after:" + lastSyncDate;
    var attOffset = Number(props.getProperty("GW_ATT_OFFSET") || "0");
    var threads = GmailApp.search(queryNew, attOffset, ATT_BATCH_SIZE);
    var allAttachments = [];
    var MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024;

    if (threads.length === 0) {
      props.deleteProperty("GW_ATT_OFFSET");
      Logger.log("[AttachmentSync] 모든 배치 완료, 오프셋 초기화");
      return;
    }

    threads.forEach(function(thread) {
      thread.getMessages().forEach(function(msg) {
        var id = msg.getId();
        var atts = msg.getAttachments({ includeInlineImages: false });

        atts.forEach(function(att, i) {
          var name = att.getName() || ("attachment_" + (i + 1));
          var mime = att.getContentType() || "application/octet-stream";
          var size = att.getSize();
          var lowerName = name.toLowerCase();

          var isSupported = lowerName.endsWith(".pdf") || lowerName.endsWith(".docx") ||
                            lowerName.endsWith(".hwp") || lowerName.endsWith(".pptx") ||
                            lowerName.endsWith(".xlsx") || lowerName.endsWith(".csv") ||
                            lowerName.endsWith(".txt");

          // base64 인코딩 후 payload에 추가
          if (isSupported && size <= MAX_ATTACHMENT_SIZE) {
            var dataBase64 = Utilities.base64Encode(att.getBytes());
            allAttachments.push({
              mail_id: id,
              name: name,
              mime: mime,
              data_base64: dataBase64
            });
          }
        });
      });
    });

    if (allAttachments.length === 0) {
      // 첨부파일 없어도 오프셋은 갱신 (다음 배치로 이동)
      if (threads.length < ATT_BATCH_SIZE) {
        props.deleteProperty("GW_ATT_OFFSET");
      } else {
        props.setProperty("GW_ATT_OFFSET", String(attOffset + threads.length));
      }
      Logger.log("[AttachmentSync] 이번 배치 전송할 첨부파일 없음");
      return;
    }

    // /upload-attachments 엔드포인트로 전송 (메일 본문 없이 첨부만)
    var res = UrlFetchApp.fetch(TunnelURL + "/upload-attachments", {
      method: "post",
      contentType: "application/json",
      headers: { "ngrok-skip-browser-warning": "1" },
      payload: JSON.stringify({
        gmail_id: myEmail,
        attachments: allAttachments
      }),
      muteHttpExceptions: true
    });

    var code = res.getResponseCode();
    Logger.log("[AttachmentSync] 전송 완료: " + allAttachments.length + "개 / " + code);

    // 전송 성공 시에만 오프셋 갱신
    // 실패 시 오프셋 갱신 안 함 → 다음 트리거에서 동일 배치 재시도
    if (code >= 200 && code < 300) {
      if (threads.length < ATT_BATCH_SIZE) {
        props.deleteProperty("GW_ATT_OFFSET"); // 마지막 배치: 초기화
        Logger.log("[AttachmentSync] 마지막 배치 완료, 오프셋 초기화");
      } else {
        props.setProperty("GW_ATT_OFFSET", String(attOffset + threads.length)); // 다음 배치로
        Logger.log("[AttachmentSync] 오프셋 갱신: " + attOffset + " → " + (attOffset + threads.length));
      }
    } else {
      Logger.log("[AttachmentSync] 전송 실패 (재시도 예정): " + code + " / " + res.getContentText());
    }

  } catch (err) {
    Logger.log("[AttachmentSync] 오류: " + err.message);
  }
}

function _msToDateStr(ms) {
  var d = ms ? new Date(ms) : new Date(0);
  var y = d.getFullYear();
  var m = String(d.getMonth() + 1).padStart(2, "0");
  var day = String(d.getDate()).padStart(2, "0");
  return y + "/" + m + "/" + day;
}

// 10분 트리거 등록 (최초 1회만 실행하면 됨)
// Apps Script 편집기에서 수동으로 한 번 실행해서 등록
function registerAttachmentTrigger() {
  // 기존 트리거 중복 방지: 같은 함수명 트리거가 있으면 삭제 후 재등록
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (trigger.getHandlerFunction() === "_runAttachmentSync") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // 10분마다 _runAttachmentSync 실행
  ScriptApp.newTrigger("_runAttachmentSync")
    .timeBased()
    .everyMinutes(10)
    .create();

  Logger.log("[Trigger] _runAttachmentSync 10분 트리거 등록 완료");
}