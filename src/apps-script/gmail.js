// src/apps-script/gmail.js
// Gmail 동기화, 라벨, 캘린더, 단일 메일 업로드

// 동기화 버튼 핸들러
// 서버에 없는 메일 추가
function onSyncNewOnly(e) {
  return _runSync("append");
}

// 전체 갱신 (보낸 메일 포함)
function onSyncAll(e) {
  return _runSync("rewrite");
}

// Gmail 동기화
function _runSync(mode) {
  try {
    // 사용자 속성에서 마지막 동기화 시간 가져옴
    var props = PropertiesService.getUserProperties();
    var lastSyncMs = Number(props.getProperty("GW_LAST_SYNC_MS") || "0");

    // 공통 변수
    var threads = []; // 스레드 목록
    var myEmail = Session.getActiveUser().getEmail(); // 발신자 구분에 사용
    var allText = ""; // 서버로 전송할 메일 본문
    var count = 0; // 메일 수
    var allAttachments = []; // 첨부파일 목록

    // 전체 갱신할 때
    if (mode === "rewrite") {
      var queryAll = "in:inbox OR in:sent";
      threads = GmailApp.search(queryAll, 0, 200);
      threads.forEach(function (thread) {
        // 각 스레드 순회
        thread.getMessages().forEach(function (msg) {
          // 스레드 속 메일 순회
          count++;
          allText += _buildMessageText(msg, myEmail, count) + "\n"; // 메일 본문 내용 추가
          allAttachments = allAttachments.concat(_buildAttachmentPayload(msg));
        });
      });

      if (count === 0) {
        // 전송할 메일 없을 때 팝업
        return _toast("📭 전송할 메일이 없습니다.");
      }

      var filenameAll = "mail_latest.txt";

      // 서버에 /upload 엔드포인트로 post 전송
      var resAll = UrlFetchApp.fetch(TunnelURL + "/upload", {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify({
          filename: filenameAll, // 파일명
          content: allText, // 본문
          attachment: allAttachments, // 첨부파일
          syncmode: "rewrite", // 처리방식
          gmail_id: myEmail,
        }),
        muteHttpExceptions: true, // HTTP 오류 에러 말고 응답으로 수신
      });

      var codeAll = resAll.getResponseCode();
      var textAll = resAll.getContentText();

      if (codeAll < 200 || codeAll >= 300) {
        throw new Error("upload failed: " + codeAll + " / " + textAll);
      }

      // 메일 전송 성공 시 동기화 시간 저장
      props.setProperty("GW_LAST_SYNC_MS", String(Date.now()));

      Logger.log("upload success: " + codeAll + " / " + textAll);
      Logger.log("메일 수: " + count);
      Logger.log("첨부 전송 개수: " + allAttachments.length);

      return _toast("✅ " + count + "개 메일 전송 완료. 첨부파일 내용은 메일 인덱싱 후 자동 처리됩니다.");
    }

    // 새로운 메일만 추가할 때
    var queryNew = "in:inbox OR in:sent";
    threads = GmailApp.search(queryNew, 0, 200);

    var newMessages = []; // 새로운 메일 메시지들 저장할 변수

    threads.forEach(function (thread) {
      thread.getMessages().forEach(function (msg) {
        var msgTime = msg.getDate().getTime();
        if (msgTime > lastSyncMs) {
          // 마지막 동기화 시간보다 나중인 메일만 선택해서 저장
          newMessages.push(msg);
        }
      });
    });

    // 새로운 메일을 위로 정렬
    newMessages.sort(function (a, b) {
      return b.getDate().getTime() - a.getDate().getTime();
    });

    // 새로운 메일만 추가할 때 처리
    newMessages.forEach(function (msg) {
      count++;
      allText += _buildMessageText(msg, myEmail, count) + "\n";
      allAttachments = allAttachments.concat(_buildAttachmentPayload(msg));
    });

    if (count === 0) {
      return _toast("📭 새로 추가할 메일이 없습니다.");
    }

    // append 누를 때마다 새로운 input 파일로 생성
    var filename = "inc_" + _dateToYmdHms(new Date()) + ".txt";

    // 서버에 /upload 엔드포인트로 post 전송
    var resNew = UrlFetchApp.fetch(TunnelURL + "/upload", {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({
        filename: filename,
        content: allText,
        attachment: allAttachments,
        syncmode: "append",
        gmail_id: myEmail,
      }),
      muteHttpExceptions: true,
    });

    var codeNew = resNew.getResponseCode();
    var textNew = resNew.getContentText();

    if (codeNew < 200 || codeNew >= 300) {
      throw new Error("upload failed: " + codeNew + " / " + textNew);
    }

    var dataNew = {};
    try {
      dataNew = JSON.parse(textNew); // 서버 응답 문자열 json으로 파싱 (밑에서 전체갱신으로 바꼈는지 아닌지 확인하기 위해 if문에 사용하기 위함)
    } catch (err) {
      throw new Error("응답 JSON 파싱 실패: " + textNew);
    }

    // 서버에 메일 전송 성공 시 동기화 시간 저장
    props.setProperty("GW_LAST_SYNC_MS", String(Date.now()));

    Logger.log("upload success: " + codeNew + " / " + textNew);
    Logger.log("메일 수: " + count);
    Logger.log("첨부 전송 개수: " + allAttachments.length);

    if (dataNew.fallback_to_rewrite) {
      // 새로운 메일만 추가 눌렀는데 인덱싱 안돼있어서 인덱싱 모드로 바뀌었으면
      return _toast("✅ 기존 인덱스가 없어 전체 인덱싱을 먼저 실행합니다.");
    }
    return _toast("✅ " + count + "개 새 메일 전송 완료. 첨부파일 내용은 메일 내용 인덱싱 후 자동 처리됩니다.");
  } catch (err) {
    return _toast("⚠️ 동기화 실패: " + err.message);
  }
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
// 메일 1개의 TXT 블록 생성
function _buildMessageText(msg, myEmail, mailIndex) {
  // 1) 기본 정보
  var id = msg.getId(); // 메시지 ID
  var from = msg.getFrom() || ""; // 송신인
  var to = msg.getTo() || ""; // 수신인
  var cc = msg.getCc() || ""; // 참조
  var subject = msg.getSubject() || "(제목 없음)"; // 메시지 제목
  var date = Utilities.formatDate(
    // 날짜
    msg.getDate(),
    Session.getScriptTimeZone(),
    "yyyy-MM-dd HH:mm:ss", // 문자열로 정규화
  );

  // 2) 수신/발신 구분
  var direction = from.includes(myEmail) ? "발신" : "수신";

  // 3) 라벨 정보 처리
  var thread = msg.getThread();
  var userLabels = thread.getLabels().map(function (label) {
    return label.getName();
  });

  var labelInfo = userLabels.length > 0 ? userLabels.join(", ") : "없음";

  // 4) 첨부파일 처리
  var atts = msg.getAttachments({ includeInlineImages: false }); // 본문에 인라인 이미지로 삽입된 경우 제외
  var attachmentInfo = ""; // 첨부파일 정보. TXT 기록용

  if (atts.length === 0) {
    attachmentInfo = "첨부파일: 없음";
  } else {
    attachmentInfo =
      "첨부파일:\n" +
      atts
        .map(function (a, i) {
          var name = a.getName() || "attachment_" + (i + 1);
          var mime = a.getContentType() || "application/octet-stream";
          var size = a.getSize();
          var lowerName = name.toLowerCase();

          // 확장자 및 MIME 타입 체크 확장
          var isSupported =
            lowerName.endsWith(".pdf") ||
            lowerName.endsWith(".docx") ||
            lowerName.endsWith(".hwp") ||
            lowerName.endsWith(".pptx") ||
            lowerName.endsWith(".xlsx") ||
            lowerName.endsWith(".csv") ||
            lowerName.endsWith(".txt");

          var status = "";

          if (!isSupported) {
            status = "업로드 제외: 형식 미지원";
          } else if (size > 10 * 1024 * 1024) {
            status = "업로드 제외: 용량 초과";
          } else {
            status = "업로드 포함";
          }

          return (
            "- " +
            name +
            " (" +
            (size / 1024).toFixed(1) +
            " KB) [" +
            status +
            "]"
          );
        })
        .join("\n");
  }

  var body = msg.getPlainBody() || "";
  // 본문 input txt 필요없는 요소 줄이기
  body = body.replace(/\r\n/g, "\n"); // \r\n (윈도우 방식) -> \n 변경
  body = body.replace(/\[image:[^\]]*\]/gi, ""); // [image: ] 제거
  body = body.replace(/[ \t]+/g, " "); // 연속 스페이스, 탭 -> 1개
  body = body.replace(/\n{2,}/g, "\n"); // 연속 줄바꿈 -> 1줄 줄바꿈
  body = body.replace(/ \n/g, "\n"); // 줄 끝 스페이스 제거
  body = body.replace(/\n /g, "\n"); // 줄 시작 스페이스 제거

  return [
    "============================================================",
    "[메일 " + mailIndex + "]",
    "ID: " + id,
    "구분: " + direction,
    "제목: " + subject,
    "발신인: " + from,
    "수신인: " + to,
    "참조(CC): " + cc,
    "날짜: " + date,
    "",

    "[라벨 정보]",
    labelInfo,
    "",
    "[첨부파일 정보]",
    attachmentInfo,
    "",
    "[메일 본문]",
    body,
    "============================================================",
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
      var dataBase64 = Utilities.base64Encode(att.getBytes());
      payload.push({
        mail_id: id,
        name: name,
        mime: mime,
        data_base64: dataBase64,
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
    var myEmail = Session.getActiveUser().getEmail();
    var queryAll = "in:inbox OR in:sent";
    var threads = GmailApp.search(queryAll, 0, 200);
    var allAttachments = [];
    var MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024;

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
      Logger.log("[AttachmentSync] 전송할 첨부파일 없음");
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

    Logger.log("[AttachmentSync] 전송 완료: " + allAttachments.length + "개 / " + res.getResponseCode());

  } catch (err) {
    Logger.log("[AttachmentSync] 오류: " + err.message);
  }
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