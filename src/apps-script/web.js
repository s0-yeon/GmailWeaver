// src/apps-script/web.js

// Web App 진입점
// 브라우저에서 Web App URL로 접근 시 실행
function doGet(e) {
  const email = Session.getActiveUser().getEmail();
  const name = encodeURIComponent(email.split("@")[0]);
  const dashboardUrl =
    TunnelURL +
    "/dashboard/?name=" +
    name +
    "&gmail_id=" +
    encodeURIComponent(email);
  return HtmlService.createHtmlOutput(
    '<script>window.location.href = "' + dashboardUrl + '";<\/script>',
  ).setTitle("GmailWeaver Web App");
}

function doPost(e) {
  const data = JSON.parse(e.postData.contents);
  const action = data.action;
  const calendar = CalendarApp.getDefaultCalendar();

  // ── 캘린더: 일정 조회 ──
  if (action === "getEvents") {
    const start = new Date(data.start);
    const end = new Date(data.end);
    const events = calendar.getEvents(start, end);
    const result = events.map((ev) => ({
      id: ev.getId(),
      title: ev.getTitle(),
      start: ev.getStartTime().toISOString(),
      end: ev.getEndTime().toISOString(),
    }));
    return ContentService.createTextOutput(
      JSON.stringify({ events: result }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 캘린더: 일정 추가 ──
  if (action === "addEvent") {
    const ev = calendar.createEvent(
      data.title,
      new Date(data.start),
      new Date(data.end),
      { description: data.description || "" },
    );
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true, id: ev.getId() }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 캘린더: 일정 삭제 ──
  if (action === "deleteEvent") {
    const ev = calendar.getEventById(data.id);
    if (ev) ev.deleteEvent();
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 라벨 목록 조회 ──
  if (action === "getLabels") {
    const labels = GmailApp.getUserLabels();
    const result = labels.map((l) => ({
      id: l.getName(),
      name: l.getName(),
      unreadCount: l.getUnreadCount(),
      threadCount: l.getThreads(0, 1).length,
    }));
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true, labels: result }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 라벨 생성 ──
  if (action === "createLabel") {
    const labelName = (data.labelName || "").trim();
    if (!labelName) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: "라벨 이름이 비어있습니다." }),
      ).setMimeType(ContentService.MimeType.JSON);
    }
    let label = GmailApp.getUserLabelByName(labelName);
    if (!label) label = GmailApp.createLabel(labelName);
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true, name: label.getName() }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 라벨별 메일 목록 조회 ──
  if (action === "getLabelMessages") {
    const labelName = (data.labelName || "").trim();
    const maxResults = Math.min(Number(data.maxResults || 30), 50);
    if (!labelName) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: "라벨 이름이 필요합니다." }),
      ).setMimeType(ContentService.MimeType.JSON);
    }
    const label = GmailApp.getUserLabelByName(labelName);
    if (!label) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: "라벨을 찾을 수 없습니다." }),
      ).setMimeType(ContentService.MimeType.JSON);
    }
    const threads = label.getThreads(0, maxResults);
    const messages = threads.map((thread) => {
      const msg = thread.getMessages()[0];
      return {
        id: msg.getId(),
        threadId: thread.getId(),
        subject: msg.getSubject() || "(제목 없음)",
        from: msg.getFrom() || "",
        date: msg.getDate().toISOString(),
        snippet: msg.getPlainBody().substring(0, 100).replace(/\n/g, " "),
        isUnread: msg.isUnread(),
      };
    });
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true, messages: messages }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 메일 본문 조회 ──
  if (action === "getMessage") {
    const messageId = (data.messageId || "").trim();
    if (!messageId) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: "messageId가 필요합니다." }),
      ).setMimeType(ContentService.MimeType.JSON);
    }
    try {
      const msg = GmailApp.getMessageById(messageId);
      const result = {
        id: msg.getId(),
        subject: msg.getSubject() || "(제목 없음)",
        from: msg.getFrom() || "",
        to: msg.getTo() || "",
        date: msg.getDate().toISOString(),
        body: msg.getPlainBody() || "",
        gmailUrl: "https://mail.google.com/mail/u/0/#all/" + msg.getId(),
      };
      return ContentService.createTextOutput(
        JSON.stringify({ ok: true, message: result }),
      ).setMimeType(ContentService.MimeType.JSON);
    } catch (err) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: err.message }),
      ).setMimeType(ContentService.MimeType.JSON);
    }
  }

  // ── GraphRAG 검색 후 라벨에 메일 추가 ──
  if (action === "addLabelBySearch") {
    const labelName = (data.labelName || "").trim();
    const messageIds = data.messageIds || [];

    if (!labelName || messageIds.length === 0) {
      return ContentService.createTextOutput(
        JSON.stringify({ ok: false, error: "라벨명과 메일 ID가 필요합니다." }),
      ).setMimeType(ContentService.MimeType.JSON);
    }

    let label = GmailApp.getUserLabelByName(labelName);
    if (!label) label = GmailApp.createLabel(labelName);

    let successCount = 0;
    messageIds.forEach(function (mid) {
      try {
        const msg = GmailApp.getMessageById(mid);
        msg.getThread().addLabel(label);
        successCount++;
      } catch (e) {
        Logger.log("addLabel error for " + mid + ": " + e.message);
      }
    });

    return ContentService.createTextOutput(
      JSON.stringify({
        ok: true,
        labelName: label.getName(),
        addedCount: successCount,
      }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 라벨 삭제 ──
  if (action === "deleteLabel") {
    const labelName = (data.labelName || "").trim();
    const label = GmailApp.getUserLabelByName(labelName);
    if (label) label.deleteLabel();
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 메일에서 라벨 제거 ──
  if (action === "removeLabelFromMessage") {
    const labelName = (data.labelName || "").trim();
    const messageId = (data.messageId || "").trim();
    const msg = GmailApp.getMessageById(messageId);
    const label = GmailApp.getUserLabelByName(labelName);
    if (msg && label) msg.getThread().removeLabel(label);
    return ContentService.createTextOutput(
      JSON.stringify({ ok: true }),
    ).setMimeType(ContentService.MimeType.JSON);
  }

  // ── 알 수 없는 action ──
  return ContentService.createTextOutput(
    JSON.stringify({ error: "unknown action" }),
  ).setMimeType(ContentService.MimeType.JSON);
}

// HTML 파일 include 유틸
function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

// 받은 편지함 상단 스레드 제목 조회 (테스트용)
function getInboxTopSubjects(limit) {
  const n = Math.max(1, Math.min(Number(limit || 5), 20));
  const threads = GmailApp.getInboxThreads(0, n);
  return threads.map((t) => t.getFirstMessageSubject());
}

// 최근 받은편지함 스레드에 라벨 적용
function labelRecentInboxThreads(labelName, n) {
  const limit = Math.max(1, Math.min(Number(n || 5), 50));
  const name = String(labelName || "").trim();
  if (!name) throw new Error("labelName이 비어있습니다.");

  let label = GmailApp.getUserLabelByName(name);
  if (!label) label = GmailApp.createLabel(name);

  const threads = GmailApp.getInboxThreads(0, limit);
  threads.forEach((t) => t.addLabel(label));

  return {
    ok: true,
    labeledCount: threads.length,
    labelName: label.getName(),
  };
}

// Flask 서버에서 그래프 데이터 가져오기
function getGraphData() {
  const url = TunnelURL + "/graph-data";

  const res = UrlFetchApp.fetch(url, {
    method: "get",
    headers: {
      "ngrok-skip-browser-warning": "1",
      "User-Agent": "AppsScript/1.0",
    },
    followRedirects: true,
    muteHttpExceptions: true,
  });

  const code = res.getResponseCode();
  const text = res.getContentText();

  if (code !== 200) {
    throw new Error(
      "graph-data fetch failed: " + code + " / " + text.slice(0, 200),
    );
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error("graph-data JSON parse failed. head=" + text.slice(0, 200));
  }

  if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
    throw new Error(
      "graph-data 형식이 올바르지 않습니다. {nodes, edges} 필요. body_head=" +
        text.slice(0, 200),
    );
  }

  return data;
}

// 메일 목록 가져오기 (D3 시각화용)
function getInboxMessages(limit) {
  const n = Math.max(1, Math.min(Number(limit || 20), 50));
  const myEmail = Session.getActiveUser().getEmail();
  const threads = GmailApp.search("in:inbox", 0, n);

  const result = [];
  threads.forEach(function (thread) {
    const msg = thread.getMessages()[0];
    const from = msg.getFrom() || "";
    const direction = from.includes(myEmail) ? "발신" : "수신";
    const atts = msg.getAttachments({ includeInlineImages: false });

    result.push({
      id: msg.getId(),
      subject: msg.getSubject() || "(제목 없음)",
      from: from,
      to: msg.getTo() || "",
      date: msg.getDate().toLocaleDateString("ko-KR"),
      direction: direction,
      hasAttachment: atts.length > 0,
    });
  });
  return result;
}

// 메일 휴지통으로 이동
function trashMessage(messageId) {
  const msg = GmailApp.getMessageById(messageId);
  msg.moveToTrash();
  return { ok: true, id: messageId };
}

// 메일 완전 삭제
function deleteMessage(messageId) {
  Gmail.Users.Messages.remove("me", messageId);
  return { ok: true, id: messageId };
}
