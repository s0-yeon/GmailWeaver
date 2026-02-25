function doGet(e) {
  return HtmlService.createHtmlOutputFromFile("index")
    .setTitle("Web UI");
}

/**
 * (선택) Web App 페이지에서 GmailApp 호출 테스트용
 * index.html에서 google.script.run으로 호출함
 */
function getInboxTopSubjects(limit) {
  const n = Math.max(1, Math.min(Number(limit || 5), 20));
  const threads = GmailApp.getInboxThreads(0, n);
  return threads.map(t => t.getFirstMessageSubject());
}

// 브라우저에서 메일 라벨링 요청 처리함수
function labelRecentInboxThreads(labelName, n) {
  const limit = Math.max(1, Math.min(Number(n || 5), 50));
  const name = String(labelName || "").trim();
  if (!name) throw new Error("labelName이 비어있습니다.");

  // 라벨 가져오거나 없으면 생성
  let label = GmailApp.getUserLabelByName(name);
  if (!label) label = GmailApp.createLabel(name);

  // 최근 받은편지함 스레드 가져와 라벨 적용
  const threads = GmailApp.getInboxThreads(0, limit);
  threads.forEach(t => t.addLabel(label));

  return {
    ok: true,
    labeledCount: threads.length,
    labelName: label.getName(),
  };
}


// ─────────────────────────────────────────
// Flask 서버에서 그래프 데이터 가져오기
// TunnelURL은 common.js에 정의 (ngrok 주소)
// ─────────────────────────────────────────

function getGraphData() {
  const url = TunnelURL + "/graph-data";

  const res = UrlFetchApp.fetch(url, {
    method: "get",
    headers: {
      "ngrok-skip-browser-warning": "1",
      "User-Agent": "AppsScript/1.0"
    },
    followRedirects: true,
    muteHttpExceptions: true,
  });

  const code = res.getResponseCode();
  const text = res.getContentText();

  if (code !== 200) {
    throw new Error("graph-data fetch failed: " + code + " / " + text.slice(0, 200));
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error("graph-data JSON parse failed. head=" + text.slice(0, 200));
  }

  // 기대 형태: { nodes: [...], edges: [...] }
  if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
    throw new Error("graph-data 형식이 올바르지 않습니다. {nodes, edges} 필요. body_head=" + text.slice(0, 200));
  }

  return data;
}

// ─────────────────────────────────────────
// 메일 목록 가져오기 (D3 줌동그라미용)
// exportAllInboxAndSentIntoOneTxt()의 필드 구조 참고해서
// 수신/발신 구분, 첨부파일 여부까지 포함
// ─────────────────────────────────────────
function getInboxMessages(limit) {
  const n = Math.max(1, Math.min(Number(limit || 20), 50));
  // 현재 로그인 계정 이메일 (수신/발신 구분에 사용)
  const myEmail = Session.getActiveUser().getEmail();
  // getInboxThreads() 대신 search()로 더 정밀한 조건 지정 가능
  const threads = GmailApp.search("in:inbox", 0, n);

  const result = [];
  threads.forEach(function(thread) {
    const msg = thread.getMessages()[0]; // 스레드 대표 메시지
    const from = msg.getFrom() || "";
    // 발신자에 내 이메일이 포함되면 "발신", 아니면 "수신"
    const direction = from.includes(myEmail) ? "발신" : "수신";
    // 첨부파일 여부 (인라인 이미지 제외)
    const atts = msg.getAttachments({ includeInlineImages: false });

    result.push({
      id: msg.getId(),                                  // Gmail 고유 ID → 삭제 시 사용
      subject: msg.getSubject() || "(제목 없음)",
      from: from,
      to: msg.getTo() || "",
      date: msg.getDate().toLocaleDateString("ko-KR"), // "2026. 2. 25." 형식
      direction: direction,                             // "수신" or "발신"
      hasAttachment: atts.length > 0                   // 동그라미 색상 구분용
    });
  });
  return result;
}


// ─────────────────────────────────────────
// 메일 휴지통으로 이동 (복구 가능, 30일 보관)
// gmail.modify scope로 동작 (appsscript.json에 이미 있음)
// ─────────────────────────────────────────
function trashMessage(messageId) {
  const msg = GmailApp.getMessageById(messageId);
  msg.moveToTrash(); // 휴지통 이동 (복구 가능)
  return { ok: true, id: messageId };
}


// ─────────────────────────────────────────
// 메일 완전 삭제 (복구 불가)
// Gmail Advanced Service 필요
// Apps Script 에디터 → 서비스(+) → Gmail API 추가 후 사용 가능
// ─────────────────────────────────────────
function deleteMessage(messageId) {
  Gmail.Users.Messages.remove("me", messageId); // 영구 삭제
  return { ok: true, id: messageId };
}