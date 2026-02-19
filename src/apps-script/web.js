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

const TUNNEL_URL = "https://unmatching-sandy-hydrocinnamyl.ngrok-free.dev";

function getGraphData() {
  const url = TUNNEL_URL + "/graph-data";

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

  // 디버그 로그
  Logger.log("[getGraphData] url=" + url);
  Logger.log("[getGraphData] status=" + code);
  Logger.log("[getGraphData] body_head=" + text.slice(0, 200));

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