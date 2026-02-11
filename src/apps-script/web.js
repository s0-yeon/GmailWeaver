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


function getGraphData() {
  const files = DriveApp.getFilesByName("graphml_data.json");
  if (!files.hasNext()) {
    throw new Error("Drive에서 graphml_data.json 파일을 찾을 수 없습니다.");
  }

  const file = files.next();
  const text = file.getBlob().getDataAsString("UTF-8");
  const data = JSON.parse(text);

  // 기대 형태: { nodes: [...], edges: [...] }
  if (!data || !Array.isArray(data.nodes) || !Array.isArray(data.edges)) {
    throw new Error("graphml_data.json 형식이 올바르지 않습니다. {nodes, edges}가 필요합니다.");
  }
  return data;
}