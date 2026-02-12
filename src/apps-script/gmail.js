function onGmailMessage(e) {
  var emailSection = CardService.newCardSection().setHeader("📧 최근 메일 5개");

  var threads = GmailApp.getInboxThreads(0, 5);
  for (var i = 0; i < threads.length; i++) {
    var threadSubject = threads[i].getFirstMessageSubject();
    emailSection.addWidget(
      CardService.newTextParagraph().setText(i + 1 + ". " + threadSubject)
    );
  }

  return CardService.newCardBuilder().addSection(emailSection).build();
}



function sendFirstMail() {
  // 1) 최근 메일 1개 제목 가져오기
  var threads = GmailApp.getInboxThreads(0, 1);
  var subject = threads[0].getMessages()[0].getSubject();

  // 2) 제목은 값만 인코딩
  var encodedSubject = encodeURIComponent(subject);

  // 3) "진짜 URL" 만들기 (ngrok 주소 + 엔드포인트 + 쿼리)
  var requestUrl = TunnelURL + "/mails?title=" + encodedSubject;

  // 4) ngrok 경고 페이지 우회 헤더 + 응답 확인용 로그
  var res = UrlFetchApp.fetch(requestUrl, {
    method: "get",
    muteHttpExceptions: true,
    headers: {
      "ngrok-skip-browser-warning": "1",
    },
  });

  var status = res.getResponseCode();
  var body = res.getContentText();

  Logger.log("HTTP status: " + status);
  Logger.log("Response body: " + body);

  // 5) 디버깅 로그 (실행 기록에서 확인)
  Logger.log("requestUrl: " + requestUrl);
  Logger.log("status: " + res.getResponseCode());
  Logger.log("body(head): " + res.getContentText().slice(0, 200));
}
