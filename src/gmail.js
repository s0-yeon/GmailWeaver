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

function onGmailCompose(e) {
  var header = CardService.newCardHeader()
    .setTitle("Insert cat")
    .setSubtitle("Add a custom cat image to your email message.");
  var input = CardService.newTextInput()
    .setFieldName("text")
    .setTitle("Caption")
    .setHint("What do you want the cat to say?");
  var action = CardService.newAction().setFunctionName("onGmailInsertCat");
  var button = CardService.newTextButton()
    .setText("Insert cat")
    .setOnClickAction(action)
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED);
  var buttonSet = CardService.newButtonSet().addButton(button);
  var section = CardService.newCardSection()
    .addWidget(input)
    .addWidget(buttonSet);
  var card = CardService.newCardBuilder().setHeader(header).addSection(section);
  return card.build();
}

function onGmailInsertCat(e) {
  var text = e.formInput.text;
  var now = new Date();
  var imageUrl = "https://cataas.com/cat";
  if (text) {
    var caption = text.replace(/\//g, " ");
    imageUrl += Utilities.formatString(
      "/says/%s?time=%s",
      encodeURIComponent(caption),
      now.getTime()
    );
  }
  var imageHtmlContent =
    '<img style="display: block; max-height: 300px;" src="' + imageUrl + '"/>';
  var response = CardService.newUpdateDraftActionResponseBuilder()
    .setUpdateDraftBodyAction(
      CardService.newUpdateDraftBodyAction()
        .addUpdateContent(
          imageHtmlContent,
          CardService.ContentType.MUTABLE_HTML
        )
        .setUpdateType(CardService.UpdateDraftBodyType.IN_PLACE_INSERT)
    )
    .build();
  return response;
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
