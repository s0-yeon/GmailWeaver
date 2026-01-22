var MAX_MESSAGE_LENGTH = 40;
var TunnelURL = "https://interatrial-tana-wishfully.ngrok-free.dev"; // 테스트 할때 마다 수시로 변경

function onHomepage(e) {
  var emailSection = CardService.newCardSection().setHeader("📧 최근 메일 5개");
  sendFirstMail();
  var threads = GmailApp.getInboxThreads(0, 5);
  for (var i = 0; i < threads.length; i++) {
    var threadSubject = threads[i].getFirstMessageSubject();
    emailSection.addWidget(
      CardService.newTextParagraph().setText(i + 1 + ". " + threadSubject)
    );
  }
  exportAllInboxAndSentIntoOneTxt();

  var driveSection =
    CardService.newCardSection().setHeader("📁 드라이브 파일 2개");

  var files = DriveApp.getFiles();
  var fileCount = 0;
  while (files.hasNext() && fileCount < 2) {
    var file = files.next();
    driveSection.addWidget(
      CardService.newTextParagraph().setText(
        fileCount + 1 + ". " + file.getName()
      )
    );
    fileCount++;
  }

  var inputSection = CardService.newCardSection().setHeader("💬 서버 질의");
  var input = CardService.newTextInput()
    .setFieldName("message")
    .setTitle("서버로 보낼 메시지")
    .setHint("메시지를 입력하세요")
    .setMultiline(true);

  var action = CardService.newAction().setFunctionName("sendMessageToServer");
  
  var button = CardService.newTextButton()
    .setText("서버로 전송")
    .setOnClickAction(action);

  inputSection.addWidget(input);
  inputSection.addWidget(button);

  return CardService.newCardBuilder()
    .addSection(emailSection)
    .addSection(driveSection)
    .addSection(inputSection)
    .build();
}

function sendMessageToServer(e) {
  var text = (e && e.formInput && e.formInput.message) ? e.formInput.message : "";

 var response = UrlFetchApp.fetch(TunnelURL + "/run-query", {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify({
      message: text,
      resMethod: "local",
      resType: "text"
    })
  });
 var raw = response.getContentText();
  Logger.log("Raw response: " + raw);

  var resultText = raw;
  try {
    var data = JSON.parse(raw);
    resultText = data.result || raw;
  } catch (err) {
    resultText = raw;
  }

  // 응답을 카드로 보여주기
  var card = CardService.newCardBuilder()
    .addSection(
      CardService.newCardSection()
        .setHeader("✅ 서버 응답")
        .addWidget(CardService.newTextParagraph().setText(resultText))
    )
    .build();

  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
    }

function truncate(message) {
  if (message.length > MAX_MESSAGE_LENGTH) {
    message = message.slice(0, MAX_MESSAGE_LENGTH);
    message = message.slice(0, message.lastIndexOf(" ")) + "...";
  }
  return message;
}
