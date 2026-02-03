var MAX_MESSAGE_LENGTH = 40;
var TunnelURL = "https://interatrial-tana-wishfully.ngrok-free.dev"; // 테스트 할때 마다 수시로 변경

function onHomepage(e) {

  sendFirstMail();   // 서버 통신 확인용

  var inputSection = CardService.newCardSection().setHeader("💬 서버 질의");
  var input = CardService.newTextInput()
    .setFieldName("message")
    .setTitle("서버로 보낼 메시지")
    .setHint("메시지를 입력하세요")
    .setMultiline(true);

  var sendMessageToServerAction = CardService.newAction().setFunctionName("sendMessageToServer");
  var extractGmailAction = CardService.newAction().setFunctionName("exportAllInboxAndSentIntoOneTxt");
  
  var querySendButton = CardService.newTextButton()
    .setText("서버로 질의전송")
    .setOnClickAction(sendMessageToServerAction);

  var extractGmailButton = CardService.newTextButton()
    .setText("서버로 Gmail 내역 전송")
    .setOnClickAction(extractGmailAction);  


  inputSection.addWidget(input);
  inputSection.addWidget(querySendButton);
  inputSection.addWidget(extractGmailButton);

  return CardService.newCardBuilder()
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
