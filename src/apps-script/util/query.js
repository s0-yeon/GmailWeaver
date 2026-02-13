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