var MAX_MESSAGE_LENGTH = 40;
var TunnelURL = "https://interatrial-tana-wishfully.ngrok-free.dev"; // 테스트 할때 마다 수시로 변경
const WEBAPP_URL = "https://script.google.com/macros/s/AKfycbz6IrE751nxencGl5KXbeUaZS2GtT7Jcg9J5vOMSVY/dev";


function onHomepage(e) {
  // ===== 질의용 =====
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

  inputSection.addWidget(input);
  inputSection.addWidget(querySendButton);

  // =========================

  // ===== gmail 데이터 플라스크 서버로 전송 =====
  var extractGmailButton = CardService.newTextButton()
    .setText("서버로 Gmail 내역 전송")
    .setOnClickAction(extractGmailAction);  
  
  
  inputSection.addWidget(extractGmailButton);

  // =========================

  // 메일 라벨링 섹션 추가
  const labelSection = CardService.newCardSection().setHeader("🏷 최근 메일 라벨링");

  const labelInput = CardService.newTextInput()
    .setFieldName("labelName")
    .setTitle("라벨 이름")
    .setHint("예: 프로젝트, 광고, 중요");

  const nInput = CardService.newTextInput()
    .setFieldName("n")
    .setTitle("최근 몇 개?")
    .setValue("5");

  const btn = CardService.newTextButton()
    .setText("라벨링 실행")
    .setOnClickAction(
      CardService.newAction().setFunctionName("onClickApplyLabelRecent_")
    );

  const openWebBtn = CardService.newTextButton()
    .setText("웹 페이지 열기 (Web App)")
    .setOpenLink(
      CardService.newOpenLink()
        .setUrl(WEBAPP_URL)
        .setOpenAs(CardService.OpenAs.FULL_SIZE) // 새 창(풀페이지)로
    ); 


  labelSection.addWidget(labelInput);
  labelSection.addWidget(nInput);
  labelSection.addWidget(btn);

  // =========================

  const openWebSection = CardService.newCardSection()
    .setHeader("테스트")
    .addWidget(CardService.newTextParagraph().setText("버튼을 누르면 Web App 페이지가 열립니다."))
    .addWidget(openWebBtn);

  return CardService.newCardBuilder()
    .addSection(inputSection)
    .addSection(openWebSection)
    .addSection(labelSection)
    .build();
}





