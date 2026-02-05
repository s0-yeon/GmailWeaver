var MAX_MESSAGE_LENGTH = 40;
var TunnelURL = "https://laevorotatory-nonnutritively-nelle.ngrok-free.dev"; // 본인의 ngrok 주소로 변경 필수!

function deleteLatestEmail() {
  // 수신함의 가장 위(최근) 메일 1개를 가져옴 (Index 0)
  var threads = GmailApp.getInboxThreads(0, 1); 

  if (threads.length > 0) {
    var targetThread = threads[0];
    var subject = targetThread.getFirstMessageSubject(); // 삭제될 메일 제목
    
    // 휴지통으로 이동
    targetThread.moveToTrash(); 
    
    // 사용자에게 어떤 메일이 삭제되었는지 알림
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("최신 메일 삭제 완료: [" + subject + "]. 휴지통을 확인해보세요."))
      .build();
  } else {
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("삭제할 메일이 없습니다."))
      .build();
  }
}

function onHomepage(e) {
  var inputSection = CardService.newCardSection().setHeader("💬 검색어를 입력하세요");
  
  // 1. 분석 질의 입력창
  var input = CardService.newTextInput()
    .setFieldName("message")
    .setTitle("분석 요청")
    .setHint("메시지를 입력하세요")
    .setMultiline(true);
  inputSection.addWidget(input);

  // 2. 서버 전송 버튼
  var sendAction = CardService.newAction().setFunctionName("sendMessageToServer");
  inputSection.addWidget(CardService.newTextButton().setText("서버로 전송 및 분석").setOnClickAction(sendAction));

  // 3. (신규 추가) 보안 폴더 이동 버튼 🔒
  var securityAction = CardService.newAction().setFunctionName("moveLatestMailToSecurity");
  inputSection.addWidget(
    CardService.newTextButton()
      .setText("🔒 최신 메일 '보안' 폴더 이동")
      .setOnClickAction(securityAction)
  );

  // 4. 메일 삭제 테스트 버튼 (추가됨)
  var deleteAction = CardService.newAction().setFunctionName("deleteLatestEmail");
  inputSection.addWidget(
    CardService.newTextButton()
      .setText("🗑️ 가장 최근 메일 삭제 (휴지통)")
      .setOnClickAction(deleteAction)
  );

  return CardService.newCardBuilder().addSection(inputSection).build();
}

// [메일 삭제 함수]
function deleteFourthEmail() {
  // 4번째 메일 스레드 가져오기 (인덱스 3)
  var threads = GmailApp.getInboxThreads(3, 1); 

  if (threads.length > 0) {
    var subject = threads[0].getFirstMessageSubject();
    
    // 실제 삭제(휴지통 이동) 실행
    threads[0].moveToTrash(); 
    
    // 알림 메시지 출력
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("성공: '" + subject + "' 메일을 삭제했습니다."))
      .build();
  } else {
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("오류: 4번째 메일을 찾을 수 없습니다."))
      .build();
  }
}

// [서버 전송 함수]
function sendMessageToServer(e) {
  var text = (e && e.formInput && e.formInput.message) ? e.formInput.message : "내용 없음";
  var threads = GmailApp.getInboxThreads(0, 1); 
  var latestMailId = "";
  if (threads.length > 0) {
    latestMailId = threads[0].getMessages()[threads[0].getMessageCount() - 1].getId();
  }

  var response = UrlFetchApp.fetch(TunnelURL + "/run-query", {
    method: "post",
    contentType: "application/json",
    headers: { "ngrok-skip-browser-warning": "1" },
    payload: JSON.stringify({
      message: text,
      resMethod: "local",
      resType: "text",
      mail_id: latestMailId 
    })
  });

  var raw = response.getContentText();
  var resultText = "";
  var evidenceId = "";

  try {
    var data = JSON.parse(raw);
    resultText = data.result || raw;
    evidenceId = data.evidence_id || "";
  } catch (err) {
    resultText = raw;
  }

  var section = CardService.newCardSection().setHeader("✅ 분석 결과");
  section.addWidget(CardService.newTextParagraph().setText(resultText));

  if (evidenceId) {
    var mailUrl = "https://mail.google.com/mail/u/0/#inbox/" + evidenceId;
    section.addWidget(
      CardService.newTextButton()
        .setText("📄 근거 메일 확인하기")
        .setOpenLink(CardService.newOpenLink().setUrl(mailUrl))
    );
  }

  // --- 추가된 버튼 2: AI 추천 폴더 이동 (신규) ---
  if (data.suggested_category) {
    var moveAction = CardService.newAction()
        .setFunctionName("moveMailToFolder")
        .setParameters({ "category": data.suggested_category, "mailId": data.evidence_id });
    section.addWidget(CardService.newTextButton().setText("📁 [" + data.suggested_category + "] 폴더로 정리").setOnClickAction(moveAction));
  }

  var card = CardService.newCardBuilder().addSection(section).build();
  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
}

// 폴더 이동 실행 함수 (추가)
function moveMailToFolder(e) {
  var category = e.parameters.category;
  var mailId = e.parameters.mailId;
  var thread = GmailApp.getMessageById(mailId).getThread();
  var label = GmailApp.getUserLabelByName(category) || GmailApp.createLabel(category);
  thread.addLabel(label).moveToArchive();
  return CardService.newActionResponseBuilder().setNotification(CardService.newNotification().setText(category + " 폴더로 이동 완료!")).build();
}

function moveLatestMailToSecurity() {
  // 1. 받은편지함에서 가장 최근 메일 스레드 1개를 가져옵니다.
  var threads = GmailApp.getInboxThreads(0, 1);
  
  if (threads.length > 0) {
    var latestThread = threads[0];
    var category = "보안"; // 이동할 라벨 이름
    
    // 2. '보안' 라벨이 없으면 생성, 있으면 가져오기
    var label = GmailApp.getUserLabelByName(category) || GmailApp.createLabel(category);
    
    // 3. 라벨 추가 및 받은편지함에서 보관처리(이동)
    latestThread.addLabel(label);
    latestThread.moveToArchive();
    
    // 결과 알림 (로그 확인용)
    Logger.log("최신 메일이 '" + category + "' 폴더로 이동되었습니다.");
    
    return CardService.newActionResponseBuilder()
        .setNotification(CardService.newNotification().setText("최신 메일을 '보안' 폴더로 이동했습니다!"))
        .build();
  } else {
    return CardService.newActionResponseBuilder()
        .setNotification(CardService.newNotification().setText("이동할 메일이 없습니다."))
        .build();
  }
}