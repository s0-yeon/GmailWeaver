// ========== 설정 ==========
var MAX_MESSAGE_LENGTH = 40;
var TunnelURL = "https://detainable-thumbless-arnav.ngrok-free.dev"; // 테스트할 때마다 수시로 변경

// ========== 홈페이지 ==========
function onHomepage(e) {
  // 📧 최근 메일 섹션
  var emailSection = CardService.newCardSection().setHeader("📧 최근 메일 5개");
  
  // 첫 메일 전송 (기존 코드 유지)
  try {
    sendFirstMail();
  } catch (error) {
    Logger.log("⚠️ sendFirstMail 오류: " + error.toString());
  }
  
  var threads = GmailApp.getInboxThreads(0, 5);
  for (var i = 0; i < threads.length; i++) {
    var threadSubject = threads[i].getFirstMessageSubject();
    emailSection.addWidget(
      CardService.newTextParagraph().setText((i + 1) + ". " + threadSubject)
    );
  }
  
  // 메일 내보내기 (기존 코드 유지)
  try {
    exportAllInboxAndSentIntoOneTxt();
  } catch (error) {
    Logger.log("⚠️ exportAllInboxAndSentIntoOneTxt 오류: " + error.toString());
  }

  // 📁 드라이브 섹션
  var driveSection = CardService.newCardSection().setHeader("📁 드라이브 파일 2개");
  var files = DriveApp.getFiles();
  var fileCount = 0;
  while (files.hasNext() && fileCount < 2) {
    var file = files.next();
    driveSection.addWidget(
      CardService.newTextParagraph().setText((fileCount + 1) + ". " + file.getName())
    );
    fileCount++;
  }

  // 💬 서버 질의 섹션
  var inputSection = CardService.newCardSection().setHeader("💬 서버 질의");
  
  var input = CardService.newTextInput()
    .setFieldName("message")
    .setTitle("서버로 보낼 메시지")
    .setHint("메시지를 입력하세요 (필수)")
    .setMultiline(true);

  var action = CardService.newAction()
    .setFunctionName("sendMessageToServer")
    .setLoadIndicator(CardService.LoadIndicator.SPINNER);  // 로딩 표시

  var button = CardService.newTextButton()
    .setText("서버로 전송")
    .setOnClickAction(action);

  inputSection.addWidget(input);
  inputSection.addWidget(button);

  // 📊 그래프 섹션
  var graphSection = CardService.newCardSection()
    .setHeader("📊 그래프 확인");
  
  var graphButton = CardService.newTextButton()
    .setText("📊 그래프 보기")
    .setOpenLink(CardService.newOpenLink()
      .setUrl(TunnelURL + "/"))
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED);
  
  graphSection.addWidget(graphButton);

  return CardService.newCardBuilder()
    .addSection(emailSection)
    .addSection(driveSection)
    .addSection(inputSection)
    .addSection(graphSection)
    .build();
}

// ========== 서버 질의 ==========
function sendMessageToServer(e) {
  // ✅ 수정: e가 undefined인 경우 처리
  if (!e) {
    Logger.log("❌ 이벤트 객체가 없습니다.");
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("❌ 이벤트 객체가 전달되지 않았습니다.")
        .setType(CardService.NotificationType.ERROR))
      .build();
  }

  var text = "";
  
  try {
    // 방법 1: formInput에서 추출
    if (e.formInput && e.formInput.message) {
      text = e.formInput.message;
      Logger.log("✅ formInput에서 메시지 추출: " + text);
    }
    // 방법 2: commonEventObject에서 추출 (백업)
    else if (e.commonEventObject && e.commonEventObject.formInputs) {
      var formInputs = e.commonEventObject.formInputs;
      if (formInputs.message && formInputs.message.stringInputs) {
        text = formInputs.message.stringInputs.value[0];
        Logger.log("✅ commonEventObject에서 메시지 추출: " + text);
      }
    }
    // 방법 3: parameters에서 추출 (드물지만 가능)
    else if (e.parameters && e.parameters.message) {
      text = e.parameters.message;
      Logger.log("✅ parameters에서 메시지 추출: " + text);
    }
    // 방법 4: 전체 이벤트 로깅 (디버깅용)
    else {
      Logger.log("⚠️ 이벤트 객체 전체:");
      Logger.log(JSON.stringify(e, null, 2));
    }
  } catch (error) {
    Logger.log("❌ 메시지 추출 오류: " + error.toString());
    Logger.log("이벤트 객체: " + JSON.stringify(e));
  }

  // 빈 메시지 검증
  if (!text || text.trim() === "") {
    Logger.log("❌ 메시지가 비어있습니다.");
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("❌ 메시지를 입력해주세요!")
        .setType(CardService.NotificationType.ERROR))
      .build();
  }

  Logger.log("📤 서버로 전송할 메시지: " + text);

  try {
    var response = UrlFetchApp.fetch(TunnelURL + "/run-query", {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({
        message: text.trim(),
        resMethod: "local",
        resType: "text"
      }),
      muteHttpExceptions: true  // 에러 상세 확인
    });

    var responseCode = response.getResponseCode();
    var raw = response.getContentText();
    
    Logger.log("📥 응답 코드: " + responseCode);
    Logger.log("📥 응답 내용: " + raw);

    var resultText = "";
    
    if (responseCode === 200) {
      try {
        var data = JSON.parse(raw);
        
        // Flask 응답 구조 처리
        if (data.status === "success") {
          resultText = data.response || data.result || "응답을 받았습니다.";
        } else if (data.result) {
          // 기존 코드 호환성
          resultText = data.result;
        } else if (data.error) {
          resultText = "❌ 오류: " + data.error;
        } else {
          resultText = raw;
        }
      } catch (parseError) {
        Logger.log("⚠️ JSON 파싱 실패, 원문 사용");
        resultText = raw;
      }
    } else {
      // 에러 응답 처리
      try {
        var errorData = JSON.parse(raw);
        resultText = "❌ 서버 오류 (" + responseCode + "): " + (errorData.error || raw);
      } catch (e) {
        resultText = "❌ 서버 오류 (" + responseCode + "): " + raw;
      }
    }

    // 그래프 버튼 섹션
    var graphSection = CardService.newCardSection()
      .setHeader("📊 그래프 확인");
    
    var graphButton = CardService.newTextButton()
      .setText("📊 그래프 보기")
      .setOpenLink(CardService.newOpenLink()
        .setUrl(TunnelURL + "/"))
      .setTextButtonStyle(CardService.TextButtonStyle.FILLED);
    
    graphSection.addWidget(graphButton);

    // 응답 카드
    var card = CardService.newCardBuilder()
      .addSection(
        CardService.newCardSection()
          .setHeader("✅ 서버 응답")
          .addWidget(CardService.newTextParagraph().setText(resultText))
      )
      .addSection(graphSection)
      .build();

    return CardService.newActionResponseBuilder()
      .setNavigation(CardService.newNavigation().pushCard(card))
      .build();

  } catch (error) {
    Logger.log("❌ 요청 실패: " + error.toString());
    
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification()
        .setText("❌ 서버 연결 실패: " + error.toString())
        .setType(CardService.NotificationType.ERROR))
      .build();
  }
}

// ========== 드라이브 관련 ==========
function onDriveItemsSelected(e) {
  var driveSection = CardService.newCardSection()
    .setHeader("📁 드라이브 파일 2개");
  
  var files = DriveApp.getFiles();
  var fileCount = 0;
  while (files.hasNext() && fileCount < 2) {
    var file = files.next();
    driveSection.addWidget(
      CardService.newTextParagraph()
        .setText((fileCount + 1) + '. ' + file.getName())
    );
    fileCount++;
  }
  
  return CardService.newCardBuilder()
    .addSection(driveSection)
    .build();
}

// ========== 유틸리티 함수 ==========
function truncate(message) {
  // ✅ 수정: undefined, null 체크 추가
  if (!message) {
    Logger.log("⚠️ truncate: 메시지가 비어있습니다.");
    return "";
  }
  
  // 문자열이 아닌 경우 문자열로 변환
  message = String(message);
  
  if (message.length > MAX_MESSAGE_LENGTH) {
    message = message.slice(0, MAX_MESSAGE_LENGTH);
    var lastSpace = message.lastIndexOf(" ");
    if (lastSpace > 0) {
      message = message.slice(0, lastSpace) + "...";
    } else {
      message = message + "...";
    }
  }
  return message;
}

// ========== 첫 번째 코드에서 가져온 함수들 ==========
function sendFirstMail() {
  // 첫 번째 코드의 구현 내용
  // 함수가 정의되어 있지 않으면 주석 처리하거나 빈 함수로 유지
  Logger.log("sendFirstMail 실행됨");
}

function exportAllInboxAndSentIntoOneTxt() {
  // 첫 번째 코드의 구현 내용
  // 함수가 정의되어 있지 않으면 주석 처리하거나 빈 함수로 유지
  Logger.log("exportAllInboxAndSentIntoOneTxt 실행됨");
}

// ========== 디버깅용 테스트 함수 ==========
function testSendMessage() {
  // ✅ 수정: 올바른 테스트 이벤트 객체 생성
  var testEvent = {
    formInput: {
      message: "테스트 메시지입니다"
    },
    commonEventObject: {
      formInputs: {
        message: {
          stringInputs: {
            value: ["테스트 메시지입니다"]
          }
        }
      }
    }
  };
  
  Logger.log("=== 테스트 시작 ===");
  var result = sendMessageToServer(testEvent);
  Logger.log("테스트 결과: " + JSON.stringify(result));
  Logger.log("=== 테스트 종료 ===");
}

function testTruncate() {
  // ✅ truncate 함수 테스트
  Logger.log("=== truncate 테스트 시작 ===");
  
  Logger.log("1. 정상 문자열: " + truncate("안녕하세요 반갑습니다"));
  Logger.log("2. 긴 문자열: " + truncate("이것은 매우 긴 메시지입니다. 최대 길이를 초과하는 텍스트를 테스트합니다."));
  Logger.log("3. 빈 문자열: " + truncate(""));
  Logger.log("4. undefined: " + truncate(undefined));
  Logger.log("5. null: " + truncate(null));
  Logger.log("6. 숫자: " + truncate(12345));
  
  Logger.log("=== truncate 테스트 종료 ===");
}
