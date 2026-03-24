// src/apps-script/util/query.js (내부 로직)

// 애드온 UI에서 "스마트 검색" 버튼을 눌렀을 때 호출되는 함수 
function onSmartSearch(e) {    // e: Apps Script 이벤트 객체. e.commonEventObject.formInputs 안에 사용자 입력값이 담겨 있음
    var inputs = (e && e.commonEventObject && e.commonEventObject.formInputs) || {};  // 이벤트 객체에서 폼 입력값 추출 && 체이닝으로 null 에러 방지
    var query  = (inputs.searchQuery && inputs.searchQuery.stringInputs)  
        ? inputs.searchQuery.stringInputs.value[0].trim() : ""; // 폼에서 searchQuery라는 이름의 입력값 추출. 없으면 빈 문자열. 앞뒤 공백 제거
    if (!query) return _toast("메시지를 입력해주세요.");     // 검색어 비어있으면 토스트 알림 띄우기
    var jobId;    // jobId(작업 번호)

    try {
        var res = UrlFetchApp.fetch(TunnelURL + "/run-query-async", {   // Flask 서버의 run-query-async로 POST 요청
            method: "post",   
            contentType: "application/json",
            headers: { "ngrok-skip-browser-warning": "1" },   // ngrok이 브라우저에 경고 페이지를 띄우는 것을 방지하는 헤더
            payload: JSON.stringify({ message: query, resMethod: "global", resType: "structured" })    // '구조화'로 응답 형식 지정
        });
        jobId = JSON.parse(res.getContentText()).jobId;     // 서버 응답 객체(res)에서 텍스트를 추출하여 JSON 객체로 변환한 후, 그 객체에서 jobId 값을 추출
    } catch (err) { return _toast("⚠️ 서버 연결 실패: " + err.message); }

    if (!jobId) return _toast("⚠️ 서버 응답 오류");
    return _pendingCard(query, jobId);   // 작업 번호가 존재하면 '처리 중(pending)' 카드를 화면에 출력
}

// 캘린더에서 일정 추출을 요청히는 함수
function _requestCalendarAsync(message, messageId) {    // message: 질의 내용(검색어) / messageId: 어떤 메일에서 트리거됐는지 확인하는 추적용 ID
    var jobId;
    try {
        var res = UrlFetchApp.fetch(TunnelURL + "/run-query-async", {   // Flask 서버로 POST 요청
            method: "post",
            contentType: "application/json",
            headers: { "ngrok-skip-browser-warning": "1" },
            payload: JSON.stringify({ message: message, resMethod: "local", resType: "calendar" })    // '캘린더용 JSON 형식'으로 응답 형식 지정
        });
        jobId = JSON.parse(res.getContentText()).jobId;
    } catch (err) { return _toast("⚠️ 서버 연결 실패: " + err.message); }

    if (!jobId) return _toast("⚠️ 서버 응답 오류");
    return _pendingCard("일정 분석 중...", jobId, "calendar", messageId);
}

// 결과 확인 버튼 이벤트 핸들러
function onCheckJobResult(e) {
  var params    = (e && e.commonEventObject && e.commonEventObject.parameters) || {};   // 파라미터 추출
  var jobId     = params.jobId     || "";   // 작업 번호
  var query     = params.query     || "";   // 검색어(질의 내용)
  var jobType   = params.jobType   || "query";  // 작업 종류 (일반 검색/캘린더 일정 추출 구분)
  var messageId = params.messageId || "";   

  if (!jobId) return _toast("jobId를 찾을 수 없습니다.");

  var data; // 서버 응답 데이터를 저장할 변수
  try { // 서버에서 상태 조회
    var res = UrlFetchApp.fetch(TunnelURL + "/job-status/" + jobId, {   // 해당 URL로 GET 요청 전송
      method: "get",
      headers: { "ngrok-skip-browser-warning": "1" }    // ngrok이 브라우저에 경고 페이지를 띄우는 것을 방지하는 헤더
    });
    data = JSON.parse(res.getContentText());    // 서버 응답 객체(res)에서 텍스트를 추출하여 JSON 객체로 변환한 후, data에 저장
  } catch (err) { return _toast("⚠️ 상태 확인 실패: " + err.message); }

  var status = data.status || "";

  if (status === "pending") {
    return _pendingCard(query, jobId, jobType, messageId, "⏳ 아직 처리 중입니다.\n잠시 후 다시 확인해주세요.");
  }

  if (status === "error") {
    return _toast("⚠️ 오류: " + (data.result || "알 수 없는 오류"));
  }

  if (jobType === "calendar") {     // 작업 종류가 '캘린더'인 경우, 캘린더 핸들러 호출
    return _handleCalendarResult(data.data || {});
  }

  var result = data.result || "";   // 서버 응답에서 결과값 추출
  var parsed = null;    
  try { parsed = JSON.parse(result); } catch(_) {}  // JSON으로 변환 시도

  if (parsed) {     // 변환 성공 시
    var intent = (parsed.intent || "").toLowerCase();   // intent(의도)별 분류 위해 대소문자 통일
    if (intent === "label"  && parsed.actions) return _toast(_executeLabelActions(parsed.actions));   // 라벨 붙이기
    if (intent === "delete" && parsed.actions) return _toast(_executeDeleteActions(parsed.actions));  // 삭제
    result = parsed.result || result;
  }
  // 변환 실패 시, parsed는 null 유지, 에러 무시

  return _answerCard(query, result);
}

// 서버가 분석해 준 일정 정보를 실제 Google Calendar에 등록하는 함수
function _handleCalendarResult(calData) {  // calData: 캘린더 객체
  var events = calData.events || []; 
  if (!events.length) return _toast("📅 날짜/일정 정보를 찾지 못했습니다.");

  var cal = CalendarApp.getDefaultCalendar();   // 사용자의 기본 캘린더 추출하여 cal에 저장
  var added = 0;    // 실제로 등록된 이벤트 개수 카운터

  events.forEach(function(ev) {     
    try {
      var start = new Date(ev.startTime);   // 시작 시간
      var end   = ev.endTime ? new Date(ev.endTime) : new Date(start.getTime() + 3600000);  // 종료 시간. 존재하지 않으면 시작 시간 + 1시간
      cal.createEvent(ev.title || "일정", start, end, { description: ev.description || "" });   // 실제 이벤트 생성. 
      added++;  // 카운트 증가
    } catch(_) {}
  });

  return _toast(added > 0 ? "📅 " + added + "개 일정이 캘린더에 등록되었습니다." : "⚠️ 일정 등록에 실패했습니다.");    // 루프 종료 후 토스트 반환
}

// '처리 중' 카드. 서버 비동기 작업 상태 관리의 중간 화면
// 서버가 아직 작업 중일 때, '검색 중...' 화면을 만들고,
// '결과 확인' 버튼에 필요한 정보를 심어 두는 UI 생성 함수
function _pendingCard(query, jobId, jobType, messageId, notice) {  // 질의, 작업 ID, 작업 종류(질의/일정 등록), 메일 ID, 안내 문구
  notice    = notice    || "분석 중입니다.\n완료 후 아래 버튼을 눌러 결과를 확인하세요.";
  jobType   = jobType   || "query";
  messageId = messageId || "";

  var card = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader()
      .setTitle("🔍 검색 중...")
      .setSubtitle(_truncate(query, 60)))
    .addSection(CardService.newCardSection()
      .addWidget(CardService.newDecoratedText()
        .setText(notice)
        .setStartIcon(CardService.newIconImage()
          .setIconUrl("https://www.gstatic.com/images/icons/material/system/1x/hourglass_empty_grey600_24dp.png")))
      .addWidget(CardService.newDivider())
      .addWidget(CardService.newTextButton()
        .setText("🔄 결과 확인")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(CardService.newAction()
          .setFunctionName("onCheckJobResult")
          .setParameters({
            jobId:     jobId,
            query:     _truncate(query, 200),
            jobType:   jobType,
            messageId: messageId
          })))
      .addWidget(CardService.newTextButton()
        .setText("← 홈으로")
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOnClickAction(CardService.newAction().setFunctionName("_goBackHome"))))
    .build();

  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
}

// 라벨링을 실행하는 함수
function _executeLabelActions(actions) {   // actions: [{ labelName: string, threadIds: string[] }, ...] 형태의 배열
  var total = 0;    // 라벨이 적용된 스레드 수 카운터
  actions.forEach(function(action) {    
    var lname = action.labelName || "";     // 라벨의 이름
    if (!lname) return;     
    var label = GmailApp.getUserLabelByName(lname) || GmailApp.createLabel(lname);  // 라벨 이름을 이용하여 라벨을 가져와 label 변수에 저장. 존재하지 않으면 라벨 생성 후 저장
    (action.threadIds || []).forEach(function(id) {        // 예외 무시
      try { var t = GmailApp.getThreadById(id); if (t) { t.addLabel(label); total++; } } catch(_) {}
    });
  });
  return total > 0 ? "✅ " + total + "개 스레드에 라벨 적용 완료" : "⚠️ 적용할 스레드를 찾지 못했습니다.";
}

// 주어진 스레드 목록을 삭제하는 함수
function _executeDeleteActions(actions) { 
    var deleted = 0;  // 삭제된 스레드 수 카운터
    
    actions.forEach(function(action) {
        (action.threadIds || []).forEach(function(id) {
            try {
                let t = GmailApp.getThreadById(id);
                if(t) {
                    t.moveToTrash();
                    deleted++;
                }
            } catch(_) { /* 삭제됐거나 접근 불가한 스레드 예외 무시 */ }
        });
    });
    
    return deleted > 0 
        ? "🗑️ " + deleted + "개 스레드 삭제 완료" 
        : "⚠️ 삭제할 스레드를 찾지 못했습니다.";
}

// 검색어와 답변을 받아 답변용 카드를 만드는 함수
function _answerCard(query, answer) {
  var card = CardService.newCardBuilder()   // 카드 객체 생성
    .setHeader(CardService.newCardHeader()  // 헤더 설정
      .setTitle("💬 검색 결과")     // 헤더 제목 설정 (고정 텍스트)
      .setSubtitle(_truncate(query, 60)))  // 쿼리(검색어)를 60자로 잘라 헤더 부제목으로 설정

    .addSection(CardService.newCardSection()    // 본문 섹션 추가
      .addWidget(CardService.newTextParagraph().setText(answer))    // 문단 위젯 추가 (답변으로 채우기)
      .addWidget(CardService.newDivider())  // 본문-버튼 사이 구분선 위젯 추가
      .addWidget(CardService.newTextButton()    // 텍스트 버튼 위젯 추가
        .setText("🔎 자세히 검색하기")  // 버튼에 텍스트 설정
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED) // 버튼 스타일 설정
        .setOpenLink(CardService.newOpenLink()  // 오픈 링크 추가
          .setUrl(WEBAPP_URL)   // 웹앱 URL로 설정
          .setOpenAs(CardService.OpenAs.FULL_SIZE)))    // 풀 사이즈로 오픈되도록 설정
      .addWidget(CardService.newTextButton()    // 텍스트 버튼 추가
        .setText("← 홈으로 돌아가기")   // 텍스트 설정
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)  // 버튼 스타일 설정
        .setOnClickAction(CardService.newAction().setFunctionName("_goBackHome")))) // 클릭 시 _goBackHome() 함수가 실행되도록 설정
    .build();   // 카드 객체 완성 (빌드)

  return CardService.newActionResponseBuilder() // 완성된 객체를 새 카드로 push하여 navigate (화면 전환)
    .setNavigation(CardService.newNavigation().pushCard(card))
    .build();
}


// 공통 유틸리티 함수
// 카드 스택을 루트(홈)까지 모두 pop하여 홈 화면으로 돌아가는 함수
function _goBackHome(e) {      
  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().popToRoot())
    .build();
}

// 알림 토스트 메시지를 표시하는 함수
function _toast(msg) {
  return CardService.newActionResponseBuilder()
    .setNotification(CardService.newNotification().setText(msg))
    .build();
} 

// 문자열 str을 최대 글자수 max에 맞춰 자르는 유틸 함수
function _truncate(str, max) {    
  if (!str) return "";
  return str.length > max ? str.slice(0, max) + "…" : str;
}
