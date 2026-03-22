// src/apps-script/common.js

var TunnelURL = "https://unmatching-sandy-hydrocinnamyl.ngrok-free.dev";    // ngrok로 열어둔 백엔드 서버(Flask/GraphRAG) 주소
const WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzR29ycMGq8ig5H8NMB4fciIwTleDtN-7UJKH-agPx_uK3tN4yKtkfe9v0lZ_kAvS8a/exec";   // Apps Script Web App으로 배포된 URL

// 공식 엔트리 포인트 (외부에서 호출)
function onHomepage(e) {    // Gmail 사이드 패널을 처음 열거나, 메일을 선택하지 않은 상태일 때 자동 호출
    return _buildHomeCard();
}

function onGmailMessage(e) {    // 사용자가 Gmail에서 어떤 메일을 클릭했을 때 자동 호출
    return _buildGmailMessageCard(e);
}

// 공통 유틸
function _webpageBtn() {    
    return CardService.newTextButton()  // 텍스트 버튼 객체 생성
        .setText("🌐 웹페이지로 보기")  
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOpenLink(   // 버튼 클릭 시 웹앱 URL을 새 창으로 열도록 설정
            CardService.newOpenLink()  
                .setUrl(WEBAPP_URL)    
                .setOpenAs(CardService.OpenAs.FULL_SIZE)
        );
}

// 홈 카드 (메일을 선택하지 않은 기본 상태의 UI)
function _buildHomeCard() {
    // 섹션 1: 웹페이지 바로가기 
    var webSection = CardService.newCardSection()     
        .addWidget(_webpageBtn());

    // 섹션 2: Gmail 동기화 
    var syncSection = CardService.newCardSection()
        .setHeader("📬 Gmail 동기화");  // 헤더 텍스트

    var syncNewBtn = CardService.newTextButton()    
        .setText("＋ 서버에 없는 메일 추가")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(  
            CardService.newAction() 
                .setFunctionName("onSyncNewOnly")   // 클릭 시 "onSyncNewOnly" 함수를 서버에서 실행
        );

    var syncAllBtn = CardService.newTextButton()
        .setText("🔄 전체 갱신 (보낸 메일 포함)")
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOnClickAction(  
            CardService.newAction()
                .setFunctionName("onSyncAll")   // 클릭 시 "onSyncAll" 함수를 서버에서 실행
        );

    syncSection.addWidget(syncNewBtn).addWidget(syncAllBtn);    // 동기화 섹션에 위젯 추가

    // 섹션 3: 스마트 검색/질문/명령
    var searchSection = CardService.newCardSection()    
        .setHeader("🔍 검색 · 질문 · 명령");

    var searchInput = CardService.newTextInput()    // 텍스트 입력창
        .setFieldName("searchQuery")
        .setTitle("메시지를 입력하세요")
        .setHint("예: 광고 메일 다 광고 라벨로 옮겨줘 / 이번 달 메일 요약해줘")
        .setMultiline(true);
 
    var searchBtn = CardService.newTextButton()     // 텍스트 버튼
        .setText("전송")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onSmartSearch")   // 클릭 시 "onSmartSearch" 함수를 서버에서 실행
        );

    searchSection.addWidget(searchInput).addWidget(searchBtn);  // 스마트 검색 섹션에 위젯 추가

    return CardService.newCardBuilder()   // 카드 빌더 시작
        .setHeader(
            CardService.newCardHeader()
                .setTitle("📮 GmailWeaver")
                .setSubtitle("메일을 스마트하게 관리하세요")
        )
        .addSection(webSection)     // 위에서 만든 섹션들을 순서대로 붙임
        .addSection(syncSection)
        .addSection(searchSection)
        .build();   // 최종 Card 객체 생성 및 반환
}

// Gmail 메시지 선택 카드
function _buildGmailMessageCard(e) {
    // Gmail 메타데이터 추출
    var accessToken = e.messageMetadata ? e.messageMetadata.accessToken : null;  // OAuth 토큰. Gmail API 호출
    var messageId = e.messageMetadata ? e.messageMetadata.messageId : null;   // Gmail 메시지의 고유 ID

    if (accessToken) 
        GmailApp.setCurrentMessageAccessToken(accessToken);    // GmnailApp이 현재 메시지를 읽을 수 있도록 토큰 등록

    // 섹션 1: 웹페이지 바로가기 
    var webSection = CardService.newCardSection()   
        .addWidget(_webpageBtn());

    // 섹션 2: 라벨 분류
    var labelSection = CardService.newCardSection()
        .setHeader("🏷 라벨 분류");

    var labelInput = CardService.newTextInput()     // 라벨 입력창
        .setFieldName("labelName")
        .setTitle("라벨 이름")
        .setHint("기존 라벨이면 추가, 없으면 자동 생성");

    var labelBtn = CardService.newTextButton()     // 텍스트 버튼
        .setText("라벨 적용")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onApplyLabelToMessage")    // 클릭 시 "onApplyLAbelToMessages" 함수를 서버에서 실행
                .setParameters({ messageId: messageId || "" })
        );

    labelSection.addWidget(labelInput).addWidget(labelBtn); // 라벨 섹션에 위젯 추가

    // 섹션 3: 캘린더 분석
    var calSection = CardService.newCardSection()
        .setHeader("📅 일정 분석 · 등록");

    var calDesc = CardService.newTextParagraph()    // 단순 텍스트 표시 위젯 (버튼 X)
        .setText("메일 본문에서 날짜/시간 정보를 추출하여 Google 캘린더에 일정을 등록합니다.");

    var calBtn = CardService.newTextButton()    // 일정 분석 및 등록 버튼
        .setText("📆 일정 분석 및 등록")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setOnClickAction(
            CardService.newAction()
            .setFunctionName("onExtractAndAddCalendar")     // 클릭 시 "onExtractAndAddCalendar" 함수를 서버에서 실행
            .setParameters({ messageId: messageId || "" })  // Gmail 고유 ID를 파라미터로 설정
        );

    calSection.addWidget(calDesc).addWidget(calBtn);   // 캘린더 섹션에 위젯 추가

    return CardService.newCardBuilder() 
        .setHeader(
            CardService.newCardHeader()
                .setTitle("📨 선택된 메일")
                .setSubtitle("이 메일에 대한 작업을 선택하세요")
        )
        .addSection(webSection)
        .addSection(labelSection)
        .addSection(calSection)
        .build();   // 최종 Card 객체 생성 및 반환
    }