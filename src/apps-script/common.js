// src/apps-script/common.js

var TunnelURL = "https://interatrial-tana-wishfully.ngrok-free.dev";    // ngrok로 열어둔 백엔드 서버(Flask/GraphRAG) 주소
const WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwAk_JabdKuGUHIVcaKeEnY1DUiYb0uqkiu-KdUG67Zf1U3D8k-F06RGS5043k_fZS8MQ/exec";   // Apps Script Web App으로 배포된 URL

const OLIVE = "#c6d8a5";

// 공식 엔트리 포인트 (외부에서 호출)
function onHomepage(e) {
    return _buildHomeCard();
}

function onGmailMessage(e) {
    return _buildGmailMessageCard(e);
}

// 공통 유틸
function _webpageBtn() {
    return CardService.newTextButton()
        .setText("ㅤㅤㅤ🌐 웹페이지 보기ㅤㅤㅤㅤ")
        .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
        .setBackgroundColor(OLIVE)
        .setOpenLink(
            CardService.newOpenLink()
                .setUrl(WEBAPP_URL)
                .setOpenAs(CardService.OpenAs.FULL_SIZE)
        );
}

// 그래프 시각화 버튼
function _graphBtn() {
    var gmailId = Session.getActiveUser().getEmail();

    return CardService.newTextButton()
        .setText("그래프")
        .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
        .setOpenLink(
            CardService.newOpenLink()
                .setUrl(
                    TunnelURL+ "/graph-view?gmail_id=" + encodeURIComponent(gmailId)
                )
                .setOpenAs(CardService.OpenAs.FULL_SIZE)
        );
}

// 홈 카드
function _buildHomeCard() {
    var syncNewBtn = CardService.newTextButton()
        .setText("ㅤㅤ＋ 서버에 없는 메일 추가ㅤㅤ")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onSyncNewOnly")
        );

    var syncAllBtn = CardService.newTextButton()
        .setText("🔄 전체 갱신 (보낸 메일 포함)ㅤ")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onSyncAll")
        );

    var mainSection = CardService.newCardSection()
        .addWidget(_webpageBtn())
        .addWidget(syncNewBtn)
        .addWidget(syncAllBtn);

    // 이하 동일

    var searchInput = CardService.newTextInput()
        .setFieldName("searchQuery")
        .setTitle("메시지를 입력하세요")
        .setHint("예: 광고 메일 다 광고 라벨로 옮겨줘 / 이번 달 메일 요약해줘")
        .setMultiline(true);

    var searchBtn = CardService.newTextButton()
        .setText("전송")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onSmartSearch")
        );

    var searchSection = CardService.newCardSection()
        .setHeader("ㅤㅤ🔍 검색 · 질문 · 명령ㅤㅤㅤ")
        .addWidget(searchInput)
        .addWidget(searchBtn);

    return CardService.newCardBuilder()
        .setHeader(
            CardService.newCardHeader()
                .setTitle("📮 메일을 스마트하게 관리하세요")
        )
        .addSection(mainSection)
        .addSection(searchSection)
        .addSection(CardService.newCardSection().addWidget(_graphBtn())) // 그래프 보기 버튼
        .build();
}

// Gmail 메시지 선택 카드
function _buildGmailMessageCard(e) {
    var accessToken = e.messageMetadata ? e.messageMetadata.accessToken : null;
    var messageId = e.messageMetadata ? e.messageMetadata.messageId : null;

    if (accessToken)
        GmailApp.setCurrentMessageAccessToken(accessToken);

    // 섹션 1: 웹페이지 + 서버 전송
    var serverBtn = CardService.newTextButton()
        .setText("ㅤㅤ☁ 이 메일을 서버로 전송ㅤㅤ")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onUploadSingleMessage")
                .setParameters({ messageId: messageId || "" })
        );

    var webSection = CardService.newCardSection()
        .addWidget(_webpageBtn())
        .addWidget(serverBtn);

    // 섹션 2: 라벨 분류
    var labelInput = CardService.newTextInput()
        .setFieldName("labelName")
        .setTitle("이름")
        .setHint("기존 라벨이면 추가, 없으면 자동 생성");

    var labelBtn = CardService.newTextButton()
        .setText("적용")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onApplyLabelToMessage")
                .setParameters({ messageId: messageId || "" })
        );

    var labelSection = CardService.newCardSection()
        .setHeader("🏷 라벨 분류")
        .addWidget(labelInput)
        .addWidget(labelBtn);

    // 섹션 3: 캘린더 분석 — 버튼 먼저, 설명 아래
    var calBtn = CardService.newTextButton()
        .setText("ㅤㅤ📆 일정 분석 및 등록ㅤㅤㅤ")
        .setTextButtonStyle(CardService.TextButtonStyle.OUTLINED)
        .setOnClickAction(
            CardService.newAction()
                .setFunctionName("onExtractAndAddCalendar")
                .setParameters({ messageId: messageId || "" })
        );

    var calDesc = CardService.newTextParagraph()
        .setText("메일 본문에서 날짜/시간 정보를 추출하여 Google 캘린더에 일정을 등록합니다.");

    var calSection = CardService.newCardSection()
        .addWidget(calBtn)      // 버튼 위
        .addWidget(calDesc);    // 설명 아래 (섹션 헤더 없음)

    return CardService.newCardBuilder()

        .setHeader(
            CardService.newCardHeader()
                .setTitle("이 메일에 대한 작업을 선택하세요")
        )
        .addSection(webSection)
        .addSection(labelSection)
        .addSection(calSection)
        .build();
}

