// ============================================================
// src/apps-script/common.js
// 진입점 & 카드 빌더
// ============================================================

var TunnelURL = "https://unmatching-sandy-hydrocinnamyl.ngrok-free.dev";
const WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwQ8hXa8QIpxyD-XUbZ3FThm-XD3Jx9WKNpA4Xozf2t/dev";

// ============================================================
// 진입점
// ============================================================

function onHomepage(e) {
  return buildHomeCard_();
}

function onGmailMessage(e) {
  return buildGmailMessageCard_(e);
}

// ============================================================
// 웹페이지 열기 버튼 (공통)
// ============================================================

function _webpageBtn_() {
  return CardService.newTextButton()
    .setText("🌐 웹페이지로 보기")
    .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
    .setOpenLink(
      CardService.newOpenLink()
        .setUrl(WEBAPP_URL)
        .setOpenAs(CardService.OpenAs.FULL_SIZE)
    );
}

// ============================================================
// 홈 카드 (메일 미선택)
// ============================================================

function buildHomeCard_() {
  // ── 섹션 1: 웹페이지 바로가기 ────────────────────────────
  var webSection = CardService.newCardSection()
    .addWidget(_webpageBtn_());

  // ── 섹션 2: Gmail 동기화 ──────────────────────────────────
  var syncSection = CardService.newCardSection()
    .setHeader("📬 Gmail 동기화");

  var syncNewBtn = CardService.newTextButton()
    .setText("＋ 서버에 없는 메일 추가")
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setOnClickAction(
      CardService.newAction().setFunctionName("onSyncNewOnly_")
    );

  var syncAllBtn = CardService.newTextButton()
    .setText("🔄 전체 갱신 (보낸 메일 포함)")
    .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
    .setOnClickAction(
      CardService.newAction().setFunctionName("onSyncAll_")
    );

  syncSection
    .addWidget(syncNewBtn)
    .addWidget(syncAllBtn);

  // ── 섹션 3: 스마트 검색 / 명령 ────────────────────────────
  var searchSection = CardService.newCardSection()
    .setHeader("🔍 검색 · 질문 · 명령");

  var searchInput = CardService.newTextInput()
    .setFieldName("searchQuery")
    .setTitle("메시지를 입력하세요")
    .setHint("예: 광고 메일 다 광고 라벨로 옮겨줘 / 이번 달 메일 요약해줘")
    .setMultiline(true);

  var searchBtn = CardService.newTextButton()
    .setText("전송")
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setOnClickAction(
      CardService.newAction().setFunctionName("onSmartSearch_")
    );

  searchSection.addWidget(searchInput).addWidget(searchBtn);

  return CardService.newCardBuilder()
    .setHeader(
      CardService.newCardHeader()
        .setTitle("📮 GmailWeaver")
        .setSubtitle("메일을 스마트하게 관리하세요")
    )
    .addSection(webSection)
    .addSection(syncSection)
    .addSection(searchSection)
    .build();
}

// ============================================================
// Gmail 메시지 선택 카드
// ============================================================

function buildGmailMessageCard_(e) {
  var accessToken = e.messageMetadata ? e.messageMetadata.accessToken : null;
  var messageId   = e.messageMetadata ? e.messageMetadata.messageId   : null;

  if (accessToken) GmailApp.setCurrentMessageAccessToken(accessToken);

  // ── 섹션 1: 웹페이지 바로가기 ────────────────────────────
  var webSection = CardService.newCardSection()
    .addWidget(_webpageBtn_());

  // ── 섹션 2: 라벨 분류 ─────────────────────────────────────
  var labelSection = CardService.newCardSection()
    .setHeader("🏷 라벨 분류");

  var labelInput = CardService.newTextInput()
    .setFieldName("labelName")
    .setTitle("라벨 이름")
    .setHint("기존 라벨이면 추가, 없으면 자동 생성");

  var labelBtn = CardService.newTextButton()
    .setText("라벨 적용")
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setOnClickAction(
      CardService.newAction()
        .setFunctionName("onApplyLabelToMessage_")
        .setParameters({ messageId: messageId || "" })
    );

  labelSection.addWidget(labelInput).addWidget(labelBtn);

  // ── 섹션 3: 캘린더 분석 ───────────────────────────────────
  var calSection = CardService.newCardSection()
    .setHeader("📅 일정 분석 · 등록");

  var calDesc = CardService.newTextParagraph()
    .setText("메일 본문에서 날짜/시간 정보를 추출하여 Google 캘린더에 일정을 등록합니다.");

  var calBtn = CardService.newTextButton()
    .setText("📆 일정 분석 및 등록")
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setOnClickAction(
      CardService.newAction()
        .setFunctionName("onExtractAndAddCalendar_")
        .setParameters({ messageId: messageId || "" })
    );

  calSection.addWidget(calDesc).addWidget(calBtn);

  // ── 섹션 4: 서버로 전송 ───────────────────────────────────
  var serverSection = CardService.newCardSection()
    .setHeader("☁ 서버로 전송");

  var serverBtn = CardService.newTextButton()
    .setText("이 메일을 서버로 전송")
    .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
    .setOnClickAction(
      CardService.newAction()
        .setFunctionName("onUploadSingleMessage_")
        .setParameters({ messageId: messageId || "" })
    );

  serverSection.addWidget(serverBtn);

  return CardService.newCardBuilder()
    .setHeader(
      CardService.newCardHeader()
        .setTitle("📨 선택된 메일")
        .setSubtitle("이 메일에 대한 작업을 선택하세요")
    )
    .addSection(webSection)
    .addSection(labelSection)
    .addSection(calSection)
    .addSection(serverSection)
    .build();
}