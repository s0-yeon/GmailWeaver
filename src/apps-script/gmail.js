
function applyLabelToRecentInboxThreads_(labelName, n) {
  labelName = (labelName || "").trim();
  n = Number(n || 5);

  if (!labelName) {
    return { ok: false, msg: "라벨 이름을 입력해줘." };
  }
  if (!Number.isFinite(n) || n <= 0) {
    return { ok: false, msg: "최근 개수(N)는 1 이상 숫자여야 해." };
  }

  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);

  const threads = GmailApp.getInboxThreads(0, n);

  threads.forEach(t => t.addLabel(label));

  return {
    ok: true,
    msg: `✅ 최근 Inbox 스레드 ${threads.length}개에 "${labelName}" 라벨 적용 완료`
  };
}

function onClickApplyLabelRecent_(e) {
  const inputs = (e && e.commonEventObject && e.commonEventObject.formInputs) || {};

  const labelName = (inputs.labelName && inputs.labelName.stringInputs)
    ? inputs.labelName.stringInputs.value[0]
    : "";

  const n = (inputs.n && inputs.n.stringInputs)
    ? inputs.n.stringInputs.value[0]
    : "5";

  const res = applyLabelToRecentInboxThreads_(labelName, n);

  return CardService.newActionResponseBuilder()
    .setNotification(CardService.newNotification().setText(res.msg))
    .build();
}

/**
 * 최근 N개의 Inbox thread에 사용자가 고른 라벨을 적용 (라벨 없으면 생성)
 */
function applyLabelToRecentInboxThreads_(labelName, n) {
  labelName = (labelName || "").trim();
  n = Number(n || 5);

  if (!labelName) {
    return { ok: false, msg: "라벨 이름을 입력하세요." };
  }
  if (!Number.isFinite(n) || n <= 0) {
    return { ok: false, msg: "개수는 1개 이상이어야 해요." };
  }

  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);

  const threads = GmailApp.getInboxThreads(0, n);
  threads.forEach((t) => t.addLabel(label));

  return {
    ok: true,
    msg: `✅ 최근 Inbox 스레드 ${threads.length}개에 "${labelName}" 라벨 적용 완료`,
  };
}

/**
 * 버튼 클릭 핸들러: 입력값 읽어서 라벨링 실행 후 토스트로 결과 표시
 */
function onClickApplyLabelRecent_(e) {
  const inputs =
    (e && e.commonEventObject && e.commonEventObject.formInputs) || {};

  const labelName =
    inputs.labelName && inputs.labelName.stringInputs
      ? inputs.labelName.stringInputs.value[0]
      : "";

  const n =
    inputs.n && inputs.n.stringInputs
      ? inputs.n.stringInputs.value[0]
      : "5";

  const res = applyLabelToRecentInboxThreads_(labelName, n);

  return CardService.newActionResponseBuilder()
    .setNotification(CardService.newNotification().setText(res.msg))
    .build();
}