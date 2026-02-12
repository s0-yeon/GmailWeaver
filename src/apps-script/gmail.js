function onGmailMessage(e) {
  return onHomepage(e);
}

function onGmailCompose(e) {
  return onHomepage(e);
}

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