// apps-script/util/extractioninfo.js

// Gmail Add-on(클라이언트)이 사용자의 전체 Gmail 스냅샷을 만들어 서버(GraphRAG 파이프라인 쪽)에 업로드하는 함수
function _exportAllInboxAndSentIntoOneTxt() {
    // 받은 메일 + 보낸 메일
    const query = "in:inbox OR in:sent";  // Gmail 검색 쿼리(검색어) 지정: 수신함 or 송신함
    const threads = GmailApp.search(query, 0, 500);   // 조건에 맞는 스레드 최대 500개 가져와서 threads 배열에 저장. 이때 단위는 스레드이므로, 각 thread 안에는 여러 message들이 존재

    const folder = DriveApp.getRootFolder();  // Drive에 파일 저장 시 사용. 그러나 현재 createDrive는 주석 처리되어 있으므로, 오직 서버 전송에만 사용됨
    const myEmail = Session.getActiveUser().getEmail();   // 해당 스크립트를 이용하는 사용자의 Gmail 주소. 

    let allText = "";   // 모든 메일을 하나의 문자열로 저장(누적)
    let mailCount = 0;  // 추출된 메일 총 개수 카운터

    threads.forEach((thread) => {   // 각 스레드에 대해
        thread.getMessages().forEach((msg) => {   //  그 스레드 안의 각 메시지(GmailMessage 객체)에 대해
        mailCount++;  // 메일 카운트 증가

        const id = msg.getId();   // 메시지 ID
        const subject = msg.getSubject() || "(제목 없음)";  // 메시지 제목
        const from = msg.getFrom() || "";   // 송신인
        const to = msg.getTo() || "";   // 수신인
        const cc = msg.getCc() || "";   // 참조
        const date = msg.getDate();     // 날짜

        // 수신 / 발신 구분
        const direction = from.includes(myEmail) ? "발신" : "수신";   // 송신인 중에서 사용자 추출

        // 첨부파일 정보
        const atts = msg.getAttachments({ includeInlineImages: false });  // 첨부파일 추출. 본문에 인라인 이미지로 삽입된 경우 제외
        let attachmentInfo = "";  
        if (atts.length === 0) {  // 첨부파일이 존재하지 않는 경우
            attachmentInfo = "첨부파일: 없음\n";
        } else {  // 첨부파일이 존재하는 경우
            attachmentInfo = "첨부파일:\n"; 
            atts.forEach((att, i) => {
            attachmentInfo += `  ${i + 1}. ${att.getName()} | ${att.getContentType()} | ${att.getSize()} bytes\n`;  // 파일명, MIME 타입, 크기(bytes) 추출하여 문자열 저장 
            });
        }

        // 메일 본문 (텍스트)
        const body = msg.getPlainBody() || "";  // 플레인 텍스트. not HTML

        // TXT에 추가. 하나의 메일을 하나의 텍스트 파일로 만들기
        allText += 
        `============================================================
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

    if (mailCount === 0) {  // 메일이 하나도 없는 경우
        allText = "메일이 없습니다.\n";
    }

    const filename = `gmail_ALL_inbox_sent_${_dateToYmdHms(new Date())}.txt`;   // 파일 이름 생성
    //folder.createFile(filename, allText, MimeType.PLAIN_TEXT);  // 구글 드라이브에 파일 저장
    UrlFetchApp.fetch(TunnelURL +"/upload", {   // 해당 서버 주소로 HTTP POST. POST 방식을 사용하는 이유는 '서버 상태를 변경(파일 생성/저장)하기 때문
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
function _dateToYmdHms(d) {   // 매개변수는 Date 객체
    const pad = (n) => String(n).padStart(2, "0");  // 숫자를 문자열로 변환하고, 문자열 길이가 2가 되도록 앞쪽에 0을 채워서 반환. pad(padding): 데이터의 길이를 맞추기 위해 앞이나 뒤에 값을 채워 넣는 것
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;    // eg. 2026년 2월 3일 오후 4시 5분 9초 -> 2026-02-03_160509와 같은 형태로 반환. padStart()는 String 객체의 메서드
}
