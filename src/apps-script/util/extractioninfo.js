// apps-script/util/extractioninfo.js

// Gmail Add-on(클라이언트)이 사용자의 전체 Gmail 스냅샷을 만들어 서버(GraphRAG 파이프라인 쪽)에 업로드하는 함수
function _exportAllInboxAndSentIntoOneTxt() {
    const query = "in:inbox OR in:sent";  // Gmail 검색 쿼리
    const threads = GmailApp.search(query, 0, 50);   // 스레드
    // const folder = DriveApp.getRootFolder();  // Drive에 파일 저장 시 사용. 그러나 현재 createDrive는 주석 처리되어 있으므로, 오직 서버 전송에만 사용됨
    const myEmail = Session.getActiveUser().getEmail();   // 해당 스크립트를 이용하는 사용자의 Gmail 주소

    // 서버로 실제 전송할 첨부 제한
    const allowedMimes = [      
        "application/pdf",
        "application/haansoftpdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ];
    const MAX_ATTACHMENT_SIZE = 5 * 1024 * 1024 * 1024 * 1024; 
    
    let allText = "";   // 모든 메일 텍스트 누적 문자열
    let mailCount = 0;  // 추출된 메일 총 개수 카운터
    let allAttachments = [];    // 전체 첨부 누적 배열

    threads.forEach((thread) => {   // 각 스레드에 대해
        thread.getMessages().forEach((msg) => {   //  그 스레드 안의 각 메시지(GmailMessage 객체)에 대해
        mailCount++;  // 메일 카운트 증가

        // 1) 기본 정보
        const id = msg.getId();   // 메시지 ID
        const subject = msg.getSubject() || "(제목 없음)";  // 메시지 제목
        const from = msg.getFrom() || "";   // 송신인
        const to = msg.getTo() || "";   // 수신인
        const cc = msg.getCc() || "";   // 참조
        const date = Utilities.formatDate(  // 날짜
            msg.getDate(),
            Session.getScriptTimeZone(),    
            "yyyy-MM-dd HH:mm:ss"   // 문자열로 정규화
        );

        // 2) 수신/발신 구분
        const direction = from.includes(myEmail) ? "발신" : "수신";   

        // 3) 첨부파일 처리
        const atts = msg.getAttachments({ includeInlineImages: false });  // 본문에 인라인 이미지로 삽입된 경우 제외
        let attachmentInfo = "";    // 정보. TXT 기록용
        const attachmentPayload = [];   // 페이로드. 서버 전송용

        if (atts.length === 0) { 
            attachmentInfo = "첨부파일: 없음\n";
        } else {  
            attachmentInfo = "첨부파일:\n"; 

            atts.forEach((att, i) => {
                const name = att.getName() || `attachment_${i + 1}`;
                const mime = att.getContentType() || "application/octet-stream";
                const size = att.getSize();
                
                let uploadStatus = "";  // 업로드 상태

                // MIME / 크기 조건을 만족하는 경우만 base64 인코딩 및 payload 추가
                if (!allowedMimes.includes(mime)) {
                    uploadStatus = "업로드 제외: MIME 미지원";
                } else if (size > MAX_ATTACHMENT_SIZE) {
                    uploadStatus = "업로드 제외: 용량 초과";
                } else {
                    const dataBase64 = Utilities.base64Encode(att.getBytes());

                    attachmentPayload.push({
                        mail_id: id,
                        name: name,
                        mime: mime,
                        data_base64: dataBase64
                    });

                    uploadStatus = "업로드 포함";
                }
            
                // 사람이 읽는 문자열 기록
                attachmentInfo += `  ${i + 1}. ${name} | ${mime} | ${size} bytes | ${uploadStatus}\n`;    
            });
        }

        // 전체 첨부 데이터 누적
        allAttachments = allAttachments.concat(attachmentPayload);  

        // 4) 메일 본문 (텍스트)
        const body = msg.getPlainBody() || "";  // 플레인 텍스트. not HTML

        //  5) TXT 문자열 누적
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

    if (mailCount === 0) {
        allText = "메일이 없습니다.\n";
    }

    const filename = `gmail_ALL_inbox_sent_${_dateToYmdHms(new Date())}.txt`;   // 파일 이름 생성
    
    const res = UrlFetchApp.fetch(TunnelURL + "/upload", {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify({
            filename,
            content: allText,
            attachment: allAttachments,
        }),
        muteHttpExceptions: true,
    });

    const code = res.getResponseCode();
    const text = res.getContentText();

    if (code < 200 || code >= 300) {
        throw new Error(`upload failed: ${code} / ${text}`);
    }

    Logger.log(`upload success: ${code} / ${text}`);
    Logger.log(`총 ${mailCount}개의 메일 업로드 완료: ${filename}`);
    Logger.log(`서버로 전송한 첨부 개수: ${allAttachments.length}`);
}

// 파일명용 날짜 문자열
function _dateToYmdHms(d) {   // 매개변수는 Date 객체
    const pad = (n) => String(n).padStart(2, "0");  // 숫자를 문자열로 변환하고, 문자열 길이가 2가 되도록 앞쪽에 0을 채워서 반환. pad(padding): 데이터의 길이를 맞추기 위해 앞이나 뒤에 값을 채워 넣는 것
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;    // eg. 2026년 2월 3일 오후 4시 5분 9초 -> 2026-02-03_160509와 같은 형태로 반환. padStart()는 String 객체의 메서드
}
