import re
import os
import base64
from email.header import decode_header
from email.utils import getaddresses, parsedate_to_datetime

from config.settings import MAIL_BLOCK_SEP

IMAP_SUPPORTED_ATT_EXTS = {".pdf", ".docx", ".hwp", ".pptx", ".xlsx", ".csv", ".txt"}
IMAP_MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024  # 10MB

# 메일 헤더(Subject 등) MIME 인코딩 디코딩
def _imap_decode_header_str(raw) -> str:
    if not raw:
        return ""
    decoded = ""
    for text, enc in decode_header(raw):
        if isinstance(text, bytes):
            try:
                decoded += text.decode(enc or "utf-8", errors="replace")
            except (LookupError, UnicodeDecodeError):
                decoded += text.decode("utf-8", errors="replace")
        else:
            decoded += text
    return decoded.strip()

# 발신/수신인 "이름 <계정>" 포맷
def _imap_format_person(name: str, addr: str) -> str:
    name = (name or "").strip()
    addr = (addr or "").strip().lower()
    if not addr:
        return "없음"
    if name and name.lower() != addr:
        return f"{name} <{addr}>"
    return f"<{addr}>"

# 여러명이 나열된 값을 파싱해서 사람 목록으로 쪼개줌
def _imap_parse_person_list(raw_header) -> list[tuple[str, str]]:
    if not raw_header:
        return []
    people = []
    for name, addr in getaddresses([raw_header]):
        addr = (addr or "").strip()
        if addr:
            people.append((_imap_decode_header_str(name), addr))
    return people

# 본문 추출. text/plain 우선으로 추출하고 없으면 text/html에서 태그 제거해서 대체 본문으로 사용함
def _imap_extract_body(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            disp = str(part.get("Content-Disposition") or "")
            if part.get_content_type() == "text/plain" and "attachment" not in disp.lower():
                charset = part.get_content_charset() or "utf-8"
                payload = part.get_payload(decode=True) or b""
                try:
                    body = payload.decode(charset, errors="replace")
                except (LookupError, UnicodeDecodeError):
                    body = payload.decode("utf-8", errors="replace")
                break
        if not body:
            for part in msg.walk():
                disp = str(part.get("Content-Disposition") or "")
                if part.get_content_type() == "text/html" and "attachment" not in disp.lower():
                    charset = part.get_content_charset() or "utf-8"
                    payload = part.get_payload(decode=True) or b""
                    try:
                        body = payload.decode(charset, errors="replace")
                    except (LookupError, UnicodeDecodeError):
                        body = payload.decode("utf-8", errors="replace")
                    break
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            try:
                body = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                body = payload.decode("utf-8", errors="replace")

    # text/plain 파트인데도 실제로는 HTML 원본이 그대로 들어있는 자동발송 메일이 있어서,
    # 어느 파트에서 왔든 최종 본문에 남은 태그/주석은 방어적으로 한 번 더 제거한다.
    body = re.sub(r"<!--.*?-->", " ", body, flags=re.DOTALL)  # HTML 주석(조건부 주석 포함) 먼저 제거
    body = re.sub(r"<[^>]+>", " ", body)

    body = body.replace("\r\n", "\n")
    body = re.sub(r"[ \t]+", " ", body)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()

# 지원하는 첨부파일은 실제 데이토 수집하고, 지원하지 않으면 메타정보만 수집
def _imap_collect_attachments(msg) -> tuple[list[dict], list[dict]]:
    infos, payloads = [], []
    if not msg.is_multipart():
        return infos, payloads

    idx = 0
    for part in msg.walk():
        disp = str(part.get("Content-Disposition") or "")
        if "attachment" not in disp.lower():
            continue
        idx += 1
        filename = _imap_decode_header_str(part.get_filename()) or f"attachment_{idx}"
        mime = part.get_content_type() or "application/octet-stream"
        data = part.get_payload(decode=True) or b""
        size = len(data)
        ext = os.path.splitext(filename)[-1].lower()
        supported = ext in IMAP_SUPPORTED_ATT_EXTS

        if not supported:
            status = "제외: 형식 미지원"
        elif size > IMAP_MAX_ATTACHMENT_SIZE:
            status = "제외: 용량 초과"
        else:
            status = "포함"

        infos.append({"name": filename, "mime": mime, "size": size, "status": status})
        if supported and size <= IMAP_MAX_ATTACHMENT_SIZE:
            payloads.append({"name": filename, "mime": mime, "data": data})

    return infos, payloads

# 메일을 정해진 포맷의 텍스트 블록으로 변환
def _imap_build_block(mail_index: int, mail_id: str, msg, folder: str, my_email: str) -> tuple[str, list[dict]]:
    subject = _imap_decode_header_str(msg.get("Subject")) or "(제목 없음)"

    from_list = _imap_parse_person_list(msg.get("From"))
    from_name, from_addr = from_list[0] if from_list else ("", "")
    direction = "발신" if from_addr.lower() == my_email.strip().lower() else "수신"

    to_list = _imap_parse_person_list(msg.get("To"))
    cc_list = _imap_parse_person_list(msg.get("Cc"))

    try:
        dt = parsedate_to_datetime(msg.get("Date"))
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        date_str = ""

    att_infos, att_payloads = _imap_collect_attachments(msg)
    if att_infos:
        attachment_info = "\n".join(
            f"- {a['name']} ({a['size']/1024:.1f} KB) [{a['status']}]" for a in att_infos
        )
    else:
        attachment_info = "없음"

    body = _imap_extract_body(msg)

    block_text = "\n".join([
        MAIL_BLOCK_SEP,
        f"[메일 {mail_index}]",
        "",
        f"ID: {mail_id}",
        f"제목: {subject}",
        f"구분: {direction}",
        f"날짜: {date_str}",
        "",
        f"발신인: {_imap_format_person(from_name, from_addr)}",
        "수신인: " + (", ".join(_imap_format_person(n, a) for n, a in to_list) if to_list else "없음"),
        "참조(CC): " + (", ".join(_imap_format_person(n, a) for n, a in cc_list) if cc_list else "없음"),
        "",
        "[라벨 정보]",
        folder,
        "",
        "[첨부파일 정보]",
        attachment_info,
        "",
        "[메일 본문]",
        body,
        MAIL_BLOCK_SEP,
    ])

    attachments_payload = [
        {
            "mail_id": mail_id,
            "name": a["name"],
            "mime": a["mime"],
            "data_base64": base64.b64encode(a["data"]).decode("ascii"),
        }
        for a in att_payloads
    ]

    return block_text, attachments_payload
