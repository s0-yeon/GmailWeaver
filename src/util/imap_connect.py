# IMAP 수집 유틸
import base64
import re
import imaplib
import email
import traceback

from util.imap_message import _imap_build_block

# IMAP 폴더명(RFC 3501 Modified UTF-7)에 한글 등 비-ASCII 문자가 있을 때 인코딩
def _imap_utf7_encode_folder(folder: str) -> str:
    result = bytearray()
    i, n = 0, len(folder)
    while i < n:
        ch = folder[i]
        if ch == "&":
            result.extend(b"&-")
            i += 1
            continue
        if 0x20 <= ord(ch) <= 0x7e:
            result.append(ord(ch))
            i += 1
            continue
        j = i
        while j < n and not (0x20 <= ord(folder[j]) <= 0x7e):
            j += 1
        chunk = folder[i:j]
        b64 = base64.b64encode(chunk.encode("utf-16-be")).decode("ascii")
        b64 = b64.rstrip("=").replace("/", ",")
        result.extend(b"&" + b64.encode("ascii") + b"-")
        i = j
    return result.decode("ascii")

# IMAP 폴더명(RFC 3501 Modified UTF-7) 디코딩
def _imap_utf7_decode_folder(name: str) -> str:
    result = []
    i, n = 0, len(name)
    while i < n:
        ch = name[i]
        if ch != "&":
            result.append(ch)
            i += 1
            continue
        j = name.find("-", i)
        if j == -1:
            j = n
        b64 = name[i + 1:j]
        if b64 == "":
            result.append("&")
        else:
            b64 = b64.replace(",", "/")
            b64 += "=" * ((-len(b64)) % 4)
            try:
                result.append(base64.b64decode(b64).decode("utf-16-be"))
            except Exception:
                result.append(name[i:j + 1])
        i = j + 1
    return "".join(result)

# IMAP LIST 응답에서 실제 폴더명을 추출
def _imap_parse_list_line(line: bytes):
    try:
        text = line.decode("utf-8", errors="replace").strip()
    except Exception:
        return None

    m = re.match(r'^\(([^)]*)\)\s+(?:"([^"]*)"|NIL)\s+(.+)$', text)
    if not m:
        return None

    flags = m.group(1)
    if "\\Noselect" in flags:
        return None  # 선택 불가능한 폴더는 제외

    raw_name = m.group(3).strip()
    if raw_name.startswith('"') and raw_name.endswith('"'):
        raw_name = raw_name[1:-1]

    return _imap_utf7_decode_folder(raw_name)

# IMAP 서버에 접속해서 선택한 폴더들의 메일을 가져옴
def _imap_fetch_content(host: str, port: int, use_ssl: bool, user: str, password: str,
                         folders: list[str], limit: int, my_email: str) -> tuple[str, list[dict]]:
    conn = imaplib.IMAP4_SSL(host, port) if use_ssl else imaplib.IMAP4(host, port)

    try:
        conn.login(user, password)

        all_blocks: list[str] = []
        all_attachments: list[dict] = []
        mail_index = 0

        for folder in folders:
            encoded_folder = _imap_utf7_encode_folder(folder)
            status, _resp = conn.select(f'"{encoded_folder}"', readonly=True)
            if status != "OK":
                status, _resp = conn.select(encoded_folder, readonly=True)
            if status != "OK":
                print(f"[IMAP] 폴더 선택 실패, 스킵: {folder}")
                continue

            status, data = conn.uid("search", None, "ALL")
            if status != "OK" or not data or not data[0]:
                continue

            uids = data[0].split()
            uids.reverse()  # 최신 메일부터
            if limit and limit > 0:
                uids = uids[:limit]

            for uid in uids:
                uid_str = uid.decode(errors="ignore") if isinstance(uid, bytes) else str(uid)
                try:
                    status, msg_data = conn.uid("fetch", uid_str, "(BODY.PEEK[])")
                    if status != "OK" or not msg_data:
                        continue

                    raw_email = None
                    for part in msg_data:
                        if isinstance(part, tuple) and len(part) >= 2 and isinstance(part[1], (bytes, bytearray)):
                            raw_email = part[1]
                            break

                    if raw_email is None:
                        print(f"[IMAP] RFC822 본문 없음, 스킵: uid={uid_str}")
                        continue

                    msg = email.message_from_bytes(raw_email)
                except Exception as e:
                    print(f"[IMAP] 메일 파싱 오류, 스킵: uid={uid} / {e}")
                    traceback.print_exc()
                    continue

                message_id = (msg.get("Message-ID") or "").strip().strip("<>")
                if not message_id:
                    message_id = f"{folder}-{uid_str}"

                mail_index += 1
                block_text, attachments_payload = _imap_build_block(mail_index, message_id, msg, folder, my_email)
                all_blocks.append(block_text)
                all_attachments.extend(attachments_payload)

        content = "\n\n".join(all_blocks).strip()
        if content:
            content += "\n"
        return content, all_attachments

    finally:
        try:
            conn.logout()
        except Exception:
            pass