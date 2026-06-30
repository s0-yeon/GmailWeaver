import os
import json
import base64
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv("src/parquet/.env")

client = OpenAI(api_key=os.getenv("GRAPHRAG_API_KEY"))

AVATAR_MODEL = "gpt-image-1"
AVATAR_SIZE = "1024x1024"
AVATAR_QUALITY = "low"

_map_lock = threading.Lock()


def _avatar_filename(email: str) -> str:
    return hashlib.md5(email.strip().lower().encode("utf-8")).hexdigest() + ".png"


def _load_avatar_map(paths) -> dict:
    if not os.path.exists(paths.MAIL_AVATARS_PATH):
        return {}
    with open(paths.MAIL_AVATARS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_avatar_map(paths, avatar_map: dict):
    os.makedirs(paths.MAIL_STATICS_PATH, exist_ok=True)
    with open(paths.MAIL_AVATARS_PATH, "w", encoding="utf-8") as f:
        json.dump(avatar_map, f, ensure_ascii=False, indent=2)


def _build_avatar_prompt(name: str) -> str:
    return f"""Flat vector illustration avatar icon for a profile picture, circular framing.
A friendly portrait of one person whose given name is "{name}".
Infer a plausible gender presentation from this name and depict that person accordingly.
Simple flat-color cartoon style with clearly drawn, visible eyes, nose, and mouth (a simple friendly facial expression) — do not leave the face blank.
Minimal shading, clean geometric shapes, shoulders-up framing, centered composition.
Solid pastel-colored circular background.
No text, no logo, no watermark, no photorealism.""".strip()


def generate_avatar_image_bytes(name: str) -> bytes:
    result = client.images.generate(
        model=AVATAR_MODEL,
        prompt=_build_avatar_prompt(name),
        size=AVATAR_SIZE,
        quality=AVATAR_QUALITY,
        n=1,
    )
    b64 = result.data[0].b64_json
    return base64.b64decode(b64)


def get_cached_person_avatars(paths) -> dict:
    return _load_avatar_map(paths)


def generate_person_avatars_batch(paths, people: list) -> dict:
    """
    people: [{ "email": str, "name": str }, ...]
    이미 캐시된 사람은 건너뛰고, 새로운 사람만 GPT 이미지 API로 생성한다.
    반환: { email_lower: "/person-avatar-image/<gmail_id>/<filename>" } (요청한 사람 전체에 대한 매핑)
    """
    os.makedirs(paths.AVATAR_IMAGES_DIR, exist_ok=True)
    avatar_map = _load_avatar_map(paths)

    targets = []
    seen = set()
    for p in people:
        email = (p.get("email") or "").strip().lower()
        name = (p.get("name") or "").strip()
        if not email or not name or email in seen:
            continue
        seen.add(email)
        if email not in avatar_map:
            targets.append((email, name))

    def _generate_one(email, name):
        try:
            image_bytes = generate_avatar_image_bytes(name)
            filename = _avatar_filename(email)
            filepath = os.path.join(paths.AVATAR_IMAGES_DIR, filename)
            with open(filepath, "wb") as f:
                f.write(image_bytes)
            url = f"/person-avatar-image/{paths.GMAIL_ID}/{filename}"
            with _map_lock:
                avatar_map[email] = url
                _save_avatar_map(paths, avatar_map)
            print(f"[AVATAR] 생성 완료: {email} ({name})")
            return email, url
        except Exception as e:
            print(f"[AVATAR] 생성 실패 ({email}): {e}")
            return email, None

    if targets:
        with ThreadPoolExecutor(max_workers=min(len(targets), 3)) as executor:
            futures = [executor.submit(_generate_one, email, name) for email, name in targets]
            for future in as_completed(futures):
                future.result()

    return {email: avatar_map[email] for email in seen if email in avatar_map}
