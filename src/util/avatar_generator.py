import os
import io
import re
import json
import base64
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from openai import OpenAI
from PIL import Image

from util.database.db_reader import get_person_descriptions

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


def _extract_relationship_hint(description: str) -> str:
    """person.description 텍스트(이름/관계/자주 주고받은 내용)에서 '관계' 줄만 추출해
    아바타 스타일에 참고할 짧은 컨텍스트로 사용한다. 메일 내용 자체는 노출하지 않는다."""
    if not description:
        return ""
    m = re.search(r"관계:\s*(.+)", description)
    return m.group(1).strip() if m else ""


# 사람마다 시각적으로 뚜렷이 구분되도록, 이메일 해시로 결정적으로 고르는 속성 풀.
# (같은 이메일 → 항상 같은 조합, 다른 이메일 → 대부분 다른 조합)
_BG_COLORS = [
    ("warm coral pink", "#F4B8B8"), ("sky blue", "#AEDFF7"), ("sage green", "#BFE3C8"),
    ("soft lavender", "#D8C6F0"), ("warm sand", "#F4D9A6"), ("seafoam teal", "#A8E0D8"),
    ("dusty rose", "#F0C4D6"), ("pale sunflower yellow", "#F6E2A0"), ("powder blue", "#C7D9F0"),
    ("muted mint", "#BEEBD9"), ("warm peach", "#F6CBA6"), ("soft periwinkle", "#C9CCF4"),
]
_HAIR_STYLES = [
    "short and neatly combed", "medium-length with a side part", "long and straight reaching the shoulders",
    "long and gently wavy", "tied back in a low ponytail", "a short bob cut",
    "tousled and slightly messy", "tied back in a neat bun", "shoulder-length with bangs",
]
_HAIR_COLORS = ["jet black", "dark brown", "warm chestnut brown", "soft ash brown"]
_ACCESSORIES = ["no accessories", "simple round glasses", "small stud earrings", "a thin headband", "rectangular glasses"]
_CLOTHING_COLORS = [
    "coral red", "navy blue", "olive green", "mustard yellow", "plum purple",
    "burnt orange", "deep teal", "rose pink", "charcoal gray", "warm brown",
]


def _hex_to_rgb(hex_color: str) -> tuple:
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _pick_style_attributes(seed_key: str) -> dict:
    n = int(hashlib.md5((seed_key or "").strip().lower().encode("utf-8")).hexdigest(), 16)
    bg_name, bg_hex = _BG_COLORS[n % len(_BG_COLORS)]
    return {
        "bg_name": bg_name,
        "bg_hex": bg_hex,
        "bg_rgb": _hex_to_rgb(bg_hex),
        "hair_style": _HAIR_STYLES[(n // 7) % len(_HAIR_STYLES)],
        "hair_color": _HAIR_COLORS[(n // 13) % len(_HAIR_COLORS)],
        "accessory": _ACCESSORIES[(n // 29) % len(_ACCESSORIES)],
        "clothing_color": _CLOTHING_COLORS[(n // 41) % len(_CLOTHING_COLORS)],
    }


def _infer_gender_presentation(name: str) -> str:
    """
    이미지 모델(gpt-image-1)에게 "이름 보고 알아서 성별 추론해"라고 맡기면 부정확할 때가 많아
    (예: '최지유' → 남성으로 잘못 생성), 텍스트 추론에 강한 gpt-4o-mini로 먼저 판별해
    이미지 프롬프트에 명시적으로 박아 넣는다. 한국어 이름뿐 아니라 영어 등 다른 언어권 이름도
    함께 판단할 수 있도록 특정 문화권에 한정하지 않는다.
    반환: 'female' | 'male' | 'unknown'
    """
    try:
        result = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "주어진 사람 이름만 보고 일반적으로 인지되는 성별을 판단하는 AI입니다. "
                                "이름은 한국어, 영어 등 다양한 언어/문화권에서 올 수 있으니 이름의 언어/문화권에 맞는 "
                                "통상적인 성별 인식 관습을 적용하세요. "
                                "반드시 female, male, unknown 중 하나만 정확히 출력하세요. 판단이 애매하면 unknown.",
                },
                {"role": "user", "content": f"이름: {name}"},
            ],
            temperature=0,
        )
        answer = result.choices[0].message.content.strip().lower()
        if "female" in answer:
            return "female"
        if "male" in answer:
            return "male"
    except Exception as e:
        print(f"[AVATAR] 성별 추론 실패 ({name}): {e}")
    return "unknown"


def _build_avatar_prompt(name: str, relationship_hint: str = "", seed_key: str = "") -> str:
    context_block = ""
    if relationship_hint:
        context_block = f"""

[Persona context — style inspiration only, never literal]
A short note about this person's relationship to the user: "{relationship_hint[:200]}"
Use this ONLY as soft inspiration for clothing style and mood (e.g. business-casual for a colleague, relaxed casual for a friend/family member). Never depict any text, objects, logos, or literal scenes from this note."""

    attrs = _pick_style_attributes(seed_key or name)
    gender = _infer_gender_presentation(name)
    gender_line = {
        "female": "Depict this person with a clearly feminine gender presentation.",
        "male": "Depict this person with a clearly masculine gender presentation.",
        "unknown": "This person's gender is ambiguous from their name — depict them with a gender-neutral, androgynous presentation.",
    }[gender]

    return f"""You are the illustration engine for a unified corporate contact-avatar system, in the visual language of products like Slack, Notion, or Linear's default member avatars. Every avatar you generate must look like it belongs to the exact same icon set — consistent style, consistent rules, every time. Each person in this set must look like a clearly distinct individual, not a reused default template.

[Subject]
A single friendly portrait of one person whose given name is "{name}". {gender_line}{context_block}

[Individual appearance — follow exactly, these make this avatar visually distinct from everyone else in the set]
- Hair: {attrs['hair_color']}, styled {attrs['hair_style']}.
- Accessory: {attrs['accessory']}.
- Clothing: a flat, solid {attrs['clothing_color']} top.
- Background: this image is generated with a fully transparent background — render ONLY the person, with no background art, no vignette, no glow, no shape, no color fill of any kind behind them. A flat solid color will be composited behind the cutout programmatically afterward.

[Art direction]
- Flat vector illustration, modern corporate-avatar style: clean geometric shapes, confident outlines of uniform stroke width. No gradients, no soft shading, no drop shadows, no textures, no glossy highlights anywhere in the image.
- The face must read clearly even at very small sizes (this renders as a ~40px circular icon): simple but expressive eyes, nose, and a warm closed-mouth smile. Never leave the face blank or featureless.

[Framing & composition]
- Centered, symmetrical, shoulders-up portrait with generous headroom at the top and on both sides.
- The entire head, the full hairstyle silhouette, and both ears must be completely visible with clear empty space above the hair and on both sides — do not crop or tightly fill the frame with the face. The head should occupy roughly the middle 50-60% of the image height.
- The shoulders and clothing should extend all the way down and bleed off the bottom edge of the canvas, with NO background visible below the body — only the head/hair area needs top and side margin, the torso should fill edge-to-edge at the bottom like a standard cropped profile-picture avatar.

[Technical constraints]
- Square canvas, 1:1 aspect ratio.
- No text, no logos, no watermarks, no signatures, no UI chrome, no photorealism, no 3D rendering, no anime style.""".strip()


def _ensure_margins(subject: Image.Image, min_top: float = 0.06, min_side: float = 0.04) -> Image.Image:
    """
    모든 아바타가 같은 구도를 갖도록 항상 동일한 규칙으로 재배치한다:
    머리 위쪽과 좌우는 여백을 보장하고(귀/머리카락이 잘리지 않게), 어깨·옷은
    의도적으로 캔버스 맨 아래까지 여백 없이 꽉 채운다(표준 프로필 아이콘 스타일).
    모델이 매번 다른 구도로 그려도 결과 레이아웃은 모든 사람에게 동일하게 보장된다.
    """
    w, h = subject.size
    alpha = subject.split()[-1]
    bbox = alpha.getbbox()
    if not bbox:
        return subject
    left, top, right, bottom = bbox
    content_w, content_h = right - left, bottom - top
    if content_w <= 0 or content_h <= 0:
        return subject

    scale = (h * (1 - min_top)) / content_h
    max_w = w * (1 - 2 * min_side)
    if content_w * scale > max_w:
        scale = max_w / content_w

    cropped = subject.crop(bbox)
    new_w, new_h = max(1, round(content_w * scale)), max(1, round(content_h * scale))
    cropped = cropped.resize((new_w, new_h), Image.LANCZOS)

    canvas = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    canvas.paste(cropped, ((w - new_w) // 2, h - new_h), cropped)
    return canvas


def _composite_on_color(image_bytes: bytes, bg_rgb: tuple) -> bytes:
    """
    gpt-image-1을 background="transparent"로 호출해 받은, 인물만 있고 배경은 진짜
    알파 채널로 투명한 PNG를 우리가 정한 단색 배경 위에 합성한다. 색 차이로 배경을
    "추측"하지 않고 API가 제공하는 진짜 투명도를 쓰기 때문에, 머리카락이 배경과
    비슷한 색이어도(예: 검은 머리 + 어두운 배경) 안전하게 분리된다.
    """
    subject = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    subject = _ensure_margins(subject)
    solid_bg = Image.new("RGBA", subject.size, bg_rgb + (255,))
    result = Image.alpha_composite(solid_bg, subject).convert("RGB")

    out = io.BytesIO()
    result.save(out, format="PNG")
    return out.getvalue()


def generate_avatar_image_bytes(name: str, relationship_hint: str = "", seed_key: str = "") -> bytes:
    attrs = _pick_style_attributes(seed_key or name)
    result = client.images.generate(
        model=AVATAR_MODEL,
        prompt=_build_avatar_prompt(name, relationship_hint, seed_key),
        size=AVATAR_SIZE,
        quality=AVATAR_QUALITY,
        background="transparent",
        output_format="png",
        n=1,
    )
    b64 = result.data[0].b64_json
    raw_bytes = base64.b64decode(b64)
    return _composite_on_color(raw_bytes, attrs["bg_rgb"])


def _load_relationship_hints(gmail_id: str) -> dict:
    """person.description에서 이메일별 '관계' 한 줄만 뽑아 캐시 없이 즉시 조회한다."""
    hints = {}
    try:
        for row in get_person_descriptions(gmail_id):
            email = (row.get("person_account_id") or "").strip().lower()
            hint = _extract_relationship_hint(row.get("description") or "")
            if email and hint:
                hints[email] = hint
    except Exception as e:
        print(f"[AVATAR] 관계 설명 조회 실패 (스타일 힌트 없이 진행): {e}")
    return hints


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

    relationship_hints = _load_relationship_hints(paths.GMAIL_ID) if targets else {}

    def _generate_one(email, name):
        try:
            image_bytes = generate_avatar_image_bytes(name, relationship_hints.get(email, ""), email)
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
