import os
import io
import re
import json
import base64
import hashlib
import threading
import requests
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from openai import OpenAI
from PIL import Image, ImageChops

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


# 초대형 브랜드는 LLM 판별이 흔들릴 수 있어(도메인이 발송대행사인 경우 등) 확정 매핑을 우선 사용한다.
_KNOWN_BRAND_DOMAINS = {
    "instagram": "instagram.com", "pinterest": "pinterest.com", "google": "google.com",
    "google play": "google.com", "mcafee": "mcafee.com", "twitter": "x.com", "x": "x.com",
    "discord": "discord.com", "microsoft": "microsoft.com", "xbox": "xbox.com",
    "neo4j": "neo4j.com", "the neo4j team": "neo4j.com", "facebook": "facebook.com",
    "linkedin": "linkedin.com", "naver": "naver.com", "kakao": "kakaocorp.com",
    "amazon": "amazon.com", "apple": "apple.com", "netflix": "netflix.com",
    "youtube": "youtube.com", "spotify": "spotify.com", "slack": "slack.com",
    "zoom": "zoom.us", "adobe": "adobe.com", "dropbox": "dropbox.com",
    "paypal": "paypal.com", "ebay": "ebay.com", "samsung": "samsung.com",
    "lg": "lg.com", "steam": "steampowered.com", "playstation": "playstation.com",
    "nintendo": "nintendo.com", "airbnb": "airbnb.com", "uber": "uber.com",
    "github": "github.com", "figma": "figma.com", "notion": "notion.so",
}


def _classify_sender(name: str, domain: str) -> str | None:
    """
    표시 이름/이메일 도메인만 보고 이 발신자가 실제로 존재하는 기업/서비스의
    자동 발송(알림, 뉴스레터, 영수증 등) 계정인지 판별한다.
    실제 기업이면 로고를 찾을 공식 웹사이트 도메인을 반환하고, 실제 개인이거나
    어떤 기업인지 확실하지 않으면 None을 반환한다(→ 일러스트 아바타로 대체).
    """
    known = _KNOWN_BRAND_DOMAINS.get((name or "").strip().lower())
    if known:
        return known
    try:
        result = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "이메일 발신자 정보를 보고 이것이 실제로 존재하는 기업/서비스가 보낸 "
                        "자동 발송 계정(알림, 뉴스레터, 영수증, 마케팅 메일 등)인지 판단하는 AI입니다. "
                        "표시 이름이 유명 기업/서비스 이름과 일치하면, 이메일 도메인이 발송대행사 "
                        "도메인(예: sendgrid.net, mailgun.org, amazonses.com 등)이라서 그 기업의 "
                        "공식 도메인과 달라 보여도 표시 이름을 우선 신뢰해 그 기업으로 판단하세요. "
                        "실제로 존재하는 기업/서비스라면 그 기업의 공식 웹사이트 도메인만 "
                        "(예: google.com) 정확히 출력하세요. 실제 사람 개인 계정이거나 "
                        "어느 기업인지 확실하지 않으면 정확히 'PERSON'이라고만 출력하세요."
                    ),
                },
                {"role": "user", "content": f"표시 이름: {name}\n이메일 도메인: {domain}"},
            ],
            temperature=0,
        )
        answer = (result.choices[0].message.content or "").strip()
        if not answer or answer.upper() == "PERSON":
            return None
        m = re.search(r"[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", answer)
        return m.group(0).lower() if m else None
    except Exception as e:
        print(f"[AVATAR] 기업 판별 실패 ({name}): {e}")
        return None


def _logo_content_mask(logo: Image.Image) -> Image.Image:
    """로고 이미지에서 실제로 눈에 보이는 도형 픽셀만 표시하는 "L" 모드 마스크를 만든다.

    단순히 `alpha.getbbox()`만 쓰면 눈에는 안 보이는 극히 옅은 알파(1~수십 수준)나
    안티에일리어싱으로 생긴 아주 옅은 회색조 픽셀까지 "내용물"로 잡혀 마스크가
    이미지 가장자리까지 부풀어버리는 경우가 있었다. 실제로 눈에 뚜렷이 보이는
    픽셀만 기준으로 삼도록 임계값을 둔다."""
    alpha = logo.split()[-1]
    if alpha.getextrema()[0] < 250:  # 투명 배경이 있는 이미지 → 알파 기준
        return alpha.point(lambda a: 255 if a >= 32 else 0)
    # 불투명(흰 배경) 이미지 → 흰색과 뚜렷이 다른 영역 기준
    rgb = logo.convert("RGB")
    diff = ImageChops.difference(rgb, Image.new("RGB", rgb.size, (255, 255, 255))).convert("L")
    return diff.point(lambda d: 255 if d >= 24 else 0)


def _trim_logo_padding(logo: Image.Image) -> tuple[Image.Image, Image.Image] | tuple[None, None]:
    """로고를 실제 도형 경계까지 크롭하고, 그 도형의 내용 마스크를 함께 반환한다."""
    w, h = logo.size
    mask = _logo_content_mask(logo)
    bbox = mask.getbbox()
    if not bbox:
        return logo, mask
    # 임계값 처리 과정에서 실제 형상 가장자리의 부드러운 픽셀 한두 줄이 잘려나갈 수 있으니
    # 소폭 여유를 되돌려준다(과도한 크롭으로 로고 윤곽이 뭉개지는 것을 방지).
    pad = max(1, round(max(w, h) * 0.01))
    left, top, right, bottom = bbox
    bbox = (max(0, left - pad), max(0, top - pad), min(w, right + pad), min(h, bottom + pad))
    return logo.crop(bbox), mask.crop(bbox)


def _logo_badge_color(logo: Image.Image, mask: Image.Image) -> tuple[int, int, int] | None:
    """
    로고 도형이 이미 그 자체로 꽉 찬 색깔 배지(예: Pinterest의 빨간 원+흰 P, Discord의
    블러플 원+흰 아이콘)인지, 아니면 배경 없이 심볼만 있는 얇은 단색 마크(예: McAfee의
    방패)인지 판별한다. 후자라면 그 마크의 실제 색을 배지 배경색으로 뽑아 반환하고,
    이미 배지 형태이거나 다색(Instagram/Google처럼)이면 None을 반환해 원본 그대로 둔다.
    """
    rgb = logo.convert("RGB")
    pixels = list(rgb.getdata())
    mask_data = list(mask.getdata())
    content = [px for px, m in zip(pixels, mask_data) if m]
    if not content:
        return None

    fill_ratio = len(content) / (logo.width * logo.height)
    if fill_ratio >= 0.68:
        # 이미 도형 자체가 원/사각형을 꽉 채운 배지 형태 → 그대로 사용
        return None

    # 색 다양성 검사: 양자화한 색상 버킷 중 하나가 압도적 비중이면 "단색 마크"로 본다.
    buckets = Counter((r // 32, g // 32, b // 32) for r, g, b in content)
    top_bucket, top_count = buckets.most_common(1)[0]
    if top_count / len(content) < 0.75:
        # Instagram/Google처럼 여러 색이 섞인 다색 로고 → 재색칠하지 않고 그대로 사용
        return None

    top_pixels = [
        px for px, m in zip(pixels, mask_data)
        if m and (px[0] // 32, px[1] // 32, px[2] // 32) == top_bucket
    ]
    r = sum(p[0] for p in top_pixels) // len(top_pixels)
    g = sum(p[1] for p in top_pixels) // len(top_pixels)
    b = sum(p[2] for p in top_pixels) // len(top_pixels)
    return (r, g, b)


def _pad_logo_square(image_bytes: bytes, canvas_size: int = 512) -> bytes:
    """
    Figma에서 원 프레임에 이미지를 채우기(Fill)하듯, 로고를 정사각형 캔버스에
    여백 없이 꽉 채운다. 프론트엔드가 이 정사각형을 원형으로 마스킹해서 보여주므로,
    캔버스 네 모서리는 어차피 원 밖이라 안 보인다 — "원 안에 다 들어가게" 크기를
    역산할 필요 없이 그냥 캔버스를 완전히 채우기만 하면 결과적으로 원이 꽉 찬다.

    도형만 있고 배경이 없는 얇은 단색 마크는 채워도 흐릿하게 떠 보이므로, 그 마크의
    실제 색을 배경색으로 쓰고 마크 자체는 흰색으로 바꿔 배지 스타일로 통일한다.
    이미 배지 형태이거나 다색(Instagram/Google 등)이면 원본 그대로 흰 배경에 채운다.
    """
    logo = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    logo, mask = _trim_logo_padding(logo)
    badge_color = _logo_badge_color(logo, mask)

    if badge_color is not None:
        white_glyph = Image.new("RGBA", logo.size, (255, 255, 255, 255))
        white_glyph.putalpha(mask)
        logo = white_glyph
        bg_rgba = badge_color + (255,)
    else:
        bg_rgba = (255, 255, 255, 255)

    # cover(꽉 채우기): 짧은 변을 캔버스 크기에 맞춰 확대해 여백 없이 채운다.
    # 파비콘처럼 아주 작은 원본을 큰 배율로 늘리면 뭉개져 보이므로 배율 자체에 상한을 둔다.
    scale = min(max(canvas_size / logo.width, canvas_size / logo.height), 6.0)
    new_size = (max(1, round(logo.width * scale)), max(1, round(logo.height * scale)))
    logo = logo.resize(new_size, Image.LANCZOS)
    canvas = Image.new("RGBA", (canvas_size, canvas_size), bg_rgba)
    x, y = (canvas_size - logo.width) // 2, (canvas_size - logo.height) // 2
    canvas.paste(logo, (x, y), logo)
    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG")
    return out.getvalue()


_BRAND_LOGOS_DIR = os.path.join(os.path.dirname(__file__), "brand_logos")

# Clearbit/파비콘이 화질이 낮거나 배지 형태가 아닌 로고를 주는 브랜드는
# 직접 준비한 원본 이미지를 우선 사용한다.
_HARDCODED_LOGO_FILES = {
    "pinterest.com": "pinterest.png",
    "mcafee.com": "mcafee.png",
    "neo4j.com": "neo4j.png",
}


def _place_hardcoded_logo(image_bytes: bytes, canvas_size: int = 512) -> bytes:
    """
    직접 고른 완성도 있는 로고 이미지를 위한 단순 배치. 배지 재색칠이나 꽉 채우기(cover)
    크롭 없이, 여백만 다듬어 자르고 잘리지 않게 캔버스 안에 맞춘다(contain) — 이미
    보기 좋은 이미지이므로 재해석하지 않고 그대로 살린다.
    """
    logo = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    logo, _ = _trim_logo_padding(logo)
    target = int(canvas_size * 0.68)
    scale = min(target / max(logo.width, logo.height), 6.0)
    new_size = (max(1, round(logo.width * scale)), max(1, round(logo.height * scale)))
    logo = logo.resize(new_size, Image.LANCZOS)
    canvas = Image.new("RGBA", (canvas_size, canvas_size), (255, 255, 255, 255))
    x, y = (canvas_size - logo.width) // 2, (canvas_size - logo.height) // 2
    canvas.paste(logo, (x, y), logo)
    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG")
    return out.getvalue()


def _fetch_company_logo(domain: str) -> bytes | None:
    """공개 로고 서비스에서 실제 기업 로고를 가져온다. 실패 시 None."""
    hardcoded = _HARDCODED_LOGO_FILES.get(domain)
    if hardcoded:
        filepath = os.path.join(_BRAND_LOGOS_DIR, hardcoded)
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                return _place_hardcoded_logo(f.read())

    for url in (
        f"https://logo.clearbit.com/{domain}?size=256",
        f"https://www.google.com/s2/favicons?sz=256&domain={domain}",
    ):
        try:
            res = requests.get(url, timeout=8)
            if res.status_code == 200 and res.content and len(res.content) > 200:
                return _pad_logo_square(res.content)
        except Exception as e:
            print(f"[AVATAR] 로고 요청 실패 ({url}): {e}")
    return None


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
    # 안티에일리어싱으로 생긴 흐릿한 알파 언저리는 실제로 눈에 보이지 않으므로
    # bbox 기준에서 제외한다 — 그 언저리까지 "내용물"로 잡으면 실제 옷/몸이
    # 바닥까지 채워지지 못하고 그 아래로 배경색 여백이 보이게 된다.
    solid_alpha = alpha.point(lambda a: 255 if a >= 128 else 0)
    bbox = solid_alpha.getbbox()
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


def _load_relationship_hints(user_id: str) -> dict:
    """person.description에서 이메일별 '관계' 한 줄만 뽑아 캐시 없이 즉시 조회한다."""
    hints = {}
    try:
        for row in get_person_descriptions(user_id):
            email = (row.get("person_account_id") or "").strip().lower()
            hint = _extract_relationship_hint(row.get("description") or "")
            if email and hint:
                hints[email] = hint
    except Exception as e:
        print(f"[AVATAR] 관계 설명 조회 실패 (스타일 힌트 없이 진행): {e}")
    return hints


def get_cached_person_avatars(paths) -> dict:
    return _load_avatar_map(paths)


_SELF_AVATAR_KEY = "__self__"


def get_cached_self_avatar(paths):
    """로그인한 사용자 본인의 아바타 캐시를 조회한다. 없으면 None."""
    return _load_avatar_map(paths).get(_SELF_AVATAR_KEY)


def generate_self_avatar(paths, name: str) -> str:
    """로그인한 사용자 본인의 아바타를 (없으면) 한 번 생성해 캐시하고 URL을 반환한다.
    사람 카드와 같은 일러스트 아바타 파이프라인을 그대로 재사용하되, 이메일이 아닌
    고정 키(__self__)로 캐시해서 실제 연락처 이메일과 절대 충돌하지 않게 한다."""
    avatar_map = _load_avatar_map(paths)
    cached = avatar_map.get(_SELF_AVATAR_KEY)
    if cached:
        return cached

    os.makedirs(paths.AVATAR_IMAGES_DIR, exist_ok=True)
    image_bytes = generate_avatar_image_bytes(name or "나", "", paths.USER_ID + ":self")
    filename = _avatar_filename(_SELF_AVATAR_KEY + ":" + paths.USER_ID)
    filepath = os.path.join(paths.AVATAR_IMAGES_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(image_bytes)
    url = f"/person-avatar-image/{paths.USER_ID}/{filename}"
    with _map_lock:
        avatar_map[_SELF_AVATAR_KEY] = url
        _save_avatar_map(paths, avatar_map)
    return url


def generate_person_avatars_batch(paths, people: list) -> dict:
    """
    people: [{ "email": str, "name": str }, ...]
    이미 캐시된 사람은 건너뛰고, 새로운 발신자만 처리한다. 발신자별로 먼저 LLM에게
    실제 존재하는 기업/서비스인지 물어보고, 기업이면 실제 로고 이미지를, 아니면(개인)
    GPT 이미지 API로 생성한 일러스트 아바타를 사용한다.
    반환: { email_lower: "/person-avatar-image/<user_id>/<filename>" } (요청한 사람 전체에 대한 매핑)
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
            domain = email.split("@", 1)[1] if "@" in email else ""
            targets.append((email, name, domain))

    relationship_hints = _load_relationship_hints(paths.USER_ID) if targets else {}

    def _generate_one(email, name, domain):
        try:
            brand_domain = _classify_sender(name, domain)
            image_bytes = _fetch_company_logo(brand_domain) if brand_domain else None
            is_logo = image_bytes is not None
            if image_bytes is None:
                image_bytes = generate_avatar_image_bytes(name, relationship_hints.get(email, ""), email)

            filename = _avatar_filename(email)
            filepath = os.path.join(paths.AVATAR_IMAGES_DIR, filename)
            with open(filepath, "wb") as f:
                f.write(image_bytes)
            url = f"/person-avatar-image/{paths.USER_ID}/{filename}"
            with _map_lock:
                avatar_map[email] = url
                _save_avatar_map(paths, avatar_map)
            print(f"[AVATAR] 생성 완료: {email} ({name}){' [기업 로고]' if is_logo else ''}")
            return email, url
        except Exception as e:
            print(f"[AVATAR] 생성 실패 ({email}): {e}")
            return email, None

    if targets:
        with ThreadPoolExecutor(max_workers=min(len(targets), 3)) as executor:
            futures = [executor.submit(_generate_one, email, name, domain) for email, name, domain in targets]
            for future in as_completed(futures):
                future.result()

    return {email: avatar_map[email] for email in seen if email in avatar_map}
