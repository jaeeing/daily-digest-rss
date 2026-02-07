# digest.py
# Daily Economic Headline Digest (No LLM)
# - RSS(Google News) + GDELT 수집
# - 중복 제거 + 키워드 스코어링
# - Rule-based "단타용" 시그널(방향/강도/Risk-on/off/액션) 분석
# - 이메일(SMTP) / 슬랙(Webhook) 전송
#
# Recommended env (local / GitHub Actions):
#   USE_RSS=1
#   GDELT_MAX=50
#   RSS_MAX=80
#   RECENT_HOURS=72
#   ALLOW_UNDATED_RSS=1
#   DEBUG_RSS_N=0
#
# SMTP (NAVER typically):
#   SMTP_HOST=smtp.naver.com
#   SMTP_PORT=465
#   SMTP_USER=...
#   SMTP_PASS=... (앱 비밀번호)
#   MAIL_FROM=...
#   MAIL_TO=...
#
# Slack (optional):
#   SLACK_WEBHOOK_URL=...

import os
import re
import json
import hashlib
import textwrap
import smtplib
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime
from typing import List, Dict, Tuple, Optional

import requests
import feedparser


# -----------------------------
# Timezones
# -----------------------------
KST = timezone(timedelta(hours=9))
UTC = timezone.utc

# Recent window (default 72h is more robust for Google News RSS)
RECENT_HOURS = int(os.getenv("RECENT_HOURS", "72"))

# RSS: include undated items if True (prevents "0 items" when feeds omit dates)
ALLOW_UNDATED_RSS = os.getenv("ALLOW_UNDATED_RSS", "1") == "1"

# Debug: show N RSS samples per feed (0 disables)
DEBUG_RSS_N = int(os.getenv("DEBUG_RSS_N", "0"))

# Raw preview: print first N collected raw items (0 disables)
RAW_PREVIEW = int(os.getenv("RAW_PREVIEW", "0"))


# -----------------------------
# RSS Feeds
# - Use when:3d to bias toward recent items in Google News search RSS
# -----------------------------
RSS_FEEDS = [
    # KR
    "https://news.google.com/rss/search?q=%EA%B8%88%EB%A6%AC%20%EC%97%B0%EC%A4%80%20%ED%99%98%EC%9C%A8%20%EB%AC%BC%EA%B0%80%20when%3A3d&hl=ko&gl=KR&ceid=KR:ko",
    # US/EN
    "https://news.google.com/rss/search?q=nasdaq%20fed%20inflation%20yield%20when%3A3d&hl=en&gl=US&ceid=US:en",
]


# -----------------------------
# Keyword scoring (simple)
# -----------------------------
KEYWORDS = {
    # Macro / rates
    "금리": 3, "연준": 3, "fed": 3, "fomc": 3, "hawkish": 3, "dovish": 3,
    "inflation": 3, "물가": 3, "cpi": 3, "ppi": 2, "core cpi": 3,

    # Employment / growth
    "고용": 3, "jobs": 3, "nfp": 3, "pmi": 3, "gdp": 2,
    "침체": 3, "recession": 3, "soft landing": 2,

    # FX / bonds
    "환율": 3, "달러": 3, "dollar": 3, "dxy": 3, "yen": 2, "yuan": 2,
    "채권": 2, "국채": 3, "bond": 2, "treasury": 3, "yield": 3, "10-year": 2, "2-year": 2,

    # Equity / tech
    "나스닥": 2, "nasdaq": 2, "s&p": 2, "sp500": 2, "dow": 1,
    "반도체": 3, "semiconductor": 3, "ai": 2, "gpu": 2, "nvidia": 2, "tsmc": 2,

    # Geopolitics / commodities / China
    "지정학": 3, "geopolitics": 3, "전쟁": 3, "war": 3,
    "제재": 3, "sanction": 3, "유가": 3, "oil": 3, "wti": 2, "brent": 2,
    "원자재": 2, "commodities": 2,
    "중국": 2, "china": 2, "stimulus": 2, "pbo c": 2, "property": 2,
}

THEMES = {
    "금리/연준/물가": ["금리", "연준", "fed", "fomc", "hawkish", "dovish", "inflation", "cpi", "ppi", "물가"],
    "환율/달러/국채": ["환율", "달러", "dollar", "dxy", "채권", "국채", "treasury", "yield", "bond", "10-year", "2-year", "yen", "yuan"],
    "미국지표/경기": ["고용", "jobs", "nfp", "pmi", "gdp", "침체", "recession", "soft landing"],
    "기술/반도체/AI": ["나스닥", "nasdaq", "s&p", "sp500", "반도체", "semiconductor", "ai", "gpu", "nvidia", "tsmc"],
    "중국/정책": ["중국", "china", "stimulus", "pbo c", "property", "yuan"],
    "지정학/원자재": ["지정학", "geopolitics", "전쟁", "war", "제재", "sanction", "유가", "oil", "wti", "brent", "원자재", "commodities"],
}

# Sector/asset hints (not stock picks; just trading map)
THEME_HINTS = {
    "금리/연준/물가": "성장주(나스닥)/채권(가격)/은행(순이자마진) 로테이션",
    "환율/달러/국채": "달러강세: 수출/달러매출↑, 수입원가/내수 부담",
    "미국지표/경기": "지표서프라이즈: 지수선물/섹터 로테이션",
    "기술/반도체/AI": "반도체/AI: 나스닥 민감, 변동성↑",
    "중국/정책": "중국부양: 소재/화학/중국노출 소비재",
    "지정학/원자재": "유가/지정학: 정유/방산↑, 항공/운송 부담",
    "기타": "단기 이벤트성/개별 이슈",
}


# -----------------------------
# Utils
# -----------------------------
def norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def stable_id(title: str, link: str) -> str:
    base = f"{norm(title).lower()}|{norm(link).lower()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def fuzzy_key(title: str) -> str:
    t = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", (title or "").lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t[:80]


def within_last_hours(dt: Optional[datetime], hours: int) -> bool:
    if dt is None:
        return False
    now = datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt >= (now - timedelta(hours=hours))


def score_text(title: str, summary: str) -> int:
    text = f"{title} {summary}".lower()
    score = 0
    for k, w in KEYWORDS.items():
        if k.lower() in text:
            score += w
    return score


def classify_themes(title: str, summary: str) -> List[str]:
    text = f"{title} {summary}".lower()
    matched = []
    for theme, keys in THEMES.items():
        for k in keys:
            if k.lower() in text:
                matched.append(theme)
                break
    return matched or ["기타"]


def safe_dt_to_str(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M KST")


# -----------------------------
# RSS datetime extraction (robust)
# -----------------------------
def get_entry_datetime(e) -> Optional[datetime]:
    # published string
    if hasattr(e, "published"):
        try:
            return parsedate_to_datetime(e.published)
        except Exception:
            pass
    # updated string
    if hasattr(e, "updated"):
        try:
            return parsedate_to_datetime(e.updated)
        except Exception:
            pass
    # published_parsed (struct_time)
    pp = getattr(e, "published_parsed", None)
    if pp:
        try:
            return datetime(*pp[:6], tzinfo=UTC)
        except Exception:
            pass
    # updated_parsed (struct_time)
    up = getattr(e, "updated_parsed", None)
    if up:
        try:
            return datetime(*up[:6], tzinfo=UTC)
        except Exception:
            pass
    return None


# -----------------------------
# Fetch: RSS
# -----------------------------
def fetch_rss_items(urls: List[str], max_total: int) -> List[Dict]:
    items: List[Dict] = []
    for url in urls:
        feed = feedparser.parse(url)
        entries = getattr(feed, "entries", []) or []

        # debug header
        if DEBUG_RSS_N > 0:
            print(f"[RSS] url={url}")
            print(f"[RSS] entries={len(entries)} bozo={getattr(feed,'bozo',None)} status={getattr(feed,'status',None)}")
            if getattr(feed, "bozo", 0):
                print(f"[RSS] bozo_exception={getattr(feed,'bozo_exception',None)}")

        for idx, e in enumerate(entries, 1):
            title = norm(getattr(e, "title", ""))
            link = norm(getattr(e, "link", ""))  # Google News RSS link is usually here
            summary = norm(getattr(e, "summary", ""))

            # fallback link from links[]
            if (not link) and hasattr(e, "links") and e.links:
                try:
                    link = norm(e.links[0].get("href", ""))
                except Exception:
                    link = link

            if DEBUG_RSS_N > 0 and idx <= DEBUG_RSS_N:
                print(f"[RSS][sample {idx}] title={title[:100]}")
                print(f"[RSS][sample {idx}] link={link[:140]}")
                print(f"[RSS][sample {idx}] published={getattr(e,'published',None)} updated={getattr(e,'updated',None)}")

            if not title or not link:
                continue

            dt = get_entry_datetime(e)
            if dt is None:
                if not ALLOW_UNDATED_RSS:
                    continue
            else:
                if not within_last_hours(dt, RECENT_HOURS):
                    continue

            items.append({
                "id": stable_id(title, link),
                "title": title,
                "link": link,
                "summary": summary,
                "source": "Google News RSS",
                "dt": dt,  # store datetime object
            })

            if len(items) >= max_total:
                return items

    return items


# -----------------------------
# Fetch: GDELT (Doc 2.0 ArtList)
# -----------------------------
def gdelt_dt(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%d%H%M%S")


def fetch_gdelt_last_hours(query: str, max_records: int) -> List[Dict]:
    """
    GDELT DOC 2.0 (ArtList)
    - start/end datetime already filters time window; avoid extra seendate parsing filters (can cause 0 items)
    """
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    end = datetime.utcnow()
    start = end - timedelta(hours=RECENT_HOURS)

    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "sort": "HybridRel",
        "maxrecords": str(max_records),
        "startdatetime": gdelt_dt(start),
        "enddatetime": gdelt_dt(end),
    }

    r = requests.get(base, params=params, timeout=30)
    if not r.ok:
        raise RuntimeError(f"GDELT HTTP {r.status_code}: {r.text[:300]}")

    ct = (r.headers.get("content-type") or "").lower()
    if "json" not in ct:
        raise RuntimeError(f"GDELT non-JSON response (content-type={ct}): {r.text[:300]}")

    try:
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"GDELT JSON decode failed: {e}; body head={r.text[:300]}")

    arts = data.get("articles", []) or []
    items: List[Dict] = []

    for a in arts:
        title = norm(a.get("title", ""))
        link = norm(a.get("url", ""))
        if not title or not link:
            continue

        # seendate may be string like YYYYMMDDHHMMSS
        dt = None
        sd = norm(a.get("seendate", ""))
        if sd.isdigit() and len(sd) >= 14:
            try:
                dt = datetime.strptime(sd[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            except Exception:
                dt = None

        items.append({
            "id": stable_id(title, link),
            "title": title,
            "link": link,
            "summary": "",  # ArtList often lacks summary
            "source": norm(a.get("domain", "")) or "GDELT",
            "dt": dt,
        })

    return items


# -----------------------------
# Dedupe + score + themes
# -----------------------------
def dedupe_score(items: List[Dict], top_n: int) -> List[Dict]:
    seen_id = set()
    seen_fuzzy = set()
    out: List[Dict] = []

    for it in items:
        sid = it.get("id") or stable_id(it.get("title", ""), it.get("link", ""))
        fk = fuzzy_key(it.get("title", ""))

        if sid in seen_id:
            continue
        if fk in seen_fuzzy:
            continue

        seen_id.add(sid)
        seen_fuzzy.add(fk)

        title = it.get("title", "")
        summary = it.get("summary", "")
        it["id"] = sid
        it["score"] = score_text(title, summary)
        it["themes"] = classify_themes(title, summary)
        out.append(it)

    out.sort(key=lambda x: (x.get("score", 0), x.get("title", "")), reverse=True)
    return out[:top_n]


# -----------------------------
# Rule-based trading signal analysis (no LLM)
# -----------------------------
RISK_OFF_TERMS = [
    "hawkish", "rate hike", "hike", "hot inflation", "inflation accelerat", "cpi beat", "ppi beat",
    "yield surge", "yields surge", "bond selloff", "risk-off", "sell-off",
    "geopolitics", "war", "attack", "sanction", "tension",
    "oil spike", "oil jumps", "wti jumps", "brent jumps",
]
RISK_ON_TERMS = [
    "dovish", "rate cut", "cut", "inflation cooling", "cpi miss", "yields fall", "bond rally",
    "soft landing", "stimulus", "easing", "risk-on",
]

STRONG_TERMS = ["surge", "spike", "soar", "plunge", "emergency", "attack", "sanction", "shock", "unexpected", "record"]
MEDIUM_TERMS = ["jump", "rise", "fall", "drop", "warn", "concern", "weighs", "boost", "pressure"]

DIRECTION_UP_TERMS = ["cut", "dovish", "cooling", "yields fall", "bond rally", "stimulus", "easing", "beats expectations", "record profit"]
DIRECTION_DOWN_TERMS = ["hike", "hawkish", "hot inflation", "yields surge", "bond selloff", "sanction", "attack", "tension", "oil spike"]


def analyze_signal(title: str, summary: str, themes: List[str], score: int) -> Dict:
    text = f"{title} {summary}".lower()

    # risk mode
    risk_off = any(t in text for t in RISK_OFF_TERMS)
    risk_on = any(t in text for t in RISK_ON_TERMS)
    if risk_off and not risk_on:
        risk_mode = "Risk-off"
    elif risk_on and not risk_off:
        risk_mode = "Risk-on"
    else:
        # fallback: rates/war/oil tends to risk-off
        if any(k in text for k in ["hawkish", "yield", "war", "attack", "sanction", "oil", "inflation"]):
            risk_mode = "Risk-off"
        else:
            risk_mode = "Mixed"

    # direction
    down = any(t in text for t in DIRECTION_DOWN_TERMS)
    up = any(t in text for t in DIRECTION_UP_TERMS)
    if up and not down:
        direction = "↑"
    elif down and not up:
        direction = "↓"
    else:
        direction = "→"

    # strength (stars)
    strength_score = 0
    strength_score += min(6, score)  # keyword score contributes

    if any(t in text for t in STRONG_TERMS):
        strength_score += 4
    elif any(t in text for t in MEDIUM_TERMS):
        strength_score += 2

    # theme emphasis
    if any(t in themes for t in ["금리/연준/물가", "환율/달러/국채", "지정학/원자재"]):
        strength_score += 2

    if strength_score >= 10:
        strength = "상"
        stars = "⭐⭐⭐"
    elif strength_score >= 6:
        strength = "중"
        stars = "⭐⭐"
    else:
        strength = "하"
        stars = "⭐"

    # trade action
    if strength == "상" and direction in ("↑", "↓"):
        trade_action = "시초가 관찰 후 5~15분 눌림목/반등 시도"
    elif strength == "중":
        trade_action = "초반 변동성 확인 후 분할/관망"
    else:
        trade_action = "관심등록(관망)"

    # 1-line summary (rule-based)
    theme_tag = themes[0] if themes else "기타"
    one_liner = f"{theme_tag} 이슈 → {risk_mode}, 방향 {direction}, 강도 {strength}"

    # keyword hits (top few keywords present)
    hits = []
    for k in KEYWORDS.keys():
        if k.lower() in text:
            hits.append(k)
        if len(hits) >= 6:
            break

    return {
        "risk_mode": risk_mode,
        "direction": direction,
        "strength": strength,
        "stars": stars,
        "trade_action": trade_action,
        "one_liner": one_liner,
        "hits": hits,
    }


# -----------------------------
# Report (RICE-ish, aggressive formatting)
# -----------------------------

def build_report(items: List[Dict]) -> Tuple[str, str, str]:
    now_kst = datetime.now(UTC).astimezone(KST)
    subject = f"[Daily Digest] {now_kst:%Y-%m-%d %H:%M} KST"

    # signals
    enriched = []
    strength_rank = {"상": 3, "중": 2, "하": 1}

    for it in items:
        sig = analyze_signal(
            it.get("title", ""),
            it.get("summary", ""),
            it.get("themes", ["기타"]),
            it.get("score", 0),
        )
        it2 = dict(it)
        it2["signal"] = sig
        enriched.append(it2)

    # rank: strength then score
    enriched.sort(key=lambda x: (strength_rank.get(x["signal"]["strength"], 1), x.get("score", 0)), reverse=True)

    top3 = enriched[:3]

    # theme buckets
    theme_buckets: Dict[str, List[Dict]] = defaultdict(list)
    for it in enriched:
        for th in it.get("themes", ["기타"]):
            theme_buckets[th].append(it)

    theme_order = list(THEMES.keys()) + ["기타"]
    theme_rows = []
    for th in theme_order:
        if th not in theme_buckets:
            continue
        best = theme_buckets[th][:2]
        if not best:
            continue
        news_titles = " / ".join([b["title"][:55] + ("…" if len(b["title"]) > 55 else "") for b in best])
        max_sig = max(best, key=lambda x: strength_rank.get(x["signal"]["strength"], 1))["signal"]
        theme_rows.append((th, news_titles, max_sig["risk_mode"], max_sig["stars"]))

    checklist = [
        "프리마켓/선물: 나스닥 선물 방향",
        "미국채(10Y/2Y) 금리 급등/급락",
        "달러인덱스(DXY) & USD/KRW 갭",
        "WTI/Brent 유가 급등 여부",
        "테마 로테이션: 반도체/AI vs 방산/정유 vs 은행",
        "장 초반 15분 변동성(휩쏘) 경계",
    ]

    # --------
    # TEXT (fallback)
    # --------
    t = []
    t.append(f"Daily Digest ({now_kst:%Y-%m-%d %H:%M} KST) / Window: last {RECENT_HOURS}h / items: {len(items)}")
    t.append("")
    t.append("== Top 3 ==")
    for i, it in enumerate(top3, 1):
        sig = it["signal"]
        ths = ", ".join(it.get("themes", ["기타"]))
        title = it.get("title", "")
        link = it.get("link", "")
        t.append(f"{i}. [{sig['risk_mode']}/{sig['direction']}/{sig['strength']}{sig['stars']}] {title}")
        t.append(f"   - themes: {ths} / score={it.get('score',0)} / hint: {THEME_HINTS.get(it.get('themes',['기타'])[0], THEME_HINTS['기타'])}")
        t.append(f"   - action: {sig['trade_action']}")
        t.append(f"   - {link}")
        t.append("")

    t.append("== Themes ==")
    for row in theme_rows[:10]:
        t.append(f"- {row[0]} | {row[2]} | {row[3]} | {row[1]}")
    t.append("")

    t.append("== Checklist ==")
    for c in checklist:
        t.append(f"- [ ] {c}")
    t.append("")

    t.append("== Top 10 (browse) ==")
    for i, it in enumerate(enriched[:10], 1):
        sig = it["signal"]
        title = it.get("title", "")
        link = it.get("link", "")
        t.append(f"{i:02d}. [{sig['risk_mode']}/{sig['direction']}/{sig['strength']}{sig['stars']}] {title} ({link})")

    text_body = "\n".join(t)

    # --------
    # HTML (primary)
    # --------
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    html = []
    html.append("<html><body style='font-family: -apple-system, Segoe UI, Roboto, Arial; font-size: 14px;'>")
    html.append(f"<div style='color:#666;margin-bottom:10px;'>Generated: {now_kst:%Y-%m-%d %H:%M} KST · Window: last {RECENT_HOURS}h · items: {len(items)}</div>")

    html.append("<h2 style='margin:14px 0 8px;'>📰 Top 3</h2>")

    for i, it in enumerate(top3, 1):
        sig = it["signal"]
        ths = ", ".join(it.get("themes", ["기타"]))
        title = esc(it.get("title", ""))
        link = it.get("link", "")
        summary = esc(it.get("summary", "") or "(요약 없음)")
        hint = esc(THEME_HINTS.get(it.get("themes", ["기타"])[0], THEME_HINTS["기타"]))
        hits = esc(", ".join(sig["hits"]) if sig.get("hits") else "-")
        src = esc(it.get("source", ""))
        dt_str = esc(safe_dt_to_str(it.get("dt")))

        html.append(f"<h3 style='margin:12px 0 6px;'>🔥 {i}순위: <a href='{link}'>{title}</a></h3>")
        html.append("<table style='border-collapse:collapse;width:100%;max-width:900px;'>")

        def tr(k, v):
            html.append(
                "<tr>"
                f"<td style='border:1px solid #ddd;padding:8px;background:#fafafa;width:140px;vertical-align:top;'><b>{k}</b></td>"
                f"<td style='border:1px solid #ddd;padding:8px;vertical-align:top;'>{v}</td>"
                "</tr>"
            )

        tr("뉴스 요약", summary[:220] + ("…" if len(summary) > 220 else ""))
        tr("시장 영향", esc(sig["risk_mode"]))
        tr("방향/강도", f"{esc(sig['direction'])} · {esc(sig['strength'])} {esc(sig['stars'])}")
        tr("관련 테마", esc(ths))
        tr("테마 힌트", hint)
        tr("매매 전략", esc(sig["trade_action"]))
        tr("체크 키워드", hits)
        tr("소스/시간", f"{src}{(' / ' + dt_str) if dt_str else ''}")

        html.append("</table>")

    # Theme table
    html.append("<h2 style='margin:18px 0 8px;'>📊 Themes</h2>")
    html.append("<table style='border-collapse:collapse;width:100%;max-width:900px;'>")
    html.append(
        "<tr>"
        "<th style='border:1px solid #ddd;padding:8px;background:#f3f3f3;text-align:left;'>테마</th>"
        "<th style='border:1px solid #ddd;padding:8px;background:#f3f3f3;text-align:left;'>관련 뉴스(Top)</th>"
        "<th style='border:1px solid #ddd;padding:8px;background:#f3f3f3;text-align:left;'>시그널</th>"
        "<th style='border:1px solid #ddd;padding:8px;background:#f3f3f3;text-align:left;'>강도</th>"
        "</tr>"
    )
    for th, news_titles, sig_mode, stars in theme_rows[:10]:
        html.append(
            "<tr>"
            f"<td style='border:1px solid #ddd;padding:8px;'>{esc(th)}</td>"
            f"<td style='border:1px solid #ddd;padding:8px;'>{esc(news_titles)}</td>"
            f"<td style='border:1px solid #ddd;padding:8px;'>{esc(sig_mode)}</td>"
            f"<td style='border:1px solid #ddd;padding:8px;'>{esc(stars)}</td>"
            "</tr>"
        )
    html.append("</table>")

    # Checklist
    html.append("<h2 style='margin:18px 0 8px;'>✅ Checklist</h2>")
    html.append("<ul>")
    for c in checklist:
        html.append(f"<li>{esc(c)}</li>")
    html.append("</ul>")

    # Top 10 list with title-linked
    html.append("<h2 style='margin:18px 0 8px;'>🧾 Top 10 (browse)</h2>")
    html.append("<ol>")
    for it in enriched[:10]:
        sig = it["signal"]
        title = esc(it.get("title", ""))
        link = it.get("link", "")
        html.append(
            f"<li>[{esc(sig['risk_mode'])}/{esc(sig['direction'])}/{esc(sig['strength'])}{esc(sig['stars'])}] "
            f"<a href='{link}'>{title}</a></li>"
        )
    html.append("</ol>")

    html.append("</body></html>")
    html_body = "\n".join(html)

    return subject, text_body, html_body

# -----------------------------
# Delivery: Slack + Email(SMTP)
# -----------------------------
def send_slack(webhook_url: str, text: str) -> None:
    chunks = textwrap.wrap(text, width=3500, break_long_words=False, replace_whitespace=False)
    for idx, chunk in enumerate(chunks, 1):
        payload = {"text": f"*Part {idx}/{len(chunks)}*\n```{chunk}```" if len(chunks) > 1 else f"```{chunk}```"}
        r = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=20)
        r.raise_for_status()


def send_email_smtp(host: str, port: int, user: str, pw: str,
                    mail_from: str, mail_to: str, subject: str, body: str) -> None:
    """
    - 465: SMTP_SSL
    - 587: STARTTLS
    """
    msg = MIMEMultipart()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=30) as s:
            s.ehlo()
            s.login(user, pw)
            s.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(user, pw)
            s.send_message(msg)


# -----------------------------
# Main
# -----------------------------
def main():
    # Collection toggles
    use_rss = os.getenv("USE_RSS", "1") == "1"
    gdelt_max = int(os.getenv("GDELT_MAX", "50"))
    rss_max = int(os.getenv("RSS_MAX", "80"))

    # GDELT query (OR terms must be wrapped with parentheses in GDELT)
    gdelt_query = os.getenv(
        "GDELT_QUERY",
        "rate OR fed OR inflation OR fx OR dollar OR bond OR treasury OR yield OR nasdaq OR semiconductor OR ai OR recession OR jobs OR pmi OR china OR geopolitics OR oil"
    ).strip()
    if " OR " in gdelt_query and not gdelt_query.startswith("("):
        gdelt_query = f"({gdelt_query})"

    # Delivery options
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()

    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "465"))  # NAVER default
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_pass = os.getenv("SMTP_PASS", "").strip()
    mail_from = os.getenv("MAIL_FROM", "").strip()
    mail_to = os.getenv("MAIL_TO", "").strip()

    items: List[Dict] = []

    # 1) GDELT
    if gdelt_max > 0:
        try:
            items += fetch_gdelt_last_hours(gdelt_query, gdelt_max)
        except Exception as e:
            print(f"[WARN] GDELT fetch failed: {e}")

    # 2) RSS
    if use_rss:
        try:
            items += fetch_rss_items(RSS_FEEDS, rss_max)
        except Exception as e:
            print(f"[WARN] RSS fetch failed: {e}")

    # RAW preview
    if RAW_PREVIEW > 0:
        print(f"\n[RAW] collected items = {len(items)} (GDELT={'on' if gdelt_max>0 else 'off'}, RSS={'on' if use_rss else 'off'})")
        for i, it in enumerate(items[:RAW_PREVIEW], 1):
            dt_str = safe_dt_to_str(it.get("dt"))
            print(f"{i:02d}. {it.get('title','')} [{it.get('source','')}] {dt_str}")
            print(f"    {it.get('link','')}")

    # Dedupe + score
    ranked = dedupe_score(items, top_n=60)

    # Report
    subject, text_body, html_body = build_report(ranked)

    # Local output (for logs)
    print(text_body)
    print("\nDone:", subject)

    # Deliver
    if slack_webhook:
        try:
            send_slack(slack_webhook, body)
        except Exception as e:
            print(f"[WARN] Slack send failed: {e}")

    if smtp_host and smtp_user and smtp_pass and mail_from and mail_to:
        try:
            send_email_smtp(smtp_host, smtp_port, smtp_user, smtp_pass,
                mail_from, mail_to, subject, text_body, html_body)
        except Exception as e:
            print(f"[WARN] Email send failed: {e}")


if __name__ == "__main__":
    main()