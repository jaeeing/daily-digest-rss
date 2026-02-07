# digest.py
# No-LLM Daily Digest: GDELT/RSS 수집 → (최근 N시간 필터) → 키워드 점수화/테마 분류
# → 전체 Top10 + 테마 Top2 + 키워드 히트 → (옵션) Email/Slack 전송

import os
import re
import json
import hashlib
import textwrap
import smtplib
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime
from typing import List, Dict, Tuple, Optional

import requests
import feedparser

# -----------------------------
# RSS Feeds (optional)
# -----------------------------
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=%EA%B8%88%EB%A6%AC%20%EC%97%B0%EC%A4%80%20%ED%99%98%EC%9C%A8%20%EB%AC%BC%EA%B0%80%20when%3A3d&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=nasdaq%20fed%20inflation%20yield%20when%3A3d&hl=en&gl=US&ceid=US:en",
]

# -----------------------------
# Keyword scoring
# -----------------------------
KEYWORDS = {
    # Macro
    "금리": 3, "연준": 3, "fed": 3, "fomc": 3, "inflation": 3, "물가": 3, "cpi": 3, "ppi": 2,
    "고용": 3, "jobs": 3, "nfp": 3, "pmi": 3, "침체": 3, "recession": 3,

    # FX / Rates / Bonds
    "환율": 3, "달러": 3, "dollar": 3, "yen": 2, "yuan": 2,
    "채권": 2, "국채": 3, "bond": 2, "treasury": 3, "yield": 3,

    # Equity / Tech
    "나스닥": 2, "nasdaq": 2, "s&p": 2, "sp500": 2,
    "반도체": 3, "semiconductor": 3, "nvidia": 2, "ai": 2,

    # Geopolitics / China / Commodities
    "지정학": 3, "geopolitics": 3, "중국": 2, "china": 2, "제재": 3, "sanction": 3,
    "전쟁": 3, "war": 3, "유가": 3, "oil": 3, "원자재": 2, "commodities": 2,
}

THEMES = {
    "금리/연준/물가": ["금리", "연준", "fed", "fomc", "inflation", "cpi", "ppi", "물가"],
    "환율/달러/채권": ["환율", "달러", "dollar", "채권", "국채", "treasury", "yield", "bond", "yen", "yuan"],
    "미국지표/경기": ["고용", "jobs", "nfp", "pmi", "침체", "recession"],
    "기술/반도체/AI": ["나스닥", "nasdaq", "반도체", "semiconductor", "nvidia", "ai", "s&p", "sp500"],
    "중국/정책": ["중국", "china", "yuan"],
    "지정학/원자재": ["지정학", "geopolitics", "전쟁", "war", "제재", "sanction", "유가", "oil", "원자재", "commodities"],
}

KST = timezone(timedelta(hours=9))

# 최근 N시간만 남김 (기본 24시간)
RECENT_HOURS = int(os.getenv("RECENT_HOURS", "24"))


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
    """
    유사 제목 중복 제거용 키.
    - 특수문자 제거
    - 공백 정리
    - 앞부분 N자만 사용
    """
    t = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", (title or "").lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t[:70]


def score_text(title: str, summary: str) -> int:
    text = f"{title} {summary}".lower()
    score = 0
    for k, w in KEYWORDS.items():
        if k.lower() in text:
            score += w
    return score


def classify_theme(title: str, summary: str) -> List[str]:
    text = f"{title} {summary}".lower()
    matched = []
    for theme, keys in THEMES.items():
        for k in keys:
            if k.lower() in text:
                matched.append(theme)
                break
    return matched or ["기타"]


def within_last_hours(dt: Optional[datetime], hours: int) -> bool:
    if dt is None:
        return False
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= (now - timedelta(hours=hours))


# -----------------------------
# GDELT: last 24h
# -----------------------------
def gdelt_dt(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%d%H%M%S")


def parse_gdelt_seendate(sd: str) -> Optional[datetime]:
    """
    seendate가 숫자(YYYYMMDDHHMMSS) 형태로 오는 경우가 많음.
    실패하면 None.
    """
    sd = norm(sd)
    if sd.isdigit() and len(sd) >= 14:
        try:
            return datetime.strptime(sd[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def fetch_gdelt_last24h(query: str, max_records: int) -> List[Dict]:

    # GDELT DOC 2.0 (ArtList) - 지난 24시간 뉴스
    # - OR 포함 쿼리는 반드시 ()로 감싸야 함(미감싸면 HTML 에러 페이지 반환 가능)
    # - HTML/빈 응답 방어

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

        # dt = parse_gdelt_seendate(a.get("seendate", ""))
        # # 안전망: 최근 RECENT_HOURS 밖이면 스킵
        # if not within_last_hours(dt, RECENT_HOURS):
        #     continue

        items.append({
            "id": stable_id(title, link),
            "title": title,
            "link": link,
            "summary": "",  # ArtList는 요약이 없는 경우가 많음
            "source": norm(a.get("domain", "")),
            "time": norm(a.get("seendate", "")),
        })

    return items


# -----------------------------
# RSS: headlines + snippet
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
    # published_parsed struct_time
    pp = getattr(e, "published_parsed", None)
    if pp:
        try:
            return datetime(*pp[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    # updated_parsed struct_time
    up = getattr(e, "updated_parsed", None)
    if up:
        try:
            return datetime(*up[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def fetch_rss_items(urls: List[str], max_total: int) -> List[Dict]:
    items: List[Dict] = []
    allow_undated = os.getenv("ALLOW_UNDATED_RSS", "1") == "1"

    skipped_no_title_link = 0
    skipped_old = 0
    skipped_undated = 0
    appended = 0

    debug_n = int(os.getenv("DEBUG_RSS_N", "3"))

    for url in urls:
        feed = feedparser.parse(url)

        print(f"[RSS] url={url}")
        print(f"[RSS] entries={len(feed.entries)} bozo={getattr(feed,'bozo',None)} status={getattr(feed,'status',None)}")
        if getattr(feed, "bozo", 0):
            print(f"[RSS] bozo_exception={getattr(feed,'bozo_exception',None)}")

        for idx, e in enumerate(feed.entries, 1):
            title = norm(getattr(e, "title", ""))
            link = norm(getattr(e, "link", ""))
            summary = norm(getattr(e, "summary", ""))

            if idx <= debug_n:
                print(f"[RSS][sample {idx}] title={title[:80]}")
                print(f"[RSS][sample {idx}] link={link[:120]}")
                print(f"[RSS][sample {idx}] published={getattr(e,'published',None)}")
                print(f"[RSS][sample {idx}] updated={getattr(e,'updated',None)}")

            if not title or not link:
                skipped_no_title_link += 1
                continue

            dt = get_entry_datetime(e)
            if dt is None:
                if not allow_undated:
                    skipped_undated += 1
                    continue
            else:
                if not within_last_hours(dt, RECENT_HOURS):
                    skipped_old += 1
                    continue

            items.append({
                "id": stable_id(title, link),
                "title": title,
                "link": link,
                "summary": summary,
                "source": norm(getattr(getattr(e, "source", None), "title", "")) if hasattr(e, "source") else "",
                "time": norm(getattr(e, "published", "")) or norm(getattr(e, "updated", "")),
            })
            appended += 1

            if len(items) >= max_total:
                print(f"[RSS] appended={appended} skipped_no_title_link={skipped_no_title_link} skipped_old={skipped_old} skipped_undated={skipped_undated}")
                return items

    print(f"[RSS] appended={appended} skipped_no_title_link={skipped_no_title_link} skipped_old={skipped_old} skipped_undated={skipped_undated}")
    return items


# -----------------------------
# Dedupe + rank
# -----------------------------
def dedupe_rank(items: List[Dict], top_n: int) -> List[Dict]:
    """
    - 링크 기반 중복(id) 제거
    - 유사 제목(fuzzy_key) 중복 제거
    - 키워드 점수화 + 테마 분류 후 정렬
    """
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

        it["id"] = sid
        it["score"] = score_text(it.get("title", ""), it.get("summary", ""))
        it["themes"] = classify_theme(it.get("title", ""), it.get("summary", ""))
        out.append(it)

    out.sort(key=lambda x: (x["score"], x.get("title", "")), reverse=True)
    return out[:top_n]


# -----------------------------
# Report
# -----------------------------
def build_report(items: List[Dict]) -> Tuple[str, str]:
    """
    출력:
    - 전체 Top 10
    - 테마별 Top 2
    - 키워드 히트 Top 10
    """
    now_kst = datetime.now(timezone.utc).astimezone(KST)
    subject = f"[Daily Digest] {now_kst:%Y-%m-%d %H:%M} KST"

    overall_top = items[:10]

    theme_map: Dict[str, List[Dict]] = {}
    for it in items:
        for t in it.get("themes", ["기타"]):
            theme_map.setdefault(t, []).append(it)

    theme_order = list(THEMES.keys()) + ["기타"]
    theme_top2: List[Tuple[str, List[Dict]]] = []
    for t in theme_order:
        if t in theme_map:
            theme_top2.append((t, theme_map[t][:2]))

    text_all = " ".join([(it.get("title", "") + " " + it.get("summary", "")).lower() for it in items])
    hits = Counter()
    for k in KEYWORDS.keys():
        k2 = k.lower()
        if k2 and (k2 in text_all):
            hits[k] = text_all.count(k2)
    hit_top = hits.most_common(10)

    lines: List[str] = []
    lines.append(f"📰 Daily Economic Headline Digest ({now_kst:%Y-%m-%d %H:%M} KST)")
    lines.append(f"- Window: last {RECENT_HOURS}h")
    lines.append(f"- Deduped + Scored items: {len(items)}")
    lines.append("")

    lines.append("=== ✅ Overall Top 10 ===")
    for i, it in enumerate(overall_top, 1):
        src = f" ({it['source']})" if it.get("source") else ""
        t = f" | {it['time']}" if it.get("time") else ""
        lines.append(f"{i:02d}. (score={it['score']}) {it['title']}{src}{t}")
        lines.append(f"    {it['link']}")
    lines.append("")

    lines.append("=== 📌 Theme Top 2 ===")
    for theme, its in theme_top2:
        lines.append(f"- {theme}")
        for it in its:
            src = f" ({it['source']})" if it.get("source") else ""
            t = f" | {it['time']}" if it.get("time") else ""
            lines.append(f"  • (score={it['score']}) {it['title']}{src}{t}")
            lines.append(f"    {it['link']}")
        lines.append("")

    lines.append("=== 🔎 Keyword Hits Top 10 ===")
    if hit_top:
        lines.append(", ".join([f"{k}({c})" for k, c in hit_top]))
    else:
        lines.append("(no keyword hits)")
    lines.append("")

    body = "\n".join(lines)
    return subject, body


# -----------------------------
# Delivery: Slack + Email(SMTP) (optional)
# -----------------------------
def send_slack(webhook_url: str, text: str) -> None:
    # Slack 메시지 길이 제한 고려해 분할 전송
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
    # ---- env / params ----
    gdelt_query = os.getenv(
        "GDELT_QUERY",
        "rate OR fed OR inflation OR fx OR dollar OR bond OR treasury OR yield OR nasdaq OR semiconductor OR ai OR recession OR jobs OR pmi OR china OR geopolitics OR oil"
    )

    # ✅ OR 포함이면 자동으로 괄호 감싸기 (GDELT 문법 요구)
    if " OR " in gdelt_query and not gdelt_query.strip().startswith("("):
        gdelt_query = f"({gdelt_query})"

    gdelt_max = int(os.getenv("GDELT_MAX", "50"))
    rss_max = int(os.getenv("RSS_MAX", "50"))
    use_rss = os.getenv("USE_RSS", "1") == "1"

    # Slack/Email (optional)
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_pass = os.getenv("SMTP_PASS", "").strip()
    mail_from = os.getenv("MAIL_FROM", "").strip()
    mail_to = os.getenv("MAIL_TO", "").strip()

    items: List[Dict] = []

    # ---- 1) GDELT ----
    if gdelt_max > 0:
        try:
            items += fetch_gdelt_last24h(gdelt_query, gdelt_max)
        except Exception as e:
            print(f"[WARN] GDELT fetch failed: {e}")

    # ---- 2) RSS ----
    if use_rss:
        try:
            items += fetch_rss_items(RSS_FEEDS, rss_max)
        except Exception as e:
            print(f"[WARN] RSS fetch failed: {e}")

    # ---- RAW snapshot (optional) ----
    raw_preview = int(os.getenv("RAW_PREVIEW", "0"))
    if raw_preview > 0:
        print(f"\n[RAW] collected items = {len(items)} (GDELT={'on' if gdelt_max>0 else 'off'}, RSS={'on' if use_rss else 'off'})")
        for i, it in enumerate(items[:raw_preview], 1):
            print(f"{i:02d}. {it['title']} [{it.get('source','')}]")
            print(f"    {it['link']}")
            if it.get("time"):
                print(f"    time: {it['time']}")

    # ---- rank & report ----
    ranked = dedupe_rank(items, 60)  # 브리핑용으로 넉넉히
    subject, body = build_report(ranked)

    # local output
    print(body)

    # deliver (optional)
    if slack_webhook:
        send_slack(slack_webhook, body)

    if smtp_host and smtp_user and smtp_pass and mail_from and mail_to:
        send_email_smtp(smtp_host, smtp_port, smtp_user, smtp_pass, mail_from, mail_to, subject, body)

    print("\nDone:", subject)


if __name__ == "__main__":
    main()