"""调度窗口判定的共用处理路径。

调度计划是否到期，必须同时满足两个条件：

1. 当前调度时点已经越过计划 cron 表达式的某个触发点；
2. 当前调度时点仍落在该计划「执行窗口」允许的补触发范围内。

本模块只负责“判定”和“生成触发凭证”，不发布消息、不写任务队列。
调度器（scheduler）和任务队列（task_queue）都调用同一套函数，
避免两个模块各写一遍窗口判定，出现“调度列表和队列窗口错位”。

时区处理有两个显式分支：

* ``mode="scheduler"``：所有计划统一换算到当前调度时区后判断；
* ``mode="plan"``：按计划冻结的时区逐条判断（计划没有时区时回落到调度时区）。

两个分支都只产出真正到期的计划，不会因为时区不一致而把全部计划放行。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone, tzinfo
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_WINDOW_SECONDS = 60
DEFAULT_LOOKBACK_DAYS = 366 * 4

_MONTH_NAMES = {
    name: index
    for index, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
         "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"),
        start=1,
    )
}
_DOW_NAMES = {
    name: index
    for index, name in enumerate(
        ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"),
        start=0,
    )
}


class ScheduleWindowError(ValueError):
    """调度窗口配置不合法。"""


# ---------------------------------------------------------------------------
# cron 解析
# ---------------------------------------------------------------------------

def _parse_value(token: str, low: int, high: int,
                 names: dict[str, int] | None = None) -> int:
    text = token.strip()
    if names and text.upper() in names:
        return names[text.upper()]
    if not text.isdigit():
        raise ScheduleWindowError(f"无法解析 cron 字段：{token!r}")
    value = int(text)
    if value < low or value > high:
        raise ScheduleWindowError(
            f"cron 字段 {token!r} 超出范围 {low}-{high}")
    return value


def _expand_field(token: str, low: int, high: int,
                  names: dict[str, int] | None = None,
                  *, sunday_as_seven: bool = False) -> tuple[frozenset[int], bool]:
    """把单个 cron 字段展开成取值集合。

    返回 ``(取值集合, 是否受限)``。``*`` / ``?`` 表示不受限。
    """
    text = token.strip()
    if not text:
        raise ScheduleWindowError("cron 字段为空")
    if text in ("*", "?"):
        return frozenset(range(low, high + 1)), False

    values: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ScheduleWindowError(f"cron 字段存在空项：{token!r}")
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) <= 0:
                raise ScheduleWindowError(f"cron 步长非法：{part!r}")
            step = int(step_text)
        else:
            base = part

        base = base.strip()
        if base in ("*", "?"):
            sequence = list(range(low, high + 1))
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = _parse_value(start_text, low, high, names)
            end = _parse_value(end_text, low, high, names)
            if start > end:
                raise ScheduleWindowError(f"cron 范围非法：{part!r}")
            sequence = list(range(start, end + 1))
        else:
            value = _parse_value(base, low, high, names)
            sequence = [value]

        values.update(sequence[::step])

    if sunday_as_seven:
        values = {0 if value == 7 else value for value in values}
    return frozenset(values), True


def _parse_seconds(token: str) -> tuple[frozenset[int], bool]:
    return _expand_field(token, 0, 59)


def _parse_month(token: str) -> tuple[frozenset[int], bool]:
    return _expand_field(token, 1, 12, _MONTH_NAMES)


def _parse_dom(token: str) -> tuple[frozenset[int], bool]:
    return _expand_field(token, 1, 31)


def _parse_dow(token: str) -> tuple[frozenset[int], bool]:
    return _expand_field(token, 0, 7, _DOW_NAMES, sunday_as_seven=True)


_CRON_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
    "@every_minute": "* * * * *",
}


@dataclass(frozen=True)
class CronSpec:
    """标准 5 段 cron（可选 6 段带秒），支持 ``*`` / ``?`` / 列表 / 范围 / 步长。"""

    expression: str
    seconds: frozenset[int]
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    dom_restricted: bool = False
    dow_restricted: bool = False
    has_seconds: bool = False
    second_restricted: bool = False

    @classmethod
    def parse(cls, expression: str) -> "CronSpec":
        raw = (expression or "").strip()
        if not raw:
            raise ScheduleWindowError("cron 表达式为空")
        lowered = raw.lower()
        if lowered in _CRON_ALIASES:
            raw = _CRON_ALIASES[lowered]
            lowered = raw.lower()
        elif lowered.startswith("@every "):
            raw = f"*/{lowered.split(' ', 1)[1].strip().rstrip('sm')} * * * *"
            lowered = raw.lower()

        parts = raw.split()
        has_seconds = False
        second_restricted = False
        if len(parts) == 5:
            seconds = frozenset({0})
            minutes_text, hours_text, dom_text, month_text, dow_text = parts
        elif len(parts) == 6:
            has_seconds = True
            seconds, second_restricted = _parse_seconds(parts[0])
            minutes_text, hours_text, dom_text, month_text, dow_text = parts[1:]
        else:
            raise ScheduleWindowError(
                "cron 表达式必须是 5 段（分 时 日 月 周）或 6 段（秒 分 时 日 月 周）")

        minutes, _ = _expand_field(minutes_text, 0, 59)
        hours, _ = _expand_field(hours_text, 0, 23)
        days_of_month, dom_restricted = _parse_dom(dom_text)
        months, _ = _parse_month(month_text)
        days_of_week, dow_restricted = _parse_dow(dow_text)

        return cls(
            expression=raw,
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            days_of_month=days_of_month,
            months=months,
            days_of_week=days_of_week,
            dom_restricted=dom_restricted,
            dow_restricted=dow_restricted,
            has_seconds=has_seconds,
            second_restricted=second_restricted,
        )

    def _day_matches(self, value: date) -> bool:
        if value.month not in self.months:
            return False
        dom_match = value.day in self.days_of_month
        # Python: Monday=0 ... Sunday=6；cron: Sunday=0 ... Saturday=6
        cron_dow = (value.weekday() + 1) % 7
        dow_match = cron_dow in self.days_of_week
        if self.dom_restricted and self.dow_restricted:
            return dom_match or dow_match
        return dom_match and dow_match

    def matches(self, value: datetime) -> bool:
        if not self._day_matches(value.date()):
            return False
        if value.hour not in self.hours:
            return False
        if value.minute not in self.minutes:
            return False
        if self.has_seconds and self.second_restricted:
            return value.second in self.seconds
        return True

    def previous_fire(self, at: datetime,
                      lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> datetime | None:
        """返回 ``at``（含）之前最近一次 cron 触发时点。"""
        if at.tzinfo is None:
            raise ScheduleWindowError("cron 计算需要带时区的调度时点")

        candidate = at.replace(second=0, microsecond=0)
        if candidate > at:
            candidate -= timedelta(minutes=1)
        # 5 段 cron 的触发秒固定为 0；如果 at 落在 10:00:45，
        # 最近触发点仍是 10:00:00。
        limit = candidate - timedelta(days=max(1, lookback_days))

        while candidate >= limit:
            if not self._day_matches(candidate.date()):
                candidate = (candidate - timedelta(days=1)).replace(
                    hour=23, minute=59, second=0, microsecond=0)
                continue
            if candidate.hour not in self.hours:
                candidate = candidate.replace(
                    minute=59, second=0, microsecond=0) - timedelta(hours=1)
                continue
            if candidate.minute not in self.minutes:
                candidate -= timedelta(minutes=1)
                continue

            if not self.has_seconds or not self.second_restricted:
                return candidate
            if candidate == at.replace(second=0, microsecond=0):
                # 同一个分钟里，取不晚于 at 的最大合法秒。
                valid_seconds = [
                    second for second in sorted(self.seconds)
                    if second <= at.second
                ]
            else:
                # 更早的分钟取该分钟最后一个合法秒。
                valid_seconds = sorted(self.seconds)
            if not valid_seconds:
                candidate -= timedelta(minutes=1)
                continue
            return candidate.replace(second=valid_seconds[-1], microsecond=0)
        return None

    def next_fire(self, at: datetime,
                  lookahead_days: int = DEFAULT_LOOKBACK_DAYS) -> datetime | None:
        """返回 ``at`` 之后最近一次 cron 触发时点，供工作台提示等待。"""
        if at.tzinfo is None:
            raise ScheduleWindowError("cron 计算需要带时区的调度时点")
        candidate = at.replace(second=0, microsecond=0)
        limit = candidate + timedelta(days=max(1, lookahead_days))

        while candidate <= limit:
            if not self._day_matches(candidate.date()):
                candidate = (candidate + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0)
                continue
            if candidate.hour not in self.hours:
                candidate = (candidate + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0)
                continue
            if candidate.minute not in self.minutes:
                candidate += timedelta(minutes=1)
                continue
            if self.has_seconds and self.second_restricted:
                # 当前分钟里取严格晚于 at 的最小合法秒；
                # 后续分钟取该分钟第一个合法秒。
                for second in sorted(self.seconds):
                    moment = candidate.replace(second=second)
                    if moment > at:
                        return moment
                candidate += timedelta(minutes=1)
                continue
            moment = candidate
            if moment <= at:
                candidate += timedelta(minutes=1)
                continue
            return moment
        return None


def parse_cron(expression: str) -> CronSpec:
    return CronSpec.parse(expression)


# ---------------------------------------------------------------------------
# 时区 / 时间参数
# ---------------------------------------------------------------------------

def normalize_mode(mode: str | None, default: str = "scheduler") -> str:
    text = (mode or default or "scheduler").strip().lower()
    if text in {"scheduler", "dispatch", "unified", "current", "scheduler_tz",
                "调度", "调度时区", "统一时区"}:
        return "scheduler"
    if text in {"plan", "frozen", "plan_tz", "plan_frozen", "计划", "计划时区",
                "冻结时区"}:
        return "plan"
    raise ScheduleWindowError(
        f"未知时区处理模式：{mode!r}（可选 scheduler / plan）")


def resolve_timezone(name: str | None, *, fallback: str = "Asia/Shanghai") -> tzinfo:
    text = (name or fallback or "Asia/Shanghai").strip()
    if not text:
        text = fallback or "Asia/Shanghai"
    lowered = text.lower()
    if lowered in {"utc", "gmt", "z"}:
        return timezone.utc
    if lowered in {"local", "system"}:
        return datetime.now().astimezone().tzinfo or timezone.utc

    offset_match = re.fullmatch(
        r"(?:utc|gmt)?([+-])(\d{1,2})(?::?(\d{2}))?", lowered)
    if offset_match:
        sign = 1 if offset_match.group(1) == "+" else -1
        hours = int(offset_match.group(2))
        minutes = int(offset_match.group(3) or 0)
        return timezone(sign * timedelta(hours=hours, minutes=minutes))

    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ScheduleWindowError(f"未知时区：{text}") from exc


def timezone_name(value: tzinfo) -> str:
    key = getattr(value, "key", None)
    if key:
        return str(key)
    return value.tzname(None) or str(value)


def parse_at(value: str | datetime | None, default_tz: tzinfo) -> datetime:
    """把 API 传入的调度时点解析成带时区 datetime。"""
    if value is None:
        return datetime.now(timezone.utc).astimezone(default_tz)
    if isinstance(value, datetime):
        moment = value
    else:
        text = str(value).strip()
        if not text:
            return datetime.now(timezone.utc).astimezone(default_tz)
        normalized = text.replace("Z", "+00:00")
        try:
            moment = datetime.fromisoformat(normalized)
        except ValueError:
            for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    moment = datetime.strptime(text, pattern)
                    break
                except ValueError:
                    continue
            else:
                raise ScheduleWindowError(f"无法解析调度时点：{value!r}")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=default_tz)
    return moment


def _plan_timezone(plan: dict[str, Any], fallback: tzinfo) -> tuple[tzinfo, str, bool]:
    raw = None
    for key in ("时区", "计划时区", "timezone", "tz", "Timezone"):
        candidate = plan.get(key)
        if candidate is not None and str(candidate).strip():
            raw = str(candidate).strip()
            break
    if raw is None:
        return fallback, timezone_name(fallback), False
    try:
        return resolve_timezone(raw), raw, True
    except ScheduleWindowError:
        # 计划时区写错时回落到调度时区，但仍然逐条判断，不把计划一起放行。
        return fallback, timezone_name(fallback), False


# ---------------------------------------------------------------------------
# 执行窗口解析
# ---------------------------------------------------------------------------

_DURATION_PATTERN = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*"
    r"(?P<unit>s|sec|secs|second|seconds|秒|"
    r"m|min|mins|minute|minutes|分|分钟|"
    r"h|hr|hrs|hour|hours|小时|"
    r"d|day|days|天)"
)


def parse_duration_seconds(text: Any) -> float | None:
    """解析执行窗口的时长写法，返回秒数。

    支持 ``30s`` / ``5m`` / ``2h`` / ``90``（按分钟） / ``00:05`` （5 分钟）
    以及 ISO 8601 的 ``PT5M``。
    """
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    lowered = raw.lower().replace(" ", "")

    iso_match = re.fullmatch(
        r"pt(?:(?P<h>\d+(?:\.\d+)?)h)?(?:(?P<m>\d+(?:\.\d+)?)m)?"
        r"(?:(?P<s>\d+(?:\.\d+)?)s)?", lowered)
    if iso_match and any(iso_match.groupdict().values()):
        return (
            float(iso_match.group("h") or 0) * 3600
            + float(iso_match.group("m") or 0) * 60
            + float(iso_match.group("s") or 0)
        )

    if re.fullmatch(r"\d+(?:\.\d+)?", lowered):
        # 纯数字沿用调度台习惯：按分钟解释。
        return float(lowered) * 60

    clock_match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", lowered)
    if clock_match:
        hours = int(clock_match.group(1))
        minutes = int(clock_match.group(2))
        seconds = int(clock_match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds

    match = _DURATION_PATTERN.fullmatch(lowered)
    if match:
        value = float(match.group("value"))
        unit = match.group("unit")
        if unit in {"s", "sec", "secs", "second", "seconds", "秒"}:
            return value
        if unit in {"h", "hr", "hrs", "hour", "hours", "小时"}:
            return value * 3600
        if unit in {"d", "day", "days", "天"}:
            return value * 86400
        return value * 60
    return None


def _parse_clock(text: str) -> dtime:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", text.strip())
    if not match:
        raise ScheduleWindowError(f"无法解析时间：{text!r}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3) or 0)
    if hour > 23 or minute > 59 or second > 59:
        raise ScheduleWindowError(f"时间超出范围：{text!r}")
    return dtime(hour, minute, second)


_TIME_BAND_PATTERN = re.compile(
    r"^\s*(\d{1,2}:\d{2}(?::\d{2})?)\s*(?:-|~|～|至|到)\s*"
    r"(\d{1,2}:\d{2}(?::\d{2})?)\s*$"
)
_DATETIME_BAND_PATTERN = re.compile(
    r"^\s*(\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{2}(?::\d{2})?)\s*"
    r"(?:-|~|～|至|到)\s*"
    r"(\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{2}(?::\d{2})?)\s*$"
)


def parse_time_band(text: Any) -> tuple[dtime, dtime] | None:
    """解析执行窗口的每日时段写法，如 ``09:00-18:00``。"""
    if text is None:
        return None
    raw = str(text).strip()
    match = _TIME_BAND_PATTERN.fullmatch(raw)
    if not match:
        return None
    return _parse_clock(match.group(1)), _parse_clock(match.group(2))


def parse_datetime_band(text: Any, tz: tzinfo) -> tuple[datetime, datetime] | None:
    """解析执行窗口的绝对时间范围写法。"""
    if text is None:
        return None
    raw = str(text).strip()
    match = _DATETIME_BAND_PATTERN.fullmatch(raw)
    if not match:
        return None
    start = parse_at(match.group(1).replace("T", " "), tz)
    end = parse_at(match.group(2).replace("T", " "), tz)
    if start.tzinfo is None:
        start = start.replace(tzinfo=tz)
    if end.tzinfo is None:
        end = end.replace(tzinfo=tz)
    return start, end


def _band_for_day(day: date, band: tuple[dtime, dtime],
                  tz: tzinfo) -> tuple[datetime, datetime]:
    start_clock, end_clock = band
    start = datetime.combine(day, start_clock, tzinfo=tz)
    end = datetime.combine(day, end_clock, tzinfo=tz)
    if end <= start:
        end += timedelta(days=1)
    return start, end


# ---------------------------------------------------------------------------
# 到期判定
# ---------------------------------------------------------------------------

def make_credential(plan_id: Any, occurrence: datetime) -> str:
    """生成窗口触发凭证：同一计划 + 同一 cron 窗口只对应一个凭证。"""
    if occurrence.tzinfo is None:
        raise ScheduleWindowError("触发凭证需要带时区的 cron 窗口")
    stamp = occurrence.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"PLAN-{plan_id}-{stamp}"


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def evaluate_plan(
    plan: dict[str, Any],
    *,
    at: datetime | None = None,
    scheduler_timezone: str | None = None,
    mode: str | None = None,
    default_window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> dict[str, Any]:
    """判定单个计划在当前调度时点是否到期。

    返回值同时包含判定结论和窗口凭证，调度器与任务队列共用。
    """
    normalized_mode = normalize_mode(mode)
    scheduler_tz = resolve_timezone(scheduler_timezone)
    scheduler_tz_name = timezone_name(scheduler_tz)
    plan_tz, plan_tz_name, plan_tz_frozen = _plan_timezone(plan, scheduler_tz)
    evaluation_tz = scheduler_tz if normalized_mode == "scheduler" else plan_tz
    evaluation_tz_name = timezone_name(evaluation_tz)

    moment = parse_at(at, scheduler_tz).astimezone(evaluation_tz)
    cron_text = str(plan.get("cron表达式") or plan.get("cron") or "").strip()
    window_text = (
        plan.get("执行窗口")
        if plan.get("执行窗口") is not None
        else plan.get("window")
    )

    base: dict[str, Any] = {
        "plan_id": plan.get("id"),
        "plan_name": plan.get("计划名称") or plan.get("name"),
        "bot_ref": plan.get("绑定流程"),
        "cron": cron_text,
        "window": window_text,
        "priority": plan.get("优先级"),
        "timezone": evaluation_tz_name,
        "evaluation_timezone": evaluation_tz_name,
        "scheduler_timezone": scheduler_tz_name,
        "plan_timezone": plan_tz_name,
        "plan_timezone_frozen": plan_tz_frozen,
        "mode": normalized_mode,
        "evaluated_at": moment.isoformat(),
        "due": False,
        "state": "未到期",
        "reason": "",
        "occurrence": None,
        "window_start": None,
        "window_end": None,
        "window_kind": "cron",
        "credential": None,
        "token": None,
        "next_occurrence": None,
    }

    if not cron_text:
        base["reason"] = "计划未配置 cron 表达式"
        return base
    try:
        spec = parse_cron(cron_text)
    except ScheduleWindowError as exc:
        base["reason"] = f"cron 表达式无效：{exc}"
        return base

    occurrence = spec.previous_fire(moment)
    if occurrence is None:
        base["reason"] = "回溯范围内没有 cron 触发点"
        return base

    base["occurrence"] = _iso(occurrence)
    base["next_occurrence"] = _iso(spec.next_fire(moment))

    def mark_due() -> dict[str, Any]:
        """只有判定通过才发出触发凭证，未到期计划不携带凭证。"""
        base["due"] = True
        base["state"] = "到期"
        base["credential"] = make_credential(plan.get("id"), occurrence)
        base["token"] = base["credential"]
        return base

    duration = parse_duration_seconds(window_text)
    time_band = parse_time_band(window_text)
    datetime_band = parse_datetime_band(window_text, evaluation_tz)

    if datetime_band is not None:
        band_start, band_end = datetime_band
        base["window_kind"] = "datetime"
        base["window_start"] = _iso(band_start)
        base["window_end"] = _iso(band_end)
        if moment < band_start:
            base["reason"] = "未到执行窗口"
            return base
        if moment > band_end:
            base["reason"] = "已过执行窗口"
            return base
        if occurrence > moment:
            base["reason"] = "cron 时点尚未到达"
            return base
        base["reason"] = "调度时点与执行窗口均满足"
        return mark_due()

    if time_band is not None:
        start_clock, end_clock = time_band
        containing: tuple[datetime, datetime] | None = None
        today = moment.date()
        for offset in (-1, 0, 1):
            band_start, band_end = _band_for_day(
                today + timedelta(days=offset), time_band, evaluation_tz)
            if band_start <= moment <= band_end:
                containing = (band_start, band_end)
                break
        base["window_kind"] = "daily"
        if containing is None:
            today_start, today_end = _band_for_day(today, time_band, evaluation_tz)
            base["window_start"] = _iso(today_start)
            base["window_end"] = _iso(today_end)
            base["reason"] = "未到执行窗口" if moment < today_start else "已过执行窗口"
            return base

        band_start, band_end = containing
        base["window_start"] = _iso(band_start)
        base["window_end"] = _iso(band_end)
        # 允许 cron 在窗口内稍早触发；跨天窗口也允许窗口起点后的触发点。
        # 但隔天、隔月的陈旧触发点不算本次窗口，避免“补触发”无限回溯。
        same_day = occurrence.astimezone(evaluation_tz).date() == band_start.date()
        if occurrence > moment or (occurrence < band_start and not same_day):
            base["reason"] = "cron 时点不在本次执行窗口内"
            return base
        base["reason"] = "调度时点与执行窗口均满足"
        return mark_due()

    # 没有可解析的显式窗口时，使用 cron 触发点后的短补触发窗口。
    seconds = duration if duration is not None else default_window_seconds
    window_start = occurrence
    window_end = occurrence + timedelta(seconds=max(0, seconds))
    base["window_kind"] = "duration" if duration is not None else "default"
    base["window_start"] = _iso(window_start)
    base["window_end"] = _iso(window_end)
    if moment < window_start:
        base["reason"] = "未到 cron 触发时点"
        return base
    if moment > window_end:
        base["reason"] = "已超过执行窗口"
        return base
    base["reason"] = (
        "调度时点与执行窗口均满足"
        if duration is not None
        else "调度时点命中 cron 窗口（执行窗口未配置，按默认窗口判定）"
    )
    return mark_due()


def evaluate_plans(
    plans: Iterable[dict[str, Any]],
    *,
    at: datetime | None = None,
    scheduler_timezone: str | None = None,
    mode: str | None = None,
    default_window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> list[dict[str, Any]]:
    """批量判定，只返回判定结果，不做任何状态写入。"""
    return [
        evaluate_plan(
            plan,
            at=at,
            scheduler_timezone=scheduler_timezone,
            mode=mode,
            default_window_seconds=default_window_seconds,
        )
        for plan in plans
    ]


def active_plans(plans: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [plan for plan in plans if plan.get("status") == "已激活"]


def due_plans(
    plans: Iterable[dict[str, Any]],
    *,
    at: datetime | None = None,
    scheduler_timezone: str | None = None,
    mode: str | None = None,
) -> list[dict[str, Any]]:
    return [
        decision
        for decision in evaluate_plans(
            active_plans(plans),
            at=at,
            scheduler_timezone=scheduler_timezone,
            mode=mode,
        )
        if decision["due"]
    ]


def validate_credential(
    plan: dict[str, Any] | None,
    occurrence: str | datetime | None,
    token: str | None,
    *,
    scheduler_timezone: str | None = None,
    mode: str | None = None,
) -> tuple[bool, str]:
    """校验任务队列收到的触发凭证是否仍对应一个真实到期窗口。

    任务队列不重新“猜”窗口，直接复用同一套判定：
    用消息里的计划、时区和 cron 窗口重新算一次凭证，再比对。
    """
    if plan is None:
        return False, "计划不存在"
    if not token:
        return False, "缺少触发凭证"
    if occurrence is None:
        return False, "缺少窗口标识"
    try:
        occurrence_dt = parse_at(
            occurrence,
            resolve_timezone(scheduler_timezone or plan.get("时区")),
        )
        expected = make_credential(plan.get("id"), occurrence_dt)
    except ScheduleWindowError as exc:
        return False, f"窗口标识无效：{exc}"
    if expected != token:
        return False, "触发凭证与计划窗口不匹配"
    if plan.get("status") != "已激活":
        return False, f"计划当前状态为{plan.get('status')}，不接收调度任务"
    return True, "凭证有效"
