"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "RPA流程自动化中心"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 调度时区：cron 与执行窗口默认按这个时区解释。
    scheduler_timezone: str = "Asia/Shanghai"
    # 历史计划时区的处理分支：scheduler = 统一换算到调度时区；
    # plan = 按计划冻结时区逐条判断。
    schedule_window_mode: str = "scheduler"


settings = Settings()
