#!/usr/bin/env python3
"""
定时调度器模块
支持多种调度模式：interval、cron、once
"""

import time
import random
import logging
import schedule
from datetime import datetime, timedelta
from croniter import croniter
from typing import Callable, Optional
import threading
import signal
import sys


class TaskScheduler:
    def __init__(self, config: dict, task_func: Callable):
        self.config = config
        self.task_func = task_func
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.scheduler_config = config.get("scheduler", {})
        self.notification_handler = None

        # 错误处理配置
        self.error_config = config.get("settings", {}).get("error_handling", {})
        self.consecutive_failures = 0
        self.last_failure_time = None

        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def set_notification_handler(self, handler):
        """设置通知处理器"""
        self.notification_handler = handler

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"接收到信号 {signum}，正在停止调度器...")
        self.stop()

    def _execute_with_error_handling(self):
        """带错误处理的任务执行"""
        try:
            # 检查是否在冷却期
            if self._in_cooldown():
                self.logger.info("处于错误冷却期，跳过本次执行")
                return

            # 随机延迟
            self._apply_random_delay()

            # 检查时间窗口
            if not self._in_time_window():
                self.logger.info("不在允许的执行时间窗口内，跳过本次执行")
                return

            self.logger.info("开始执行定时任务")
            start_time = datetime.now()

            # 执行任务
            results = self.task_func()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # 检查执行结果
            if isinstance(results, dict):
                success_count = sum(1 for v in results.values() if v)
                total_count = len(results)
                success_rate = success_count / total_count if total_count > 0 else 0

                if success_rate >= 0.5:  # 50%以上成功认为是成功
                    self.consecutive_failures = 0
                    self.logger.info(f"定时任务执行成功，耗时 {duration:.2f} 秒")

                    # 发送成功通知
                    if self.notification_handler:
                        self.notification_handler.send_success_notification(
                            results, duration
                        )
                else:
                    self._handle_failure("任务执行成功率过低")
            else:
                # 如果返回布尔值
                if results:
                    self.consecutive_failures = 0
                    self.logger.info(f"定时任务执行成功，耗时 {duration:.2f} 秒")
                else:
                    self._handle_failure("任务执行失败")

        except Exception as e:
            self.logger.error(f"定时任务执行异常: {e}")
            self._handle_failure(str(e))

    def _handle_failure(self, error_msg: str):
        """处理失败情况"""
        self.consecutive_failures += 1
        self.last_failure_time = datetime.now()

        self.logger.error(
            f"任务失败 (连续失败 {self.consecutive_failures} 次): {error_msg}"
        )

        # 发送失败通知
        if self.notification_handler:
            self.notification_handler.send_failure_notification(
                error_msg, self.consecutive_failures
            )

        # 检查是否超过最大连续失败次数
        max_failures = self.error_config.get("max_consecutive_failures", 3)
        if self.consecutive_failures >= max_failures:
            self.logger.critical(f"连续失败次数达到上限 ({max_failures})，进入冷却期")

    def _in_cooldown(self) -> bool:
        """检查是否在冷却期"""
        if not self.last_failure_time:
            return False

        max_failures = self.error_config.get("max_consecutive_failures", 3)
        if self.consecutive_failures < max_failures:
            return False

        cooldown_minutes = self.error_config.get("failure_cooldown_minutes", 60)
        cooldown_end = self.last_failure_time + timedelta(minutes=cooldown_minutes)

        return datetime.now() < cooldown_end

    def _apply_random_delay(self):
        """应用随机延迟"""
        random_config = self.scheduler_config.get("random_delay", {})
        if not random_config.get("enabled", False):
            return

        max_minutes = random_config.get("max_minutes", 30)
        delay_seconds = random.randint(0, max_minutes * 60)

        if delay_seconds > 0:
            self.logger.info(f"随机延迟 {delay_seconds} 秒")
            time.sleep(delay_seconds)

    def _in_time_window(self) -> bool:
        """检查是否在允许的时间窗口内"""
        window_config = self.scheduler_config.get("time_window", {})
        if not window_config.get("enabled", False):
            return True

        now = datetime.now()
        start_hour = window_config.get("start_hour", 0)
        end_hour = window_config.get("end_hour", 23)

        current_hour = now.hour

        if start_hour <= end_hour:
            return start_hour <= current_hour <= end_hour
        else:  # 跨天的情况，如 22:00 到 06:00
            return current_hour >= start_hour or current_hour <= end_hour

    def start_interval_scheduler(self):
        """启动间隔调度器"""
        interval_config = self.scheduler_config.get("interval", {})
        hours = interval_config.get("hours", 1)
        minutes = interval_config.get("minutes", 0)

        total_minutes = hours * 60 + minutes

        self.logger.info(f"启动间隔调度器，每 {total_minutes} 分钟执行一次")

        schedule.every(total_minutes).minutes.do(self._execute_with_error_handling)

        # 立即执行一次
        self._execute_with_error_handling()

        self.running = True
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    def start_cron_scheduler(self):
        """启动cron调度器"""
        cron_config = self.scheduler_config.get("cron", {})
        expression = cron_config.get("expression", "0 */6 * * *")

        self.logger.info(f"启动cron调度器，表达式: {expression}")

        cron = croniter(expression, datetime.now())

        self.running = True
        while self.running:
            next_run = cron.get_next(datetime)
            now = datetime.now()

            if next_run <= now:
                self._execute_with_error_handling()
                cron = croniter(expression, datetime.now())
                continue

            # 等待到下次执行时间
            sleep_seconds = (next_run - now).total_seconds()

            # 分段睡眠，以便能够响应停止信号
            while sleep_seconds > 0 and self.running:
                sleep_time = min(60, sleep_seconds)  # 最多睡眠60秒
                time.sleep(sleep_time)
                sleep_seconds -= sleep_time
                now = datetime.now()

                # 重新检查是否到了执行时间
                if now >= next_run:
                    break

    def run_once(self):
        """执行一次任务"""
        self.logger.info("执行一次性任务")
        self._execute_with_error_handling()

    def start(self):
        """启动调度器"""
        if not self.scheduler_config.get("enabled", False):
            self.logger.info("调度器未启用")
            return

        mode = self.scheduler_config.get("mode", "interval")

        try:
            if mode == "interval":
                self.start_interval_scheduler()
            elif mode == "cron":
                self.start_cron_scheduler()
            elif mode == "once":
                self.run_once()
            else:
                self.logger.error(f"不支持的调度模式: {mode}")

        except KeyboardInterrupt:
            self.logger.info("接收到中断信号，正在停止...")
        finally:
            self.stop()

    def stop(self):
        """停止调度器"""
        self.running = False
        self.logger.info("调度器已停止")
