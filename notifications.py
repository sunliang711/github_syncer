#!/usr/bin/env python3
"""
通知处理模块
支持邮件、Webhook、企业微信等通知方式
"""

import json
import smtplib
import requests
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, List


class NotificationHandler:
    def __init__(self, config: dict):
        self.config = config
        self.notification_config = config.get("notifications", {})
        self.logger = logging.getLogger(__name__)

    def send_success_notification(self, results: Dict[str, bool], duration: float):
        """发送成功通知"""
        if not self.notification_config.get("enabled", False):
            return

        total = len(results)
        success = sum(1 for v in results.values() if v)

        subject = f"✅ Release同步成功 - {success}/{total} 项目"
        message = self._format_success_message(results, duration)

        self._send_notifications(subject, message, "success")

    def send_failure_notification(self, error_msg: str, consecutive_failures: int):
        """发送失败通知"""
        if not self.notification_config.get("enabled", False):
            return

        subject = f"❌ Release同步失败 - 连续失败 {consecutive_failures} 次"
        message = self._format_failure_message(error_msg, consecutive_failures)

        self._send_notifications(subject, message, "error")

    def _format_success_message(self, results: Dict[str, bool], duration: float) -> str:
        """格式化成功消息"""
        total = len(results)
        success = sum(1 for v in results.values() if v)
        failed = total - success

        message = f"""
📊 **同步报告**
- 执行时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
- 总耗时: {duration:.2f} 秒
- 项目总数: {total}
- 成功: {success}
- 失败: {failed}

📋 **详细结果:**
"""

        for project, status in results.items():
            status_icon = "✅" if status else "❌"
            message += f"- {status_icon} {project}\n"

        return message

    def _format_failure_message(self, error_msg: str, consecutive_failures: int) -> str:
        """格式化失败消息"""
        return f"""
⚠️ **同步失败警报**
- 失败时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
- 连续失败次数: {consecutive_failures}
- 错误信息: {error_msg}

请检查日志文件获取详细信息。
"""

    def _send_notifications(self, subject: str, message: str, level: str):
        """发送所有类型的通知"""
        # 邮件通知
        if self.notification_config.get("email", {}).get("enabled", False):
            self._send_email(subject, message)

        # Webhook通知
        if self.notification_config.get("webhook", {}).get("enabled", False):
            self._send_webhook(subject, message, level)

        # 企业微信通知
        if self.notification_config.get("wechat_work", {}).get("enabled", False):
            self._send_wechat_work(subject, message)

    def _send_email(self, subject: str, message: str):
        """发送邮件通知"""
        try:
            email_config = self.notification_config["email"]

            msg = MIMEMultipart()
            msg["From"] = email_config["from_email"]
            msg["To"] = ", ".join(email_config["to_emails"])
            msg["Subject"] = subject

            msg.attach(MIMEText(message, "plain", "utf-8"))

            server = smtplib.SMTP(
                email_config["smtp_server"], email_config["smtp_port"]
            )
            server.starttls()
            server.login(email_config["username"], email_config["password"])

            text = msg.as_string()
            server.sendmail(email_config["from_email"], email_config["to_emails"], text)
            server.quit()

            self.logger.info("邮件通知发送成功")

        except Exception as e:
            self.logger.error(f"发送邮件通知失败: {e}")

    def _send_webhook(self, subject: str, message: str, level: str):
        """发送Webhook通知"""
        try:
            webhook_config = self.notification_config["webhook"]

            payload = {
                "subject": subject,
                "message": message,
                "level": level,
                "timestamp": datetime.now().isoformat(),
                "service": "release-sync",
            }

            headers = webhook_config.get("headers", {})
            method = webhook_config.get("method", "POST").upper()

            if method == "POST":
                response = requests.post(
                    webhook_config["url"], json=payload, headers=headers, timeout=30
                )
            else:
                response = requests.get(
                    webhook_config["url"], params=payload, headers=headers, timeout=30
                )

            response.raise_for_status()
            self.logger.info("Webhook通知发送成功")

        except Exception as e:
            self.logger.error(f"发送Webhook通知失败: {e}")

    def _send_wechat_work(self, subject: str, message: str):
        """发送企业微信通知"""
        try:
            wechat_config = self.notification_config["wechat_work"]

            payload = {
                "msgtype": "markdown",
                "markdown": {"content": f"## {subject}\n\n{message}"},
            }

            response = requests.post(
                wechat_config["webhook_url"], json=payload, timeout=30
            )

            response.raise_for_status()
            result = response.json()

            if result.get("errcode") == 0:
                self.logger.info("企业微信通知发送成功")
            else:
                self.logger.error(f"企业微信通知发送失败: {result}")

        except Exception as e:
            self.logger.error(f"发送企业微信通知失败: {e}")
