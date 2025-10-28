#!/usr/bin/env python3
"""
GitHub Release to Cloudflare R2 Sync Script
从指定的GitHub项目获取最新release并同步到Cloudflare R2
支持公共仓库的匿名访问
"""

import os
import sys
import yaml
import json
import logging
import requests
import boto3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from fnmatch import fnmatch
from tqdm import tqdm
import tempfile
import hashlib
from datetime import datetime
import time


class GitHubAPIClient:
    """GitHub API 客户端，支持匿名和认证访问"""

    def __init__(self, token: Optional[str] = None, config: Dict = None):
        self.session = requests.Session()
        self.token = token
        self.config = config or {}
        self.api_limits_config = self.config.get("api_limits", {}).get("github", {})
        self.logger = logging.getLogger(__name__)

        # 设置请求头
        self.session.headers.update(
            {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Release-Sync-Tool/1.0",
            }
        )

        if self.token:
            self.session.headers.update({"Authorization": f"token {self.token}"})
            self.logger.info("使用GitHub Token进行认证访问 (5000次/小时)")
        else:
            self.logger.info("使用匿名访问GitHub API (60次/小时)")

    def get_rate_limit_info(self) -> Dict:
        """获取API限制信息"""
        try:
            response = self.session.get("https://api.github.com/rate_limit", timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            self.logger.warning(f"获取API限制信息失败: {e}")
        return {}

    def check_rate_limit(self) -> bool:
        """检查API限制"""
        if not self.api_limits_config.get("respect_rate_limit", True):
            return True

        rate_limit_info = self.get_rate_limit_info()
        if not rate_limit_info:
            return True

        core_limit = rate_limit_info.get("rate", {})
        remaining = core_limit.get("remaining", 1)
        reset_time = core_limit.get("reset", 0)

        if remaining <= 5:  # 保留5次请求作为缓冲
            reset_datetime = datetime.fromtimestamp(reset_time)
            wait_seconds = max(0, reset_time - int(time.time()))

            self.logger.warning(
                f"API限制即将耗尽，剩余: {remaining}，重置时间: {reset_datetime}"
            )

            if self.api_limits_config.get("retry_on_limit", True) and wait_seconds > 0:
                if wait_seconds <= 3600:  # 最多等待1小时
                    self.logger.info(f"等待API限制重置，等待时间: {wait_seconds} 秒")
                    time.sleep(wait_seconds + 10)  # 额外等待10秒
                    return True
                else:
                    self.logger.error("API限制重置时间过长，跳过本次执行")
                    return False
            else:
                return False

        return True

    def make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """发起API请求，带重试和限制处理"""
        max_retries = self.api_limits_config.get("max_retries", 3)
        backoff_factor = self.api_limits_config.get("backoff_factor", 2)

        for attempt in range(max_retries + 1):
            # 检查API限制
            if not self.check_rate_limit():
                return None

            try:
                response = self.session.get(url, timeout=30, **kwargs)

                # 处理不同的响应状态
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    # 可能是API限制
                    if "rate limit" in response.text.lower():
                        self.logger.warning("遇到API限制，尝试等待...")
                        if attempt < max_retries:
                            wait_time = (backoff_factor**attempt) * 60
                            time.sleep(wait_time)
                            continue
                    else:
                        self.logger.error(f"API访问被禁止: {response.text}")
                        return None
                elif response.status_code == 404:
                    self.logger.error(f"资源不存在: {url}")
                    return None
                else:
                    response.raise_for_status()

            except requests.RequestException as e:
                self.logger.warning(
                    f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                )
                if attempt < max_retries:
                    wait_time = (backoff_factor**attempt) * 5
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"请求最终失败: {url}")
                    return None

        return None


class ReleaseSync:
    def __init__(self, config_path: str = "config.yaml"):
        """初始化同步器"""
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.setup_clients()

    def load_config(self, config_path: str) -> Dict:
        """加载配置文件"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"配置文件 {config_path} 不存在")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"配置文件格式错误: {e}")
            sys.exit(1)

    def setup_logging(self):
        """设置日志"""
        log_level = getattr(
            logging, self.config.get("settings", {}).get("log_level", "INFO")
        )
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("sync_releases.log", encoding="utf-8"),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def setup_clients(self):
        """设置客户端"""
        # GitHub API 客户端
        github_token = self.config.get("github", {}).get("token")
        self.github_client = GitHubAPIClient(github_token, self.config)

        # Cloudflare R2 客户端
        r2_config = self.config["cloudflare"]
        self.r2_client = boto3.client(
            "s3",
            endpoint_url=r2_config["endpoint_url"],
            aws_access_key_id=r2_config["access_key_id"],
            aws_secret_access_key=r2_config["secret_access_key"],
            region_name="auto",
        )
        self.bucket_name = r2_config["bucket_name"]

    def get_latest_release(self, owner: str, repo: str) -> Optional[Dict]:
        """获取最新release信息"""
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

        self.logger.info(f"获取 {owner}/{repo} 的最新release...")

        response = self.github_client.make_request(url)
        if response and response.status_code == 200:
            return response.json()
        else:
            self.logger.error(f"获取 {owner}/{repo} 最新release失败")
            return None

    def filter_assets(
        self, assets: List[Dict], pattern: Optional[str] = None
    ) -> List[Dict]:
        """根据模式过滤assets"""
        if not pattern:
            return assets

        filtered_assets = []
        for asset in assets:
            if fnmatch(asset["name"], pattern):
                filtered_assets.append(asset)

        self.logger.info(
            f"使用模式 '{pattern}' 过滤，匹配 {len(filtered_assets)}/{len(assets)} 个文件"
        )
        return filtered_assets

    def file_exists_in_r2(self, key: str) -> bool:
        """检查文件是否已存在于R2"""
        try:
            self.r2_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except self.r2_client.exceptions.NoSuchKey:
            return False
        except Exception as e:
            self.logger.warning(f"检查文件存在性时出错: {e}")
            return False

    def calculate_file_hash(self, file_path: str) -> str:
        """计算文件SHA256哈希"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def download_asset(self, asset: Dict, target_path: str) -> Optional[str]:
        """下载asset到临时文件"""
        download_url = asset["browser_download_url"]
        filename = asset["name"]
        file_size = asset.get("size", 0)

        self.logger.info(f"开始下载: {filename} ({self.format_size(file_size)})")

        try:
            # 使用普通的requests下载文件（不需要GitHub API）
            response = requests.get(download_url, stream=True, timeout=30)
            response.raise_for_status()

            # 创建临时文件
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}")
            temp_path = temp_file.name

            # 下载文件
            total_size = int(response.headers.get("content-length", file_size))
            chunk_size = self.config.get("settings", {}).get("chunk_size", 8192)

            with tqdm(
                total=total_size, unit="B", unit_scale=True, desc=filename
            ) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        temp_file.write(chunk)
                        pbar.update(len(chunk))

            temp_file.close()
            self.logger.info(f"下载完成: {filename}")
            return temp_path

        except Exception as e:
            self.logger.error(f"下载 {filename} 失败: {e}")
            if "temp_path" in locals():
                try:
                    os.unlink(temp_path)
                except:
                    pass
            return None

    def format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0B"
        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math

        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_names[i]}"

    def upload_to_r2(self, local_path: str, r2_key: str, metadata: Dict = None) -> bool:
        """上传文件到R2"""
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            # 计算文件大小
            file_size = os.path.getsize(local_path)

            self.logger.info(f"开始上传到R2: {r2_key} ({self.format_size(file_size)})")

            # 上传文件
            with tqdm(
                total=file_size, unit="B", unit_scale=True, desc="上传进度"
            ) as pbar:

                def upload_callback(bytes_transferred):
                    pbar.update(bytes_transferred)

                self.r2_client.upload_file(
                    local_path,
                    self.bucket_name,
                    r2_key,
                    ExtraArgs=extra_args,
                    Callback=upload_callback,
                )

            self.logger.info(f"上传完成: {r2_key}")
            return True

        except Exception as e:
            self.logger.error(f"上传到R2失败: {e}")
            return False

    def sync_project(self, project_config: Dict) -> bool:
        """同步单个项目"""
        owner = project_config["owner"]
        repo = project_config["repo"]
        asset_pattern = project_config.get("asset_pattern")
        target_path = project_config.get("target_path", f"{owner}-{repo}/")

        self.logger.info(f"开始同步项目: {owner}/{repo}")

        # 获取最新release
        release = self.get_latest_release(owner, repo)
        if not release:
            return False

        release_tag = release["tag_name"]
        release_name = release.get("name", release_tag)
        published_at = release.get("published_at", "")

        self.logger.info(
            f"找到最新release: {release_name} ({release_tag}) - 发布于 {published_at}"
        )

        # 过滤assets
        assets = self.filter_assets(release.get("assets", []), asset_pattern)
        if not assets:
            self.logger.warning(f"没有找到匹配的assets")
            if asset_pattern:
                self.logger.info(f"使用的过滤模式: {asset_pattern}")
                all_assets = release.get("assets", [])
                if all_assets:
                    self.logger.info("可用的文件:")
                    for asset in all_assets[:10]:  # 只显示前10个
                        self.logger.info(f"  - {asset['name']}")
            return True

        self.logger.info(f"找到 {len(assets)} 个匹配的文件")

        success_count = 0
        total_size = sum(asset.get("size", 0) for asset in assets)
        self.logger.info(f"总下载大小: {self.format_size(total_size)}")

        for i, asset in enumerate(assets, 1):
            filename = asset["name"]
            r2_key = f"{target_path.rstrip('/')}/{release_tag}/{filename}"

            self.logger.info(f"处理文件 {i}/{len(assets)}: {filename}")

            # 检查文件是否已存在
            if self.file_exists_in_r2(r2_key):
                self.logger.info(f"文件已存在，跳过: {r2_key}")
                success_count += 1
                continue

            # 下载文件
            temp_path = self.download_asset(asset, target_path)
            if not temp_path:
                continue

            try:
                # 准备元数据
                metadata = {
                    "project": f"{owner}/{repo}",
                    "release_tag": release_tag,
                    "release_name": release_name,
                    "asset_name": filename,
                    "download_count": str(asset.get("download_count", 0)),
                    "created_at": asset.get("created_at", ""),
                    "updated_at": asset.get("updated_at", ""),
                    "size": str(asset.get("size", 0)),
                    "sync_time": datetime.now().isoformat(),
                }

                # 上传到R2
                if self.upload_to_r2(temp_path, r2_key, metadata):
                    success_count += 1
                    self.logger.info(f"✅ 成功同步: {filename}")
                else:
                    self.logger.error(f"❌ 同步失败: {filename}")

            finally:
                # 清理临时文件
                try:
                    os.unlink(temp_path)
                except:
                    pass

        self.logger.info(
            f"项目 {owner}/{repo} 同步完成: {success_count}/{len(assets)} 成功"
        )
        return success_count == len(assets)

    def sync_all_projects(self) -> Dict[str, bool]:
        """同步所有项目"""
        projects = self.config.get("projects", [])
        if not projects:
            self.logger.warning("配置文件中没有定义项目")
            return {}

        # 显示API限制信息
        rate_limit_info = self.github_client.get_rate_limit_info()
        if rate_limit_info:
            core_limit = rate_limit_info.get("rate", {})
            remaining = core_limit.get("remaining", "unknown")
            limit = core_limit.get("limit", "unknown")
            reset_time = core_limit.get("reset", 0)
            reset_datetime = (
                datetime.fromtimestamp(reset_time) if reset_time else "unknown"
            )

            self.logger.info(
                f"GitHub API 限制: {remaining}/{limit}，重置时间: {reset_datetime}"
            )

        results = {}

        for i, project in enumerate(projects, 1):
            project_name = f"{project['owner']}/{project['repo']}"
            self.logger.info(f"\n{'=' * 60}")
            self.logger.info(f"同步项目 {i}/{len(projects)}: {project_name}")
            self.logger.info(f"{'=' * 60}")

            try:
                results[project_name] = self.sync_project(project)
            except Exception as e:
                self.logger.error(f"同步项目 {project_name} 时出现异常: {e}")
                results[project_name] = False

            # 项目间稍作延迟，避免过于频繁的API请求
            if i < len(projects):
                time.sleep(2)

        return results

    def generate_report(self, results: Dict[str, bool]):
        """生成同步报告"""
        total = len(results)
        success = sum(1 for v in results.values() if v)
        failed = total - success

        print("\n" + "=" * 60)
        print("📊 同步报告")
        print("=" * 60)
        print(f"📈 总项目数: {total}")
        print(f"✅ 成功: {success}")
        print(f"❌ 失败: {failed}")
        print(
            f"📊 成功率: {success / total * 100:.1f}%" if total > 0 else "📊 成功率: 0%"
        )
        print("-" * 60)

        for project, status in results.items():
            status_text = "✅ 成功" if status else "❌ 失败"
            print(f"{project}: {status_text}")

        print("=" * 60)

        # 显示API使用情况
        rate_limit_info = self.github_client.get_rate_limit_info()
        if rate_limit_info:
            core_limit = rate_limit_info.get("rate", {})
            used = core_limit.get("limit", 0) - core_limit.get("remaining", 0)
            limit = core_limit.get("limit", 0)
            print(f"🔗 GitHub API 使用: {used}/{limit}")
            print("=" * 60)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="GitHub Release to Cloudflare R2 同步工具"
    )
    parser.add_argument("--config", "-c", default="config.yaml", help="配置文件路径")
    parser.add_argument("--project", "-p", help="只同步指定项目 (格式: owner/repo)")
    parser.add_argument("--schedule", "-s", action="store_true", help="启动调度器")
    parser.add_argument(
        "--check-limits", action="store_true", help="检查GitHub API限制"
    )

    args = parser.parse_args()

    # 创建同步器
    syncer = ReleaseSync(args.config)

    if args.check_limits:
        # 检查API限制
        rate_limit_info = syncer.github_client.get_rate_limit_info()
        if rate_limit_info:
            core_limit = rate_limit_info.get("rate", {})
            print("GitHub API 限制信息:")
            print(f"  限制: {core_limit.get('limit', 'unknown')}")
            print(f"  剩余: {core_limit.get('remaining', 'unknown')}")
            print(f"  重置时间: {datetime.fromtimestamp(core_limit.get('reset', 0))}")
        else:
            print("无法获取API限制信息")
        return

    if args.schedule:
        # 调度器模式
        from scheduler import TaskScheduler
        from notifications import NotificationHandler

        notification_handler = NotificationHandler(syncer.config)
        scheduler = TaskScheduler(syncer.config, syncer.sync_all_projects)
        scheduler.set_notification_handler(notification_handler)
        scheduler.start()

    elif args.project:
        # 同步单个项目
        owner, repo = args.project.split("/")
        project_config = None

        for project in syncer.config.get("projects", []):
            if project["owner"] == owner and project["repo"] == repo:
                project_config = project
                break

        if not project_config:
            print(f"在配置文件中未找到项目: {args.project}")
            sys.exit(1)

        success = syncer.sync_project(project_config)
        print(f"项目 {args.project} 同步{'成功' if success else '失败'}")
    else:
        # 同步所有项目
        results = syncer.sync_all_projects()
        syncer.generate_report(results)


if __name__ == "__main__":
    main()
