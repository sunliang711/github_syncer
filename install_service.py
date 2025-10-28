#!/usr/bin/env python3
"""
服务安装脚本
用于安装systemd服务或生成cron任务
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path


def install_systemd_service():
    """安装systemd服务"""
    current_dir = Path(__file__).parent.absolute()

    # 服务文件内容
    service_content = f"""[Unit]
Description=GitHub Release to Cloudflare R2 Sync Service
After=network.target

[Service]
Type=forking
User=root
WorkingDirectory={current_dir}
ExecStart={sys.executable} {current_dir}/daemon.py start
ExecStop={sys.executable} {current_dir}/daemon.py stop
ExecReload={sys.executable} {current_dir}/daemon.py restart
PIDFile=/tmp/release-sync.pid
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"""

    # 写入服务文件
    service_file = "/etc/systemd/system/release-sync.service"
    try:
        with open(service_file, "w") as f:
            f.write(service_content)

        # 重新加载systemd
        subprocess.run(["systemctl", "daemon-reload"], check=True)

        print(f"✅ systemd服务已安装: {service_file}")
        print("使用以下命令管理服务:")
        print("  启动服务: sudo systemctl start release-sync")
        print("  停止服务: sudo systemctl stop release-sync")
        print("  开机自启: sudo systemctl enable release-sync")
        print("  查看状态: sudo systemctl status release-sync")
        print("  查看日志: sudo journalctl -u release-sync -f")

    except PermissionError:
        print("❌ 需要root权限来安装systemd服务")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 安装systemd服务失败: {e}")
        sys.exit(1)


def generate_cron_job():
    """生成cron任务"""
    current_dir = Path(__file__).parent.absolute()

    cron_content = f"""# GitHub Release Sync Cron Job
# 每6小时执行一次
0 */6 * * * cd {current_dir} && {sys.executable} sync_releases.py >> /var/log/release-sync-cron.log 2>&1

# 或者使用调度器模式（推荐）
# 0 2 * * * cd {current_dir} && {sys.executable} -c "from scheduler import TaskScheduler; from sync_releases import ReleaseSync; import yaml; config=yaml.safe_load(open('config.yaml')); syncer=ReleaseSync('config.yaml'); scheduler=TaskScheduler(config, syncer.sync_all_projects); scheduler.run_once()"
"""

    cron_file = current_dir / "cron" / "crontab.example"
    cron_file.parent.mkdir(exist_ok=True)

    with open(cron_file, "w") as f:
        f.write(cron_content)

    print(f"✅ Cron任务示例已生成: {cron_file}")
    print("使用以下命令安装cron任务:")
    print(f"  crontab -e")
    print(f"然后添加以下行:")
    print(f"  0 */6 * * * cd {current_dir} && {sys.executable} sync_releases.py")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="安装Release同步服务")
    parser.add_argument(
        "--type", "-t", choices=["systemd", "cron"], default="systemd", help="服务类型"
    )

    args = parser.parse_args()

    if args.type == "systemd":
        install_systemd_service()
    elif args.type == "cron":
        generate_cron_job()


if __name__ == "__main__":
    main()
