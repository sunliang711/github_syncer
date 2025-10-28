#!/usr/bin/env python3
"""
守护进程模块
将同步程序作为系统守护进程运行
"""

import os
import sys
import time
import atexit
import signal
import logging
from pathlib import Path


class Daemon:
    """守护进程基类"""

    def __init__(
        self, pidfile, stdin="/dev/null", stdout="/dev/null", stderr="/dev/null"
    ):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """守护进程化"""
        try:
            pid = os.fork()
            if pid > 0:
                # 退出第一个父进程
                sys.exit(0)
        except OSError as e:
            sys.stderr.write(f"fork #1 failed: {e}\n")
            sys.exit(1)

        # 从父进程环境脱离
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # 执行第二次fork
        try:
            pid = os.fork()
            if pid > 0:
                # 退出第二个父进程
                sys.exit(0)
        except OSError as e:
            sys.stderr.write(f"fork #2 failed: {e}\n")
            sys.exit(1)

        # 重定向标准文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, "r")
        so = open(self.stdout, "a+")
        se = open(self.stderr, "a+")
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # 写入pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        with open(self.pidfile, "w+") as f:
            f.write(f"{pid}\n")

    def delpid(self):
        """删除pid文件"""
        try:
            os.remove(self.pidfile)
        except:
            pass

    def start(self):
        """启动守护进程"""
        # 检查pid文件是否存在
        try:
            with open(self.pidfile, "r") as pf:
                pid = int(pf.read().strip())
        except IOError:
            pid = None

        if pid:
            message = (
                f"pidfile {self.pidfile} already exists. Daemon already running?\n"
            )
            sys.stderr.write(message)
            sys.exit(1)

        # 启动守护进程
        self.daemonize()
        self.run()

    def stop(self):
        """停止守护进程"""
        # 从pid文件获取pid
        try:
            with open(self.pidfile, "r") as pf:
                pid = int(pf.read().strip())
        except IOError:
            pid = None

        if not pid:
            message = f"pidfile {self.pidfile} does not exist. Daemon not running?\n"
            sys.stderr.write(message)
            return

        # 尝试杀死守护进程
        try:
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            if err.errno == 3:  # No such process
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(f"Error stopping daemon: {err}")
                sys.exit(1)

        print("Daemon stopped")

    def restart(self):
        """重启守护进程"""
        self.stop()
        self.start()

    def status(self):
        """检查守护进程状态"""
        try:
            with open(self.pidfile, "r") as pf:
                pid = int(pf.read().strip())
        except IOError:
            print("Daemon is not running")
            return False

        try:
            os.kill(pid, 0)  # 不发送信号，只检查进程是否存在
            print(f"Daemon is running (PID: {pid})")
            return True
        except OSError:
            print("Daemon is not running")
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return False

    def run(self):
        """子类需要重写此方法"""
        pass


class ReleaseSyncDaemon(Daemon):
    """Release同步守护进程"""

    def __init__(self, config_path, pidfile):
        super().__init__(pidfile)
        self.config_path = config_path

    def run(self):
        """运行守护进程"""
        # 导入并运行调度器
        from sync_releases import ReleaseSync
        from scheduler import TaskScheduler
        from notifications import NotificationHandler

        try:
            # 创建同步器
            syncer = ReleaseSync(self.config_path)

            # 创建通知处理器
            notification_handler = NotificationHandler(syncer.config)

            # 创建调度器
            scheduler = TaskScheduler(syncer.config, syncer.sync_all_projects)
            scheduler.set_notification_handler(notification_handler)

            # 启动调度器
            scheduler.start()

        except Exception as e:
            logging.error(f"守护进程运行异常: {e}")
            sys.exit(1)
