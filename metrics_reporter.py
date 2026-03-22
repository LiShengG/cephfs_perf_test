#!/usr/bin/env python3
"""
MDS 指标数据上报工具（模拟数据版）

用于周期性上报模拟的 MDS 性能指标数据到 Web 服务。
仅用于测试环境验证数据上报和展示功能。

使用方法:
    python metrics_reporter.py --config <config_name> --run <run_id> --interval 2
"""

from __future__ import annotations

import argparse
import random
import socket
import sys
import time
from datetime import datetime
from typing import Any

import requests


class MetricsReporter:
    """MDS 指标数据上报器（模拟数据）"""

    def __init__(
        self,
        config_name: str,
        run_id: str,
        mds_name: str | None = None,
        host: str | None = None,
        base_url: str = "http://127.0.0.1:18080",
    ):
        """
        初始化上报器

        Args:
            config_name: 配置名称
            run_id: Run ID
            mds_name: MDS 名称（可选）
            host: 主机名（可选）
            base_url: Web 服务地址
        """
        self.config_name = config_name
        self.run_id = run_id
        self.mds_name = mds_name or f"mds_{socket.gethostname()}"
        self.host = host or socket.gethostname()
        self.base_url = base_url
        self.seq = 0

    def generate_mock_data(self) -> dict[str, Any]:
        """生成模拟的 MDS 性能指标数据"""
        # 模拟 proc_stat 数据
        proc_stat = {
            "cpu_percent": round(random.uniform(10, 90), 2),
            "memory_mb": random.randint(500, 4000),
            "io_read_bytes": random.randint(10_000_000, 500_000_000),
            "io_write_bytes": random.randint(10_000_000, 500_000_000),
            "open_files": random.randint(100, 2000),
        }

        # 模拟 Ceph MDS perf_dump 数据
        perf_dump = {
            "mds": {
                "server": {
                    "handle_client_request": {
                        "avgcount": random.randint(5000, 50000),
                        "sum": random.randint(500000, 5000000),
                    },
                    "journal_latency": {
                        "avgcount": random.randint(5000, 50000),
                        "sum": round(random.uniform(0.5, 20.0), 4),
                    },
                },
                "mds_cache": {
                    "num_inodes": random.randint(10000, 200000),
                    "num_caps": random.randint(50000, 1000000),
                },
            }
        }

        return {
            "proc_stat": proc_stat,
            "perf_dump": perf_dump,
        }

    def report(self, data: dict[str, Any] | None = None) -> bool:
        """
        上报指标数据

        Args:
            data: 指标数据（如不提供则自动生成模拟数据）

        Returns:
            是否上报成功
        """
        self.seq += 1

        # 生成或使用提供的数据
        if data is None:
            data = self.generate_mock_data()

        # 构建报告
        report: dict[str, Any] = {
            "config_name": self.config_name,
            "run_id": self.run_id,
            "mds_name": self.mds_name,
            "host": self.host,
            "ts": datetime.now().isoformat(),
            "seq": self.seq,
            **data,
        }

        # 发送请求
        url = f"{self.base_url}/api/metrics/report"
        try:
            response = requests.post(url, json=report, timeout=5)
            result = response.json()
            return result.get("ok", False)
        except requests.exceptions.RequestException:
            return False


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description="MDS 指标数据上报工具（模拟数据）")
    parser.add_argument("--config", required=True, help="配置名称")
    parser.add_argument("--run", required=True, help="Run ID")
    parser.add_argument("--mds", default=None, help="MDS 名称")
    parser.add_argument("--host", default=None, help="主机名")
    parser.add_argument("--url", default="http://127.0.0.1:18080", help="Web 服务地址")
    parser.add_argument("--interval", type=int, default=0, help="上报间隔（秒），0 表示只上报一次")
    parser.add_argument("--count", type=int, default=0, help="上报次数（仅周期性模式有效），0 表示无限")

    args = parser.parse_args()

    reporter = MetricsReporter(
        config_name=args.config,
        run_id=args.run,
        mds_name=args.mds,
        host=args.host,
        base_url=args.url,
    )

    if args.interval > 0:
        # 周期性上报
        print(f"开始周期性上报 (间隔：{args.interval}秒)")
        print(f"配置：{args.config}, Run: {args.run}, MDS: {reporter.mds_name}, Host: {reporter.host}")
        print()

        total = 0
        try:
            while True:
                success = reporter.report()
                status = "✓" if success else "✗"
                print(f"{status} [{reporter.seq:03d}] 上报完成")
                total += 1
                if args.count > 0 and total >= args.count:
                    break
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print(f"\n停止上报，共上报 {reporter.seq} 次")
    else:
        # 单次上报
        success = reporter.report()
        if success:
            print("✓ 上报成功")
            return 0
        else:
            print("✗ 上报失败")
            return 1


if __name__ == "__main__":
    sys.exit(main())
