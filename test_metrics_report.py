#!/usr/bin/env python3
"""
测试指标数据上报功能

该脚本用于测试项目是否能正常接收和展示上报的指标数据。
它会：
1. 创建一个测试配置和 run 记录
2. 周期性发送 HTTP 请求上报指标数据
3. 验证数据是否能被正确存储和查询
"""

from __future__ import annotations

import json
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# 项目根目录
REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from server.utils.paths import dump_json, read_app_config
from server.services.config_store import ConfigStore
from server.services.run_store import RunStore


# 配置
BASE_URL = "http://127.0.0.1:18080"
TEST_CONFIG_NAME = "test_mdtest_config"
REPORT_INTERVAL_SECONDS = 2  # 上报间隔（秒）
REPORT_COUNT = 10  # 上报次数


def create_test_config() -> dict[str, Any]:
    """创建测试配置"""
    config = {
        "config_name": TEST_CONFIG_NAME,
        "description": "Test config for metrics report testing",
        "mds_count": 2,
        "client_count": 4,
        "test_duration": 60,
        "test_type": "mdtest",
        "filesystem": "cephfs",
        "test_dir": "/mnt/cephfs/test_mdtest",
        "mpi_hosts": ["host1", "host2", "host3", "host4"],
    }
    return config


def setup_test_run() -> tuple[str, str]:
    """设置测试 run 记录"""
    print("=" * 60)
    print("步骤 1: 创建测试配置和 Run 记录")
    print("=" * 60)
    
    config_store = ConfigStore()
    run_store = RunStore()
    
    # 检查配置是否存在，不存在则创建
    try:
        config = config_store.get_config(TEST_CONFIG_NAME)
        print(f"配置已存在：{TEST_CONFIG_NAME}")
    except FileNotFoundError:
        # 创建新配置
        config = create_test_config()
        config_store.create_config(TEST_CONFIG_NAME, config)
        print(f"创建新配置：{TEST_CONFIG_NAME}")
    
    # 检查是否有正在运行的实验
    current = run_store.current_run()
    if current:
        print(f"已有运行中的实验：{current['run_id']}")
        return current["config_name"], current["run_id"]
    
    # 创建新的 run 记录（不实际执行脚本，只创建目录结构）
    app_config = read_app_config()
    runs_dir = (REPO_ROOT / app_config["runs_dir"]).resolve()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{TEST_CONFIG_NAME}_{timestamp}"
    run_dir = runs_dir / TEST_CONFIG_NAME / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建 run_meta.json
    meta = {
        "run_id": run_id,
        "run_name": run_id,
        "config_name": TEST_CONFIG_NAME,
        "status": "running",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "ended_at": None,
        "pid": None,
        "return_code": None,
    }
    dump_json(run_dir / "run_meta.json", meta)
    
    print(f"创建 Run 记录：{run_id}")
    print(f"Run 目录：{run_dir}")
    
    return TEST_CONFIG_NAME, run_id


def generate_mock_metrics(config_name: str, run_id: str, seq: int) -> dict[str, Any]:
    """生成模拟的指标数据"""
    mds_list = ["mds0", "mds1"]
    hosts = ["host1", "host2", "host3", "host4"]
    
    mds = random.choice(mds_list)
    host = random.choice(hosts)
    
    # 模拟 proc_stat 数据
    proc_stat = {
        "cpu_percent": round(random.uniform(10, 90), 2),
        "memory_mb": random.randint(100, 2000),
        "io_read_bytes": random.randint(1000000, 100000000),
        "io_write_bytes": random.randint(1000000, 100000000),
        "open_files": random.randint(100, 1000),
    }
    
    # 模拟 perf_dump 数据（Ceph MDS 性能数据）
    perf_dump = {
        "mds": {
            "server": {
                "handle_client_request": {
                    "avgcount": random.randint(1000, 10000),
                    "sum": random.randint(100000, 1000000),
                },
                "journal_latency": {
                    "avgcount": random.randint(1000, 10000),
                    "sum": round(random.uniform(0.1, 10.0), 4),
                },
            },
            "mds_cache": {
                "num_inodes": random.randint(10000, 100000),
                "num_caps": random.randint(50000, 500000),
            },
        }
    }
    
    report = {
        "config_name": config_name,
        "run_id": run_id,
        "mds_name": mds,
        "host": host,
        "ts": datetime.now().isoformat(),
        "seq": seq,
        "proc_stat": proc_stat,
        "perf_dump": perf_dump,
    }
    
    return report


def report_metrics(config_name: str, run_id: str, seq: int) -> bool:
    """上报指标数据"""
    report = generate_mock_metrics(config_name, run_id, seq)
    
    url = f"{BASE_URL}/api/metrics/report"
    try:
        response = requests.post(url, json=report, timeout=5)
        result = response.json()
        
        if result.get("ok"):
            print(f"  ✓ [{seq:02d}] 上报成功 - MDS: {report['mds_name']}, "
                  f"Host: {report['host']}, CPU: {report['proc_stat']['cpu_percent']}%")
            return True
        else:
            print(f"  ✗ [{seq:02d}] 上报失败：{result.get('error')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  ✗ [{seq:02d}] 请求异常：{e}")
        return False


def verify_metrics(config_name: str, run_id: str) -> bool:
    """验证指标数据"""
    print("\n" + "=" * 60)
    print("步骤 3: 验证数据查询")
    print("=" * 60)
    
    # 查询所有报告
    url = f"{BASE_URL}/api/configs/{config_name}/runs/{run_id}/metrics"
    try:
        response = requests.get(url, timeout=5)
        result = response.json()
        
        if result.get("ok"):
            reports = result.get("reports", [])
            print(f"查询到 {len(reports)} 条报告记录")
            
            if reports:
                # 按 MDS 分组统计
                mds_count: dict[str, int] = {}
                for r in reports:
                    mds = r.get("mds_name", "unknown")
                    mds_count[mds] = mds_count.get(mds, 0) + 1
                
                print("按 MDS 分组:")
                for mds, count in sorted(mds_count.items()):
                    print(f"  - {mds}: {count} 条")
                
                return True
            else:
                print("未找到报告记录")
                return False
        else:
            print(f"查询失败：{result.get('error')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"查询异常：{e}")
        return False


def verify_metric_series(config_name: str, run_id: str, metric_key: str) -> bool:
    """验证指标序列查询"""
    url = f"{BASE_URL}/api/configs/{config_name}/runs/{run_id}/metrics/series?metric={metric_key}"
    try:
        response = requests.get(url, timeout=5)
        result = response.json()
        
        if result.get("ok"):
            data = result.get("data", {})
            series = data.get("series", {})
            print(f"\n指标 '{metric_key}' 的序列数据:")
            for mds_name, points in series.items():
                if points:
                    values = [p["value"] for p in points[-3:]]  # 显示最后 3 个值
                    print(f"  - {mds_name}: ... -> {values}")
            return True
        else:
            print(f"查询指标序列失败：{result.get('error')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"查询指标序列异常：{e}")
        return False


def start_report_thread(config_name: str, run_id: str, stop_event: threading.Event):
    """启动上报线程"""
    seq = 0
    
    while not stop_event.is_set():
        seq += 1
        report_metrics(config_name, run_id, seq)
        
        # 等待下一次上报
        for _ in range(int(REPORT_INTERVAL_SECONDS * 10)):
            if stop_event.is_set():
                break
            time.sleep(0.1)


def main():
    """主函数"""
    print("=" * 60)
    print("CephFS 指标数据上报测试")
    print("=" * 60)
    print(f"服务地址：{BASE_URL}")
    print(f"上报间隔：{REPORT_INTERVAL_SECONDS} 秒")
    print(f"上报次数：{REPORT_COUNT}")
    print()
    
    # 检查服务是否可用
    print("检查服务是否可用...")
    try:
        response = requests.get(BASE_URL, timeout=3)
        if response.status_code == 200:
            print("✓ 服务正常运行")
        else:
            print(f"✗ 服务异常：{response.status_code}")
            return 1
    except requests.exceptions.RequestException as e:
        print(f"✗ 无法连接服务：{e}")
        print("请先启动服务：python run_web.py")
        return 1
    
    # 设置测试 run
    config_name, run_id = setup_test_run()
    
    # 启动上报线程
    print("\n" + "=" * 60)
    print("步骤 2: 开始周期性上报指标数据")
    print("=" * 60)
    
    stop_event = threading.Event()
    report_thread = threading.Thread(
        target=start_report_thread,
        args=(config_name, run_id, stop_event),
        daemon=True,
    )
    report_thread.start()
    
    # 等待上报完成
    total_reports = 0
    start_time = time.time()
    
    while total_reports < REPORT_COUNT:
        time.sleep(0.5)
        # 检查已上报的数量
        url = f"{BASE_URL}/api/configs/{config_name}/runs/{run_id}/metrics"
        try:
            response = requests.get(url, timeout=3)
            result = response.json()
            if result.get("ok"):
                current_count = len(result.get("reports", []))
                if current_count > total_reports:
                    total_reports = current_count
                    elapsed = time.time() - start_time
                    print(f"\n进度：{total_reports}/{REPORT_COUNT} (耗时：{elapsed:.1f}s)")
        except requests.exceptions.RequestException:
            pass
    
    # 停止上报
    stop_event.set()
    report_thread.join(timeout=1)
    
    # 验证数据
    verify_metrics(config_name, run_id)
    
    # 验证指标序列
    print("\n验证指标序列查询:")
    verify_metric_series(config_name, run_id, "cpu_percent")
    verify_metric_series(config_name, run_id, "server.handle_client_request")
    verify_metric_series(config_name, run_id, "mds_cache.num_inodes")
    
    # 总结
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)
    print(f"配置名称：{config_name}")
    print(f"Run ID: {run_id}")
    print(f"上报总数：{total_reports}")
    print(f"Run 目录：{REPO_ROOT / 'data' / 'runs' / config_name / run_id}")
    print()
    print("下一步操作:")
    print("1. 访问 Web 界面查看数据：http://127.0.0.1:18080")
    print("2. 选择对应的配置和 Run 记录")
    print("3. 在 Run Detail 面板中选择指标查看图表")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
