# MDS 指标上报工具使用说明

## 概述

`metrics_reporter.py` 是一个模拟 MDS 性能指标数据上报的测试工具，用于验证项目的数据接收和展示功能。

## 使用方法

### 单次上报

```bash
python metrics_reporter.py --config <配置名称> --run <RunID>
```

示例：
```bash
python metrics_reporter.py --config test_mdtest_config --run test_mdtest_config_20260322_125415
```

### 周期性上报

```bash
python metrics_reporter.py --config <配置名称> --run <RunID> --interval <间隔秒数> --count <次数>
```

示例（每 2 秒上报一次，共 10 次）：
```bash
python metrics_reporter.py --config test_mdtest_config --run test_mdtest_config_20260322_125415 --interval 2 --count 10
```

### 后台持续上报

```bash
# Linux/Mac
nohup python metrics_reporter.py --config test --run run001 --interval 5 &

# Windows (PowerShell)
Start-Process python -ArgumentList "metrics_reporter.py --config test --run run001 --interval 5" -WindowStyle Hidden
```

## 命令行参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `--config` | 是 | 配置名称 |
| `--run` | 是 | Run ID |
| `--mds` | 否 | MDS 名称（默认：mds_主机名） |
| `--host` | 否 | 主机名（默认：当前主机名） |
| `--url` | 否 | Web 服务地址（默认：http://127.0.0.1:18080） |
| `--interval` | 否 | 上报间隔秒数（0 表示单次上报） |
| `--count` | 否 | 上报次数（仅周期性模式有效，0 表示无限） |

## 上报的数据结构

每次上报会生成以下模拟数据：

```json
{
  "config_name": "test_mdtest_config",
  "run_id": "test_mdtest_config_20260322_125415",
  "mds_name": "mds_test",
  "host": "LAPTOP-OOCU98CN",
  "ts": "2026-03-22T12:54:15.000000",
  "seq": 1,
  "proc_stat": {
    "cpu_percent": 45.67,
    "memory_mb": 2048,
    "io_read_bytes": 123456789,
    "io_write_bytes": 98765432,
    "open_files": 500
  },
  "perf_dump": {
    "mds": {
      "server": {
        "handle_client_request": {
          "avgcount": 25000,
          "sum": 2500000
        },
        "journal_latency": {
          "avgcount": 25000,
          "sum": 5.2345
        }
      },
      "mds_cache": {
        "num_inodes": 100000,
        "num_caps": 500000
      }
    }
  }
}
```

## 完整测试流程

### 1. 启动 Web 服务

```bash
python run_web.py
```

### 2. 运行测试脚本（自动创建配置和 Run）

```bash
python test_metrics_report.py
```

### 3. 或使用上报工具手动测试

```bash
# 先创建配置（通过 Web 界面或 API）
# 然后创建 Run 目录
# 最后上报数据
python metrics_reporter.py --config test --run run001 --interval 2 --count 10
```

### 4. 查看数据

访问 `http://127.0.0.1:18080`，在 Web 界面中查看上报的指标数据图表。

## 注意事项

1. **Run 目录必须存在**：上报前需要确保 `data/runs/<config_name>/<run_id>/` 目录已创建
2. **服务地址**：确保 `--url` 参数与实际 Web 服务地址匹配
3. **模拟数据**：此工具仅用于测试，生产环境请使用实际的采集脚本
