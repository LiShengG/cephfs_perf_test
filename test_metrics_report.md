# 指标数据上报测试说明

## 概述

`test_metrics_report.py` 是一个用于测试 CephFS 性能测试项目指标数据上报功能的脚本。它会模拟客户端周期性地上报 MDS 性能指标数据，并验证数据能否被正确存储和查询。

## 前置条件

1. **安装依赖**
   ```bash
   pip install requests flask
   ```

2. **启动 Web 服务**
   
   在运行测试脚本之前，需要先启动 Web 服务：
   ```bash
   python run_web.py
   ```
   
   服务默认运行在 `http://127.0.0.1:18080`

## 使用方法

### 基本用法

在一个新的终端窗口中运行测试脚本：

```bash
python test_metrics_report.py
```

### 自定义配置

可以在脚本中修改以下配置参数：

```python
BASE_URL = "http://127.0.0.1:18080"          # 服务地址
TEST_CONFIG_NAME = "test_mdtest_config"      # 测试配置名称
REPORT_INTERVAL_SECONDS = 2                  # 上报间隔（秒）
REPORT_COUNT = 10                            # 上报次数
```

## 测试流程

脚本会自动执行以下步骤：

1. **创建测试配置和 Run 记录**
   - 检查配置是否存在，不存在则创建
   - 创建 Run 目录结构

2. **周期性上报指标数据**
   - 每隔指定时间上报一次模拟的 MDS 性能数据
   - 数据包含：
     - `proc_stat`: CPU、内存、IO 等进程统计信息
     - `perf_dump`: Ceph MDS 性能转储数据

3. **验证数据查询**
   - 查询所有报告记录
   - 按 MDS 分组统计
   - 验证指标序列查询

## 上报的数据结构

每次上报的数据格式如下：

```json
{
  "config_name": "test_mdtest_config",
  "run_id": "test_mdtest_config_20260322_143000",
  "mds_name": "mds0",
  "host": "host1",
  "ts": "2026-03-22T14:30:00.000000",
  "seq": 1,
  "proc_stat": {
    "cpu_percent": 45.67,
    "memory_mb": 1024,
    "io_read_bytes": 50000000,
    "io_write_bytes": 30000000,
    "open_files": 500
  },
  "perf_dump": {
    "mds": {
      "server": {
        "handle_client_request": {
          "avgcount": 5000,
          "sum": 500000
        },
        "journal_latency": {
          "avgcount": 5000,
          "sum": 2.5
        }
      },
      "mds_cache": {
        "num_inodes": 50000,
        "num_caps": 250000
      }
    }
  }
}
```

## 数据存储位置

上报的数据存储在：

```
data/runs/<config_name>/<run_id>/metrics/
├── reports.jsonl    # 所有报告记录（JSONL 格式）
└── latest.json      # 最新一条报告
```

## 验证数据

### 1. 通过 API 查询

```bash
# 查询所有报告
curl http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/<run_id>/metrics

# 查询特定指标的序列数据
curl "http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/<run_id>/metrics/series?metric=cpu_percent"
```

### 2. 通过 Web 界面查看

1. 访问 `http://127.0.0.1:18080`
2. 在左侧 Configs 面板中选择 `test_mdtest_config`
3. 在 Run Control 面板中选择对应的 Run 记录
4. 在 Run Detail 面板中选择指标查看图表

## 清理测试数据

测试完成后，如需清理测试数据：

```bash
# 删除测试 Run 目录
rm -rf data/runs/test_mdtest_config/

# 删除测试配置
rm data/configs/test_mdtest_config.json
```

## 常见问题

### 服务无法连接

确保 Web 服务已启动：
```bash
python run_web.py
```

### 配置已存在

如果提示配置已存在，可以：
1. 使用现有的配置名称
2. 或者删除旧配置后重新运行

### 上报失败

检查：
1. Run 目录是否存在
2. config_name 和 run_id 是否匹配
3. 服务日志中的错误信息
