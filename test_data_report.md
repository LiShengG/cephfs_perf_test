# 数据上报和展示问题排查总结

## 问题现象

Web 界面看不到上报的数据图表。

## 排查过程

### 1. 确认数据已上报成功 ✓

```bash
curl http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/test_mdtest_config_20260322_125415/metrics
```

**结果**: API 返回了 150+ 条上报记录，数据存储正常。

### 2. 确认指标序列 API 正常 ✓

```bash
curl "http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/test_mdtest_config_20260322_125415/metrics/series?metric=cpu_percent"
```

**结果**: API 正常返回按 MDS 分组的时序数据。

### 3. 发现问题根源

**`conf/metrics_schema.json` 中定义的指标键名与上报数据不匹配！**

| Schema 定义 | 上报数据字段 | 是否匹配 |
|------------|-------------|---------|
| `cpu_pct` | `cpu_percent` | ❌ 不匹配 |
| `rss_kb` | `memory_mb` | ❌ 不匹配 |
| `read_bytes` | `io_read_bytes` | ❌ 不匹配 |
| `write_bytes` | `io_write_bytes` | ❌ 不匹配 |
| `mds.request` | `server.handle_client_request` | ❌ 不匹配 |

## 解决方案

### 已更新 `conf/metrics_schema.json`

新的指标定义：

```json
{
  "metrics": [
    {
      "key": "cpu_percent",
      "label": "CPU 使用率",
      "source": "proc_stat",
      "unit": "%"
    },
    {
      "key": "memory_mb",
      "label": "内存使用",
      "source": "proc_stat",
      "unit": "MB"
    },
    {
      "key": "io_read_bytes",
      "label": "IO 读取字节",
      "source": "proc_stat",
      "unit": "bytes"
    },
    {
      "key": "io_write_bytes",
      "label": "IO 写入字节",
      "source": "proc_stat",
      "unit": "bytes"
    },
    {
      "key": "open_files",
      "label": "打开文件数",
      "source": "proc_stat",
      "unit": "count"
    },
    {
      "key": "server.handle_client_request",
      "label": "MDS 请求处理",
      "source": "perf_dump",
      "unit": "count"
    },
    {
      "key": "server.journal_latency",
      "label": "日志延迟",
      "source": "perf_dump",
      "unit": "sec"
    },
    {
      "key": "mds_cache.num_inodes",
      "label": "Inode 数量",
      "source": "perf_dump",
      "unit": "count"
    },
    {
      "key": "mds_cache.num_caps",
      "label": "Caps 数量",
      "source": "perf_dump",
      "unit": "count"
    }
  ]
}
```

## 使用方法

### 1. 重启 Web 服务（如需要）

如果服务已在运行，Flask 会自动重新加载配置文件。否则：

```bash
# 停止现有服务
# 重新启动
python run_web.py
```

### 2. 上报测试数据

```bash
# 使用测试脚本（自动创建配置和 Run）
python test_metrics_report.py

# 或使用上报工具（手动指定 Run）
python metrics_reporter.py --config test_mdtest_config --run test_mdtest_config_20260322_125415 --interval 2 --count 10
```

### 3. 在 Web 界面查看数据

1. 访问 `http://127.0.0.1:18080`
2. 在左侧 Configs 面板选择 `test_mdtest_config`
3. 点击对应的 Run 记录（如 `test_mdtest_config_20260322_125415`）
4. 在 Run Detail 面板中：
   - 从下拉菜单选择指标（如 `CPU 使用率 (cpu_percent)`）
   - 点击 "Add Chart" 按钮
   - 查看图表

### 4. 使用独立测试页面

打开 `test_data_viewer.html` 文件，可以：
- 选择配置和 Run
- 查看上报数据概览
- 选择不同的指标查看图表
- 查看原始数据

## 验证步骤

```bash
# 1. 检查配置是否已更新
curl http://127.0.0.1:18080/api/configs | python -c "import sys,json; d=json.load(sys.stdin); print([m['key'] for m in d['metrics_schema']['metrics']])"

# 2. 检查数据是否存在
curl "http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/test_mdtest_config_20260322_125415/metrics" | python -c "import sys,json; d=json.load(sys.stdin); print(f'报告数量：{len(d[\"reports\"])}')"

# 3. 检查指标序列
curl "http://127.0.0.1:18080/api/configs/test_mdtest_config/runs/test_mdtest_config_20260322_125415/metrics/series?metric=cpu_percent" | python -c "import sys,json; d=json.load(sys.stdin); print(f'MDS 数量：{len(d[\"data\"][\"series\"])}')"
```

## 常见问题

### Q: 界面上看不到图表
A: 确保：
1. 已选择正确的配置和 Run
2. 该 Run 有上报数据
3. 选择的指标在上报数据中存在
4. 点击了 "Add Chart" 按钮

### Q: 图表显示 "No data for this metric"
A: 检查：
1. metrics_schema.json 中的指标键名是否正确
2. 上报数据中是否包含该字段
3. 字段值是否为数值类型

### Q: 如何添加新的指标？
A: 
1. 在 `conf/metrics_schema.json` 中添加新指标定义
2. 确保上报数据中包含对应字段
3. 重启 Web 服务或等待自动重载

## 文件清单

| 文件 | 说明 |
|------|------|
| `test_metrics_report.py` | 完整测试脚本，自动创建配置和 Run |
| `metrics_reporter.py` | 轻量级上报工具 |
| `test_data_viewer.html` | 独立测试页面 |
| `conf/metrics_schema.json` | 指标定义（已更新） |
| `test_metrics_report.md` | 测试脚本使用说明 |
| `metrics_reporter.md` | 上报工具使用说明 |
