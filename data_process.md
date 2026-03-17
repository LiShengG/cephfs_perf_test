需求名称
`analyze_mds_perf_raw.py` 离线解析、时序计算与批量绘图工具

目标
实现一个 Python 命令行工具，读取 `collect_mds_perf_raw.sh` 生成的原始采样目录，完成以下能力：

1. 扫描并加载一个或多个实验目录
2. 读取 `summary/raw_index.jsonl` 与对应 raw JSON 文件
3. 将嵌套 `perf_dump` 展平为可分析的时序表
4. 支持按指标路径进行字段选择
5. 支持四类数据变换：

   * `raw`
   * `delta`
   * `rate`
   * `period_avg`
6. 支持多种对象粒度：

   * `per_mds`
   * `per_host`
   * `aggregate`
7. 支持单实验分析与多实验对比
8. 输出规范化时序数据文件
9. 批量生成标准静态图（PNG）
10. 输出分析日志与处理摘要

本工具定位为**离线分析与标准出图工具**，不实现网页交互，不实现在线服务。

实现语言
Python 3

脚本名称

```text
analyze_mds_perf_raw.py
```

实现范围
负责：

* 输入目录扫描
* JSON/JSONL 解析
* 指标展平
* 指标类型识别
* 时序计算
* 多实验对齐
* 聚合
* 批量绘图
* 导出 tidy 格式数据
* 错误容错与日志

不负责：

* 重新采样
* Prometheus 接入
* 交互网页
* 实时监控
* 自动生成完整实验报告文档

输入数据前提
输入目录符合如下结构：

```text
run_dir/
  meta/
    run_info.json
    mds_list.txt
  raw/
    mds.a/
      000001_20260316T120000+0800.json
      000002_20260316T120100+0800.json
  summary/
    raw_index.jsonl
  logs/
```

每个 raw 文件格式固定为：

```json
{
  "sample_meta": {
    "timestamp": "2026-03-16T12:00:00+08:00",
    "timestamp_unix": 1773633600,
    "host": "node-1",
    "mds_name": "a",
    "mds_daemon": "mds.a",
    "cluster": "ceph",
    "tag": "mdtest_run_20260316",
    "seq": 1
  },
  "perf_dump": {
    "...": "..."
  }
}
```

命令行接口要求

必选参数

```text
--input-dirs /path/run1,/path/run2
```

说明：

* 一个或多个实验目录
* 逗号分隔
* 至少一个

可选参数

```text
--output-dir /path/to/output
```

* 输出目录
* 默认：当前目录下生成带时间戳子目录

```text
--metrics mds.request,mds.reply,mds_mem.rss
```

* 指定指标路径列表
* 逗号分隔
* 若未指定，则使用内置默认指标集合

```text
--transforms raw,delta,rate,period_avg
```

* 指定要计算的数据变换
* 默认：`raw,delta,rate,period_avg`

```text
--group-by per_mds
```

支持：

* `per_mds`
* `per_host`
* `aggregate`

默认：

```text
per_mds
```

```text
--compare-mode none
```

支持：

* `none`
* `overlay`

说明：

* `none`：每个实验单独分析单独出图
* `overlay`：多实验同指标叠加对比

默认：

```text
none
```

```text
--align-time relative
```

支持：

* `absolute`
* `relative`

说明：

* `absolute`：横轴使用真实时间戳
* `relative`：横轴使用相对实验起点秒数

默认：

```text
relative
```

```text
--plot-formats png
```

支持逗号分隔：

* `png`
* `svg`

默认：

```text
png
```

```text
--export-formats csv,parquet,jsonl
```

支持逗号分隔：

* `csv`
* `parquet`
* `jsonl`

默认：

```text
csv,parquet
```

```text
--default-plots standard
```

支持：

* `none`
* `standard`

说明：

* `standard`：生成内置标准图集
* `none`：只导出规范化数据，不出图

默认：

```text
standard
```

```text
--reset-policy mark
```

支持：

* `mark`
* `drop`

说明：

* `mark`：检测到 counter 回绕/重启时，将该周期标记为 reset，结果置空但保留记录
* `drop`：直接丢弃该周期记录

默认：

```text
mark
```

```text
--max-workers 4
```

* 解析文件时的最大并发 worker 数
* 默认：4

```text
--config /path/to/config.yaml
```

* 可选配置文件
* 若传入，则命令行参数优先级更高

工具总体流程
按以下阶段实现：

1. 加载实验目录
2. 读取 `meta/run_info.json`
3. 读取 `summary/raw_index.jsonl`
4. 校验索引与 raw 文件一致性
5. 加载 raw JSON
6. 展平 `perf_dump`
7. 生成规范化原始表
8. 识别指标类型
9. 计算 `delta / rate / period_avg`
10. 生成聚合表
11. 导出时序数据
12. 批量绘图
13. 输出处理摘要

内部数据模型要求
实现时必须形成清晰的数据模型，不允许直接在零散字典上堆逻辑。

建议至少有以下逻辑对象：

* `Experiment`
* `SampleRecord`
* `MetricSeries`
* `AnalysisConfig`

规范化时序表 schema
所有后续分析必须先落到统一的 tidy schema，核心表建议命名为：

```text
timeseries_tidy
```

每一行表示一个实验、一个时间点、一个对象、一个指标、一个变换结果。

字段至少包括：

* `experiment_id`
* `experiment_tag`
* `cluster_name`
* `host`
* `mds_name`
* `mds_daemon`
* `group_key`
* `timestamp`
* `timestamp_unix`
* `time_offset_sec`
* `seq`
* `metric_path`
* `metric_type`
* `transform`
* `value`
* `unit`
* `reset_detected`
* `is_valid`
* `source_file`

字段解释：

* `experiment_id`：实验唯一标识，建议由目录名或 tag 生成
* `group_key`：

  * `per_mds` 时等于 `mds.<name>`
  * `per_host` 时等于 `host`
  * `aggregate` 时等于 `all`
* `metric_path`：展平后的指标路径
* `metric_type`：`counter` / `gauge` / `latency_pair` / `unknown`
* `transform`：`raw` / `delta` / `rate` / `period_avg`
* `value`：数值结果
* `unit`：可为空，后续可扩展
* `reset_detected`：布尔值
* `is_valid`：布尔值
* `source_file`：对应 raw JSON 相对路径

输出目录结构要求
固定输出如下结构：

```text
output_dir/
  meta/
    analysis_run_info.json
    input_runs.json
    metric_catalog.csv
  tidy/
    timeseries_tidy.csv
    timeseries_tidy.parquet
    timeseries_tidy.jsonl
  derived/
    per_mds/
      ...
    per_host/
      ...
    aggregate/
      ...
  plots/
    per_experiment/
      <experiment_id>/
        ...
    compare/
      ...
  logs/
    analyze.log
    errors.log
```

指标展平规则
必须将 `perf_dump` 的嵌套结构展平成路径形式，使用 `.` 连接。

示例：

原始：

```json
{
  "mds": {
    "request": 40540430,
    "reply_latency": {
      "avgcount": 38648440,
      "sum": 6996418.812752685,
      "avgtime": 0.181027198
    }
  }
}
```

展平后应得到：

```text
mds.request
mds.reply_latency.avgcount
mds.reply_latency.sum
mds.reply_latency.avgtime
```

要求：

* 展平逻辑必须稳定
* 不得丢字段
* 不得对原始路径做别名替换
* 后续 metric_path 一律基于展平路径工作

指标类型识别规则
必须内置一套指标类型识别逻辑。

类型 1：`gauge`
直接使用原值，不做差分。
识别方式：

* 用户配置显式指定为 gauge
* 或命中内置 gauge 路径集合

默认 gauge 路径至少包含：

```text
mds.q
mds.inodes
mds.inodes_pinned
mds.inodes_with_caps
mds.caps
mds.subtrees
mds_cache.num_strays
mds_cache.num_strays_delayed
mds_cache.num_strays_enqueuing
mds_mem.ino
mds_mem.dn
mds_mem.cap
mds_mem.rss
mds_mem.heap
```

类型 2：`counter`
累计值，支持 `raw / delta / rate`。
识别方式：

* 用户配置显式指定为 counter
* 或命中内置 counter 路径集合
* 或数字型字段且不属于 latency_pair/gauge 时可暂归类为 `unknown`，不自动强行做 delta

默认 counter 路径至少包含：

```text
mds.request
mds.reply
mds.forward
mds.dir_fetch
mds.openino_peer_discover
mds.traverse
mds.traverse_hit
mds.traverse_dir_fetch
mds.load_cent
mds.exported
mds.exported_inodes
mds.imported
mds.imported_inodes
mds.root_rfiles
mds_cache.strays_created
mds_cache.strays_enqueued
mds_log.evadd
mds_log.evex
mds_log.evtrm
mds_log.segadd
mds_log.segex
mds_log.segtrm
mds_server.dispatch_client_request
mds_server.handle_client_request
mds_server.handle_client_session
```

类型 3：`latency_pair`
指一组路径：

* `<base>.avgcount`
* `<base>.sum`
* `<base>.avgtime`

计算时：

* `raw`：可分别保留原始子字段
* `period_avg`：只对 `<base>` 生成一条时序值
* `delta` / `rate`：对 `.avgcount` 和 `.sum` 可单独支持；对 `<base>` 本身不输出 delta/rate

默认 latency_pair 基路径至少包含：

```text
mds.reply_latency
mds_log.jlat
mds_server.req_create_latency
mds_server.req_lookup_latency
mds_server.req_lookupino_latency
mds_server.req_getattr_latency
mds_server.req_readdir_latency
mds_server.req_setxattr_latency
mds_server.req_unlink_latency
```

类型 4：`unknown`

* 原始值可导出
* 不参与 delta/rate/period_avg，除非用户显式配置

时序计算规则

一、`raw`
适用：

* 所有数字型字段
* 对 counter/gauge/latency 子字段均可保留原值

规则：

* 直接使用当前采样值
* 不做计算

二、`delta`
适用：

* `counter`
* `latency_pair` 的 `.avgcount`、`.sum`

规则：

```text
delta = curr - prev
```

要求：

* 按 `experiment_id + group_key + metric_path` 独立计算
* 按时间排序后，对相邻采样点做差分
* 第一条记录无前驱，值为 null，`is_valid=false`

三、`rate`
适用：

* `counter`
* `latency_pair` 的 `.avgcount`、`.sum`

规则：

```text
rate = (curr - prev) / interval_sec_actual
```

要求：

* 使用相邻采样点的实际时间差
* 第一条记录无前驱，值为 null

四、`period_avg`
适用：

* `latency_pair` 基路径

规则：
对同一 `<base>`：

```text
delta_sum = curr_sum - prev_sum
delta_count = curr_avgcount - prev_avgcount
period_avg = delta_sum / delta_count
```

要求：

* 若 `delta_count <= 0`，则 `period_avg = null`
* 输出时 metric_path 使用基路径，例如：

  * `mds_server.req_unlink_latency`
  * transform = `period_avg`

时间轴处理规则
必须同时支持两种时间轴：

1. `absolute`

* 使用 `timestamp` 作为绘图横轴
* 数据中保留真实采样时间

2. `relative`

* 每个实验以最早采样点作为零点
* `time_offset_sec = timestamp_unix - experiment_start_timestamp_unix`
* 绘图时使用 `time_offset_sec`

多实验对比时，默认使用 `relative` 更合理。

聚合规则
支持三种 group 模式：

一、`per_mds`

* 每个 `mds.<name>` 单独分析
* `group_key = mds.<name>`

二、`per_host`

* 同一 host 下多个 mds 聚合
* 对相同实验、相同时间点、相同指标做聚合

聚合方式：

* gauge：默认 `mean`
* counter raw：默认 `sum`
* delta/rate：默认 `sum`
* period_avg：默认加权平均，权重为 `delta_count`
* 若无可用权重，则退化为简单平均

三、`aggregate`

* 同一实验下所有对象聚合
* 规则同 `per_host`

要求：

* 聚合策略要写成独立函数
* 后续可扩展

多实验对比规则
支持 `compare_mode=overlay`

行为：

* 选定同一 `metric_path + transform + group_mode`
* 多个实验叠加到同一张图
* 若 `align_time=relative`，横轴为相对秒数
* 图例按 `experiment_id` 区分

要求：

* 多实验对比只在相同变换、相同聚合粒度下进行
* 若某实验缺少该指标，不报错，跳过即可

重启/回绕检测规则
仅对 `counter` 和 `latency_pair` 做检测。

判定：

```text
curr < prev
```

处理：

* 标记 `reset_detected = true`
* 若 `reset_policy = mark`：

  * 保留该周期记录
  * `value = null`
  * `is_valid = false`
* 若 `reset_policy = drop`：

  * 直接删除该周期该指标记录

注意：

* reset 检测必须按 `experiment_id + group_key + metric_path` 独立进行
* 不能跨实验、跨 daemon 混算

原始数据校验要求
在正式分析前必须做输入校验：

1. `raw_index.jsonl` 可读且每行是合法 JSON
2. `file` 指向的 raw 文件存在
3. raw 文件包含顶层：

   * `sample_meta`
   * `perf_dump`
4. `sample_meta` 至少包含：

   * `timestamp`
   * `timestamp_unix`
   * `host`
   * `mds_name`
   * `mds_daemon`
   * `seq`
5. 时间戳可解析
6. 同一实验、同一 MDS 下 `seq` 允许不连续，但必须可排序

错误容错要求

* 单个 raw 文件损坏：记录错误，跳过该文件，不中断整批分析
* 单个实验目录缺少某文件：记录错误，继续处理其他实验
* 某指标在某实验中缺失：允许，结果为空，不报致命错误
* Parquet 导出失败：记录错误，但不影响 CSV 导出
* 某张图绘制失败：记录错误，继续绘制其他图

默认标准图集要求
当 `--default-plots standard` 时，必须至少生成以下图：

一、单实验 per_mds 图
每个实验单独输出以下图，每个 MDS 一条线：

1. `mds.request` 的 `rate`
2. `mds.reply` 的 `rate`
3. `mds.forward` 的 `rate`
4. `mds.openino_peer_discover` 的 `rate`
5. `mds.traverse` 的 `rate`
6. `mds_cache.strays_created` 的 `rate`
7. `mds_mem.rss` 的 `raw`
8. `mds.q` 的 `raw`
9. `mds_server.req_lookup_latency` 的 `period_avg`
10. `mds_server.req_lookupino_latency` 的 `period_avg`
11. `mds_server.req_unlink_latency` 的 `period_avg`
12. `mds_log.jlat` 的 `period_avg`

二、单实验 aggregate 图
每个实验输出聚合图：

1. `mds.request` 的 `rate`
2. `mds.reply` 的 `rate`
3. `mds_cache.strays_created` 的 `rate`
4. `mds_mem.rss` 的 `raw`
5. `mds_server.req_lookupino_latency` 的 `period_avg`
6. `mds_server.req_unlink_latency` 的 `period_avg`

三、多实验 overlay 图
当输入目录数大于 1 且 `compare_mode=overlay` 时，至少输出：

1. `aggregate + mds.request + rate`
2. `aggregate + mds.reply + rate`
3. `aggregate + mds_cache.strays_created + rate`
4. `aggregate + mds_mem.rss + raw`
5. `aggregate + mds_server.req_lookupino_latency + period_avg`
6. `aggregate + mds_server.req_unlink_latency + period_avg`

绘图要求
使用 `matplotlib`，不要使用 seaborn。
要求：

* 每张图单独文件，不做多 subplot 拼图
* 默认不手动指定颜色
* 必须有标题
* 必须有横轴标签、纵轴标签
* 必须有图例
* 必须自动处理时间排序
* 线条缺失值处允许断线
* 图片默认尺寸统一
* 文件名稳定、可读

建议文件命名格式：

```text
<scope>__<metric_path>__<transform>__<group_mode>.png
```

其中 `metric_path` 中的 `.` 可替换为 `__`

示例：

```text
per_experiment/runA/aggregate__mds_server__req_unlink_latency__period_avg__aggregate.png
```

导出数据要求

一、`tidy/timeseries_tidy.*`
导出完整 tidy 表，包含所有 experiment、所有 group、所有 metric、所有 transform

二、`meta/metric_catalog.csv`
输出指标目录，至少包含：

* `metric_path`
* `metric_type`
* `first_seen_experiment`
* `non_null_count`
* `sample_count`

三、`meta/input_runs.json`
记录所有输入实验目录及其解析状态

四、`meta/analysis_run_info.json`
记录本次分析任务元数据，至少包含：

* 输入参数
* 启动时间
* 输入实验数
* 成功实验数
* 失败实验数
* 解析文件数
* 成功文件数
* 失败文件数
* 导出格式
* 图像数

依赖要求
Python 依赖建议：

* `pandas`
* `numpy`
* `matplotlib`
* `pyarrow`（parquet 可选）
* `pathlib`
* `json`
* `argparse`
* `logging`
* `concurrent.futures`

要求：

* 若缺少 `pyarrow`，允许跳过 parquet 导出，但必须记录 warning
* 其他核心依赖缺失则直接报错退出

配置文件支持
可选支持 YAML 配置文件。若实现，则配置项可包括：

* 默认 metrics
* gauge/counter/latency 分类覆盖
* 默认 plot 集合
* 默认聚合策略
* 默认导出格式

命令行参数优先级高于配置文件。

建议内部模块划分
即使先写成单文件，也应按清晰函数分层组织。建议逻辑函数至少包括：

* `parse_args`
* `load_config`
* `discover_runs`
* `load_run_info`
* `load_raw_index`
* `validate_run_structure`
* `load_raw_sample`
* `flatten_perf_dump`
* `build_raw_dataframe`
* `infer_metric_types`
* `compute_delta`
* `compute_rate`
* `compute_period_avg`
* `detect_reset`
* `build_tidy_timeseries`
* `aggregate_series`
* `export_tidy_data`
* `build_metric_catalog`
* `plot_metric_series`
* `plot_standard_suite`
* `write_analysis_meta`
* `log_summary`

性能要求

* 单次可稳定处理 1~10 个实验目录
* 单实验 1000~10000 个 raw 文件场景下应可运行
* 文件解析支持并发，但最终结果必须稳定排序
* 不能为了速度牺牲正确性
* 优先保证分析口径正确

成功验收标准

1. 单实验解析
   执行：

```text
python analyze_mds_perf_raw.py --input-dirs ./run1 --output-dir ./out
```

预期：

* 成功生成 `timeseries_tidy.csv`
* 成功生成 `metric_catalog.csv`
* 成功生成标准图集
* 输出日志中有处理摘要

2. 多实验对比
   执行：

```text
python analyze_mds_perf_raw.py --input-dirs ./run1,./run2 --compare-mode overlay --align-time relative --output-dir ./out
```

预期：

* 成功生成多实验 overlay 图
* 相同指标按实验叠加绘制
* 横轴为相对秒数

3. reset 检测
   人工构造某 counter 回绕。

预期：

* 检测到 reset
* `mark` 模式下该周期记录存在但 value 为空
* 日志记录 reset 事件

4. latency 计算
   对某一 latency base：

* 正确读取 `.sum` 与 `.avgcount`
* 正确计算 `period_avg`
* 第一条记录无前驱，值为空

5. 容错
   损坏一个 raw 文件。

预期：

* 程序记录错误
* 跳过坏文件
* 其他数据继续分析
* 非零坏文件不导致全任务失败

可直接执行的任务描述
请实现 Python 命令行工具 `analyze_mds_perf_raw.py`，读取一个或多个 `collect_mds_perf_raw.sh` 生成的实验目录，基于 `summary/raw_index.jsonl` 与 raw JSON 完成离线解析、指标展平、类型识别、时序计算（raw/delta/rate/period_avg）、按 `per_mds/per_host/aggregate` 聚合、单实验与多实验 overlay 对比、规范化 tidy 数据导出以及标准静态图批量生成。工具必须优先保证分析口径正确、结果稳定、容错清晰，不实现网页交互。可将上述任务做拆分，分多次渐进式编码实现
