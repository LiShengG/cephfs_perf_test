# cephfs_perf_test
请实现一个 bash 脚本 collect_mds_metrics.sh，
在run_distributed_mdtest_final.sh中启动mpi任务前提前5s做一件下面的事，
该脚本先存放在一个管控主机上，已提供用于保存cephfs mds进程IP的ceph_host文件，先将该collect_mds_metrics.sh复制到所有的mds节点, 然后ssh 启动该脚本开始采集数据
用于 CephFS 元数据压测期间周期性采集指定 MDS 的 ceph tell mds.<name> perf dump  | jq '.mds' JSON 和本机 ceph-mds 进程资源信息。
ceph daemon /var/run/ceph/ceph-mds.<name> perf dump 命令 中存在一个参数变量，需要自动识别，路径在/var/run/ceph/下如ceph-mds.node-2.cephfs.dowhqz.asok, 
每次采集结加上时序时间，将结果输出到josn中一条记录如 {"ts":"2026-03-13T14:00:01+08:00","mds":"mds.node-2.cephfs.dowhqz","perf_dump":{"mds":{...}}}。
如果某轮采集失败：不要写半截 JSON错误写到 errors.log下一轮继续采样，不要因为单次失败直接退出整个采集器 

脚本需要支持 --outdir、--interval、--duration 等参数，输出标准化目录结构，
为每个 mds 生成 perf_dump_series.jsonl、proc_stat_series.tsv、errors.log, 
支持 SIGINT/SIGTERM 优雅退出，单个 mds 或单轮采集失败不能导致整个脚本退出。

在run_distributed_mdtest_final.sh中mpi任务完成后延时5s后将远程采集的数据文件复制到本地的当此测试结果文件中，每个节点新创一个文件夹用于存放对应采集数据
采集数据文件必须标准化，预留接口为后续的数据自动绘图，自动抽取 关键字段，自动生成 HTML/PDF 报告做准备。