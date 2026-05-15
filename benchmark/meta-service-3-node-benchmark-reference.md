# Meta-service 三节点公开 Benchmark 参考

> 用途：作为 `raft-log` / meta-service 项目性能对标的基准参考。
> 范围：etcd、TiKV、Apache ZooKeeper、HashiCorp Consul，全部 3 节点强一致部署。
> 注意：各家测试硬件、key/value 尺寸、并发参数差别极大，**横向对比前必须对齐参数**。

---

## 0. 一句话竞对定位（速查）

| 系统 | 同机房 SSD 重负载写吞吐 | 写延迟 | 读吞吐 | 读延迟 |
|------|----------------------|--------|--------|--------|
| **etcd** | ~50,000 QPS | avg 20ms | ~140k QPS (linearizable) | avg 5.5ms |
| **TiKV** | ~43,200 OPS (update) | avg <10ms | ~212,000 OPS (point get) | avg <10ms |
| **ZooKeeper** | ~2,300 ops/s (set, 单 ensemble) | avg 0.4-1ms | ~5,900 ops/s (get) | avg 0.17ms |
| **Consul** | ~14,000 writes/s（社区实测） | avg 几 ms，P99 不稳定 | ~23k-98k reads/s | sub-ms |

> 上表读法：每行的测试参数差别很大，**只做"量级"对比**。详见各家小节。

---

## 1. etcd（3 节点）

### 1.1 官方 benchmark（etcd.io）

硬件：GCE n1-standard-4 或同档 AWS，SSD 本地盘。key=8B，value=256B。

**写入**

| 场景 | 并发 | 吞吐 | 平均延迟 |
|------|------|------|----------|
| 轻负载（leader） | 1 conn / 1 client | 583 QPS | 1.6 ms |
| 重负载（leader） | 100 conn / 1000 clients | 44,341 QPS | 22 ms |
| 重负载（all members） | 100 conn / 1000 clients | 50,104 QPS | 20 ms |

**读取**

| 一致性 | 轻负载 QPS | 轻负载延迟 | 重负载 QPS | 重负载延迟 |
|--------|-----------|-----------|-----------|-----------|
| Linearizable | 1,353 | 0.7 ms | 141,578 | 5.5 ms |
| Serializable | 2,909 | 0.3 ms | 185,758 | 2.2 ms |

> 官方一句话总结："轻负载 < 1ms，重负载 > 30,000 QPS"。

### 1.2 实测 P50/P90/P99（Berops 多云评测）

3 节点 sequential put 基线，1 client / 1 connection：

| 分位 | 延迟 |
|------|------|
| P50 | 4.9 ms |
| P90 | 5.9 ms |
| P99 | 8.9 ms |
| 吞吐 | 196 req/s |

跨地域（>1600 km）吞吐断崖式下降——由 raft commit RTT 决定。

### 1.3 生产健康阈值

- `wal_fsync_duration_seconds` P99 **< 10ms**（etcd 官方推荐），**< 25ms**（D2iQ 容忍上限）
- `backend_commit_duration_seconds` P99 < 25 ms
- 请求 P99 **> 50ms 用户已能感知**，**> 100ms 进入告警/级联失败区域**
- 节点数建议 ≤ 7

---

## 2. TiKV（3 节点）

### 2.1 官方 Performance Overview

**硬件（每节点）：**
- 40 vCPU（Intel Xeon E5-2630 v4 @ 2.20GHz）
- 64 GB RAM
- 500 GB NVMe SSD
- 模式：RawKV

**测试设置：** 12-pod 部署，每 pod 40 线程，10M 操作 over 10M records，工具 go-ycsb。

| 工作负载 | 吞吐 | 延迟 |
|----------|------|------|
| YCSB workload C（point get reads） | 212,000 OPS | avg < 10ms |
| YCSB workload A（updates） | 43,200 OPS | avg < 10ms |
| 峰值（任意） | 200,000 OPS | P99 < 10ms |

### 2.2 备注

- TiKV 是**事务型** KV（默认 transactional），上述 RawKV 数据是"裸 KV"模式，不带事务，所以数字偏高。
- 重写负载下"写延迟比读延迟增长快得多"——官方原话。
- TiKV 用 Raft + RocksDB，单 region ~96MB，多 region 自动分裂；和 etcd（单一 raft group）架构本质不同。

---

## 3. Apache ZooKeeper（3 节点 ensemble）

### 3.1 ServiceLatencyOverview（10,000 znodes 顺序操作）

**4 核 / 10 客户端（最优配置）：**

| 操作 | 吞吐 | 单次延迟 |
|------|------|----------|
| create | 1,828 ops/s | 0.547 ms |
| set | 2,334 ops/s | 0.429 ms |
| get | 5,899 ops/s | 0.170 ms |
| delete | 2,630 ops/s | 0.380 ms |

**4 核 / 20 客户端（高负载）：**

| 操作 | 吞吐 | 单次延迟 |
|------|------|----------|
| create | 912 ops/s | 1.097 ms |
| set | 1,311 ops/s | 0.763 ms |
| get | 2,042 ops/s | 0.490 ms |
| delete | 1,107 ops/s | 0.904 ms |

### 3.2 ZooKeeper 3.2 时代历史数据（dbtester 对比，1KB writes，1M requests）

- 最大写吞吐：25,124 req/s
- 平均写吞吐：16,842 req/s

### 3.3 备注

- ZK 的 znode 不是简单 KV：自带 stat、ACL、watcher、ephemeral / sequential 语义，单 op 开销天然比纯 KV 高。
- **读伸缩性强**：reads 由 follower 直接服务，扩节点能线性提升读吞吐；写依然走 leader + ZAB。
- 数字偏小因为 znodes 一般不大（< 1KB）、单 client 串行特征明显。

---

## 4. HashiCorp Consul（3 server cluster）

### 4.1 dbtester 历史对比（1KB writes）

- 最大写吞吐：15,865 req/s
- 平均写吞吐：5,588 req/s

### 4.2 社区与官方零散数据

- 社区实测读：~23,000 req/s（单实例，24 核 Xeon，ramdisk，200B value）
- 优化后读吞吐峰值：~98,000 reads/s（Consul 0.7.0rc2，128B value）
- 写吞吐："500 commits/s 或更高"（高负载工况），社区另一报告平均 ~14,000 writes/s
- 写延迟："几 ms 内"为目标，实测尖刺范围 370µs ~ 190ms，平均 ~680µs
- **不提供官方公开 P50/P90/P99 数据**

### 4.3 备注

- Consul 写**强 I/O bound**（Raft log fsync），读 **CPU bound**
- 三大用途：service discovery + KV + service mesh，KV 不是它的最强项
- 跨数据中心是 Consul 的卖点，但 raft commit 受 RTT 限制和 etcd 一样

---

## 5. 横向对比注意事项

`★ Insight ─────────────────────────────────────`
- **数字看上去差几十倍，常常只是参数差异**。同一个系统，1 client vs 1000 clients 吞吐能差 100 倍；value 从 256B 到 4KB 吞吐能掉一半。看 benchmark 第一件事是看参数。
- **共识系统的延迟下界 = 网络 RTT + 多数派 fsync**。任何"P99 < 5ms"的承诺，先看部署：本地 NVMe 同机房 vs EBS 跨 AZ 完全不同量级。
- **TiKV 的数字看起来碾压一切**，但它是 multi-raft（多 region 并行）；etcd/ZK/Consul 是 single-raft（单 leader 瓶颈）。结构性差异，不是单纯实现优劣。
`─────────────────────────────────────────────────`

对比时务必对齐：

1. **节点数**：本文统一 3 节点。多到 5、7 节点时所有系统吞吐都会下降，延迟上升。
2. **value 大小**：etcd 官方用 256B、ZK 经典测试用 1KB、生产负载常见 1-10KB。
3. **fsync 策略**：默认全部 fsync 每次 commit。关掉 fsync（unsafe）数字可飙 10x，但不再可比。
4. **client 并发**：单 client 测延迟，多 client 测吞吐，**不要混报**。
5. **盘和网络**：本地 NVMe（fsync ~100µs） vs EBS gp3 (~1ms) vs HDD (~10ms)；同机房 RTT ~0.1ms vs 跨 AZ ~1ms vs 跨 region 10ms+。
6. **读一致性**：linearizable read 走 raft、serializable read 走 local——后者吞吐能高一个数量级。

### 5.1 EBS gp3 / NVMe 盘参数参考

这里的 `EBS gp3 / NVMe` 应理解为两类测试介质：`EBS gp3` 是 AWS 持久化网络块存储；`NVMe` 在 benchmark 语境里通常指 EC2 本地 NVMe instance store。注意 Nitro 实例上 EBS 也会以 NVMe block device 形式暴露，所以仅凭 `/dev/nvme*` 设备名不能区分是否为本地盘。

| 介质 | 持久性 | IOPS | 吞吐 | 延迟特征 | Benchmark 含义 |
|------|--------|------|------|----------|----------------|
| EBS gp3 | 持久化，独立于 EC2 实例生命周期 | baseline 3,000；最高 80,000 | baseline 125 MiB/s；最高 2,000 MiB/s | single-digit ms 级，实际受 EC2 EBS 带宽和队列深度影响 | 更接近云上生产持久盘，适合观察真实持久化写入下界 |
| 本地 NVMe instance store | 临时盘，实例 stop / terminate / hibernate 后数据丢失 | 几万到数百万 4KiB random IOPS，取决于实例规格 | 通常 GB/s 级，取决于实例规格和 I/O pattern | 通常低于 EBS，fsync 下界更好 | 更接近本机 SSD 极限，适合测 raft-log / WAL 实现开销 |

官方实例规格中的本地 NVMe 量级示例：

| 实例规格 | 本地 NVMe | 4KiB random read IOPS | 4KiB random write IOPS |
|----------|-----------|-----------------------|------------------------|
| i4i.large | 1 x 468 GB | 50,000 | 27,500 |
| i4i.8xlarge | 2 x 3,750 GB | 800,000 | 440,000 |
| i4i.32xlarge | 8 x 3,750 GB | 3,200,000 | 1,760,000 |
| i8g.48xlarge | 12 x 3,750 GB | 7,200,000 | 3,960,000 |

对 `raft-log` 这类 WAL/fsync 敏感 benchmark，建议两组结果分开汇报，不要合并成一个结论：`gp3` 主要反映持久云盘和 EBS 网络路径约束；本地 NVMe 主要反映日志实现、batching 和序列化路径自身的上限。

## 6. 给 raft-log 项目的对标建议

| 维度 | etcd 基线 | 建议测试点 |
|------|----------|-----------|
| 单 client 顺序写 P99 | ~9ms (256B value) | 同参数对比是否能进入 5ms 以内 |
| 重负载写吞吐（1000 clients） | ~50k QPS | 至少做到同档 |
| Linearizable read 吞吐 | ~140k QPS | 看 read path 是否能命中 raft local read 优化 |
| wal_fsync P99 | < 10ms | 在 EBS gp3 / NVMe 各跑一组 |

---

## Sources

### etcd
- [etcd v3.5 Performance（官方）](https://etcd.io/docs/v3.5/op-guide/performance/)
- [Benchmarking etcd v3 demo（官方）](https://etcd.io/docs/v3.6/benchmarks/etcd-3-demo-benchmarks/)
- [Evaluating etcd's performance in multi-cloud（Berops，含 P50/P90/P99）](https://medium.com/berops-blog/evaluating-etcds-performance-in-multi-cloud-83ee5a2fd70c)
- [Etcd Performance Benchmarking（D2iQ，fsync 阈值）](https://eng.d2iq.com/blog/etcd-performance-benchmarking/)
- [Recommended etcd practices（OKD）](https://docs.okd.io/latest/etcd/etcd-practices.html)
- [Autonomous testing of etcd's robustness（CNCF）](https://www.cncf.io/blog/2025/09/25/autonomous-testing-of-etcds-robustness/)
- [Key metrics for monitoring etcd（Datadog）](https://www.datadoghq.com/blog/etcd-key-metrics/)

### TiKV
- [TiKV Performance Overview（官方）](https://tikv.org/docs/6.1/deploy/performance/overview/)
- [TiKV Benchmark and Performance（官方）](https://tikv.org/docs/6.1/deploy/performance/performance/)
- [TiKV Benchmark Instructions（官方）](https://tikv.org/docs/5.1/deploy/performance/instructions/)
- [Building, Running, and Benchmarking TiKV and TiDB（PingCAP）](https://www.pingcap.com/blog/building-running-and-benchmarking-tikv-and-tidb/)

### ZooKeeper
- [ZooKeeper ServiceLatencyOverview（Apache wiki）](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ServiceLatencyOverview)
- [ZooKeeper 3.2 Performance（Apache wiki）](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Performance)
- [ZooKeeper: Wait-free coordination for Internet-scale systems（USENIX 论文）](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf)
- [zookeeper-benchmark（Brown University）](https://github.com/brownsys/zookeeper-benchmark)

### Consul
- [Consul Server Resource Requirements（官方）](https://developer.hashicorp.com/consul/docs/install/performance)
- [Consul capacity planning（官方）](https://developer.hashicorp.com/consul/docs/reference/architecture/capacity)
- [Configure Consul for performance at scale（Criteo）](https://medium.com/criteo-engineering/configure-consul-for-performance-at-scale-f6a089706377)
- [Jepsen: etcd and Consul（Aphyr）](https://aphyr.com/posts/316-jepsen-etcd-and-consul)

### 横向对比
- [Consul vs Zookeeper vs etcd（StackShare）](https://stackshare.io/stackups/consul-vs-etcd-vs-zookeeper)
- [In-Depth Comparison of Distributed Coordination Tools（Medium）](https://medium.com/@karim.albakry/in-depth-comparison-of-distributed-coordination-tools-consul-etcd-zookeeper-and-nacos-a6f8e5d612a6)
- [etcd-io/dbtester（官方对比工具）](https://github.com/etcd-io/dbtester)
- [Exploring Performance of Etcd, Zookeeper and Consul Key-value stores 2017（HN）](https://news.ycombinator.com/item?id=23711431)

### AWS 存储
- [Amazon EBS General Purpose SSD volumes（gp3 官方规格）](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)
- [Amazon EBS and NVMe on Nitro instances（EBS 设备名说明）](https://docs.aws.amazon.com/ebs/latest/userguide/nvme-ebs-volumes.html)
- [Amazon EC2 instance store lifetime（本地盘生命周期）](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-lifetime.html)
- [Storage optimized instances（本地 NVMe IOPS 示例）](https://docs.aws.amazon.com/ec2/latest/instancetypes/so.html)
