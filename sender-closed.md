query-ctx worker 好像被杀了?

mysql 命令行里运行:


```
call system$execute_background_job('test_tenant-compactor-job');
```

执行这个sql后, Compaction job 启动后, 在spawn出来的task里运行: [`tokio::spawn(async move { job.run().await })`](https://github.com/drmingdrmer/fuse-query/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/ee/src/background_service/job_scheduler.rs#L167),

开始创建一个session, 以及ClusterApi::get_nodes() 都开始了, 但是没有返回,
就是这里 [`debug!("Done create_query_context")`](https://github.com/drmingdrmer/databend/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/ee/src/background_service/compaction_job.rs#L160) 没有打印:

![image-20230709143952876](image-20230709143952876.png)

然后报了一个 `recv-end closed` 的错误, 看起来是运行这个task的runtime被drop了, 
在这个上下文环境中有什么可能导致 名为 query-ctx 的runtime会被直接drop吗?

### 几个相关的日志:

其中有一个: `pipeline_pulling_executor.rs:213: receiver has been disconnected, finish executor now` 可能也是因为runtime 被drop造成的?

```
2023-07-07T01:26:01.606594Z  INFO enterprise_query::background_service::compaction_job: src/query/ee/src/background_service/compaction_job.rs:75: Compaction job started background=true job_name=BackgroundJobIdent { tenant: "test_tenant", name: "test_tenant-compactor-job" }

2023-07-07T01:26:01.606598Z  INFO databend_query::pipelines::executor::pipeline_executor: src/query/service/src/pipelines/executor/pipeline_executor.rs:256: Init pipeline successfully, query_id: "e54af062-7fe9-4993-9d11-e466ab137a17", elapsed: 4.625µs

2023-07-07T01:26:01.606631Z DEBUG common_management::cluster::cluster_mgr: src/query/management/src/cluster/cluster_mgr.rs:106: ClusterApi::get_nodes()

2023-07-07T01:26:01.607163Z  WARN databend_query::pipelines::executor::pipeline_pulling_executor: src/query/service/src/pipelines/executor/pipeline_pulling_executor.rs:213: receiver has been disconnected, finish executor now


2023-07-07T04:56:20.059444Z ERROR worker_loop: common_meta_client::grpc_client: src/meta/client/src/grpc_client.rs:539: MetaGrpcClient failed to send response to the handle. recv-end closed request_id=47 err=PrefixList(Ok([("__fd_clusters/test_tenant/test_cluster/databend_query/ta3eWfsVXNlnA4h1VIZJW1", SeqV { seq: 5428, meta: Some(KVMeta { expire_at: Some(1688705837) }), data: "[binary]" })]))
```

### compaction job 调用链:

https://github.com/drmingdrmer/fuse-query/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/ee/src/background_service/job_scheduler.rs#L167
```
pub async fn check_and_run_job(mut job: BoxedJob, force_execute: bool) -> Result<()> {
    let info = &job.get_info().await?;
    if !Self::should_run_job(info, Utc::now(), force_execute) {
        return Ok(());
    }

    // update job status only if it is not forced to run
    if !force_execute {
        let params = info.job_params.as_ref().unwrap();
        let mut status = info.job_status.clone().unwrap();
        status.next_task_scheduled_time = params.get_next_running_time(Utc::now());
        job.update_job_status(status.clone()).await?;
        info!(background = true, next_scheduled_time = ?status.next_task_scheduled_time, "Running job");
    } else {
        info!(background = true, "Running execute job");
    }

    tokio::spawn(async move { job.run().await });
```

https://github.com/drmingdrmer/databend/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/ee/src/background_service/compaction_job.rs#L76

```
async fn run(&mut self) {
    info!(background = true, job_name = ?self.creator.clone(), "Compaction job started");
    self.do_compaction_job()
        .await
        .expect("failed to do compaction job");
}
```

https://github.com/drmingdrmer/databend/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/ee/src/background_service/compaction_job.rs#L160

```
async fn do_compaction_job(&mut self) -> Result<()> {
    let ctx = self.session.create_query_context().await?;
    debug!("Done create_query_context");
```
上面这个debug没有打出来

https://github.com/drmingdrmer/databend/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/service/src/sessions/session.rs#L130

```
pub async fn create_query_context(self: &Arc<Self>) -> Result<Arc<QueryContext>> {
    let config = GlobalConfig::instance();
    let session = self.clone();
    let cluster = ClusterDiscovery::instance().discover(&config).await?;
```

https://github.com/drmingdrmer/databend/blob/b900c2b44948f9c5161d2a61b8a87eeaa528495c/src/query/service/src/clusters/cluster.rs#L203

```
pub async fn discover(&self, config: &InnerConfig) -> Result<Arc<Cluster>> {
    match self.api_provider.get_nodes().await {
```

