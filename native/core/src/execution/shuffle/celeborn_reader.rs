// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Celeborn Shuffle Reader for Apache Comet
//!
//! This module provides integration with Apache Celeborn for reading shuffle data.
//! It implements a shuffle reader that fetches data from Celeborn workers.
//!
//! Note: The `ClientManager` has been moved to the `celeborn_client` crate for reuse.

use crate::execution::shuffle::read_ipc_compressed;
use async_trait::async_trait;
use celeborn_client::{CelebornConfig, ExecutorShuffleClient, ClientManager};
use celeborn_client::protocol::BatchIterator;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time},
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::{Future, FutureExt, Stream};
use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

/// Configuration for Celeborn shuffle reader
#[derive(Debug, Clone)]
pub struct CelebornShuffleReaderConfig {
    /// Celeborn master endpoints
    pub master_endpoints: Vec<String>,
    /// Application ID
    pub app_id: String,
    /// Shuffle ID
    pub shuffle_id: i32,
    /// Partition ID to read
    pub partition_id: i32,
    /// Attempt number
    pub attempt_number: i32,
    /// Start map index (for range fetch)
    pub start_map_index: i32,
    /// End map index (for range fetch)
    pub end_map_index: i32,
    /// LifecycleManager host
    pub lifecycle_manager_host: String,
    /// LifecycleManager port
    pub lifecycle_manager_port: i32,
}

/// Celeborn shuffle reader execution plan
pub struct CelebornShuffleReaderExec {
    /// Schema of the shuffle data
    schema: SchemaRef,
    /// Celeborn configuration
    config: CelebornShuffleReaderConfig,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties cache
    cache: PlanProperties,
}

impl CelebornShuffleReaderExec {
    /// Create a new CelebornShuffleReaderExec
    pub fn try_new(schema: SchemaRef, config: CelebornShuffleReaderConfig) -> Result<Self> {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            schema,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }
}

impl Debug for CelebornShuffleReaderExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelebornShuffleReaderExec")
            .field("shuffle_id", &self.config.shuffle_id)
            .field("partition_id", &self.config.partition_id)
            .finish()
    }
}

impl DisplayAs for CelebornShuffleReaderExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CelebornShuffleReaderExec: shuffle_id={}, partition_id={}",
                    self.config.shuffle_id, self.config.partition_id
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CelebornShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "CelebornShuffleReaderExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(CelebornShuffleReaderExec::try_new(
                Arc::clone(&self.schema),
                self.config.clone(),
            )?))
        } else {
            Err(DataFusionError::Internal(
                "CelebornShuffleReaderExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = CelebornReaderMetrics::new(&self.metrics, 0);

        // Config already has partition_id and attempt_number set from PhysicalPlanner
        let config = self.config.clone();

        let stream = CelebornShuffleStream::new(
            Arc::clone(&self.schema),
            config,
            metrics,
        );

        Ok(Box::pin(stream))
    }
}

/// Metrics for Celeborn shuffle reader
struct CelebornReaderMetrics {
    /// Base metrics
    baseline: BaselineMetrics,
    /// Time spent fetching data from Celeborn
    fetch_time: Time,
    /// Time spent decoding IPC data
    decode_time: Time,
    /// Total bytes fetched
    bytes_fetched: Count,
    /// Number of batches read
    batches_read: Count,
}

impl CelebornReaderMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            fetch_time: MetricBuilder::new(metrics).subset_time("fetch_time", partition),
            decode_time: MetricBuilder::new(metrics).subset_time("decode_time", partition),
            bytes_fetched: MetricBuilder::new(metrics).counter("bytes_fetched", partition),
            batches_read: MetricBuilder::new(metrics).counter("batches_read", partition),
        }
    }
}

/// State of the CelebornShuffleStream
enum StreamState {
    /// Not yet initialized, need to start initialization
    Uninitialized,
    /// Currently initializing (future is in progress)
    Initializing(Pin<Box<dyn Future<Output = Result<InitializedData>> + Send>>),
    /// Initialized and ready to return batches
    Ready(ReadyState),
    /// Stream has finished
    Finished,
}

/// Data returned after successful initialization
struct InitializedData {
    client: Arc<ExecutorShuffleClient>,
    buffered_batches: Vec<RecordBatch>,
    bytes_fetched: usize,
    batches_count: usize,
}

/// State when stream is ready to return batches
struct ReadyState {
    #[allow(dead_code)]
    client: Arc<ExecutorShuffleClient>,
    buffered_batches: Vec<RecordBatch>,
    current_index: usize,
}

/// Stream that reads shuffle data from Celeborn
struct CelebornShuffleStream {
    /// Schema
    schema: SchemaRef,
    /// Configuration
    config: CelebornShuffleReaderConfig,
    /// Metrics
    metrics: CelebornReaderMetrics,
    /// Current state of the stream
    state: StreamState,
}

impl CelebornShuffleStream {
    fn new(
        schema: SchemaRef,
        config: CelebornShuffleReaderConfig,
        metrics: CelebornReaderMetrics,
    ) -> Self {
        Self {
            schema,
            config,
            metrics,
            state: StreamState::Uninitialized,
        }
    }

    /// Create the initialization future
    fn create_init_future(
        config: CelebornShuffleReaderConfig,
    ) -> Pin<Box<dyn Future<Output = Result<InitializedData>> + Send>> {
        Box::pin(async move {
            let config_clone = config.clone();

            // Offload the entire Celeborn interaction to a dedicated thread with its own Runtime.
            // This avoids any potential issues with nested Runtimes or cross-Runtime future polling.
            let init_result = tokio::task::spawn_blocking(move || {
                std::thread::spawn(move || {
                    // Create a local Runtime for this thread
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| DataFusionError::Execution(format!("Failed to create runtime: {}", e)))?;

                    rt.block_on(async {
                        // 1. Create Celeborn client
                        let celeborn_config = CelebornConfig::builder()
                            .app_id(&config_clone.app_id)
                            .master_endpoints(config_clone.master_endpoints.clone())
                            .build()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let client = ExecutorShuffleClient::new(celeborn_config);

                        // 2. Setup connection to LifecycleManager
                        client
                            .setup_lifecycle_manager_ref(
                                &config_clone.lifecycle_manager_host,
                                config_clone.lifecycle_manager_port,
                            )
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        // 3. Open Partition Stream
                        eprintln!(
                            "[CELEBORN-DEBUG] CelebornShuffleStream reading partition: shuffle_id={}, partition_id={}, attempt={}, maps={}..{}",
                            config_clone.shuffle_id, config_clone.partition_id, config_clone.attempt_number, config_clone.start_map_index, config_clone.end_map_index
                        );
                        let mut input_stream = client
                            .read_partition(
                                config_clone.shuffle_id,
                                config_clone.partition_id,
                                config_clone.attempt_number,
                                config_clone.start_map_index,
                                config_clone.end_map_index,
                            )
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        eprintln!(
                            "[CELEBORN-DEBUG] Successfully opened stream for shuffle_id={}, partition_id={}",
                            config_clone.shuffle_id, config_clone.partition_id
                        );

                        // 4. Read Loop
                        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
                        let mut accumulated_data = Vec::new();
                        let mut bytes_fetched = 0usize;
                        let mut loop_count = 0;

                        loop {
                            let read_future = input_stream.read(&mut buffer);
                            // Keep the timeout logic
                            let bytes_read = match tokio::time::timeout(Duration::from_secs(60), read_future).await {
                                Ok(result) => result.map_err(|e| DataFusionError::External(Box::new(e)))?,
                                Err(_) => {
                                    eprintln!(
                                        "[CELEBORN-ERROR] Read timeout after 60s: shuffle_id={}, partition_id={}, bytes_fetched={}",
                                        config_clone.shuffle_id, config_clone.partition_id, bytes_fetched
                                    );
                                    return Err(DataFusionError::Execution("Read timeout from Celeborn".to_string()));
                                }
                            };

                            if bytes_read == 0 {
                                eprintln!(
                                    "[CELEBORN-DEBUG] Finished reading partition: shuffle_id={}, partition_id={}, total_bytes={}",
                                    config_clone.shuffle_id, config_clone.partition_id, bytes_fetched
                                );
                                break;
                            }

                            if loop_count % 100 == 0 {
                                eprintln!(
                                    "[CELEBORN-DEBUG] Reading partition: shuffle_id={}, partition_id={}, bytes_read_this_chunk={}, total_fetched={}",
                                    config_clone.shuffle_id, config_clone.partition_id, bytes_read, bytes_fetched + bytes_read
                                );
                            }
                            loop_count += 1;

                            accumulated_data.extend_from_slice(&buffer[..bytes_read]);
                            bytes_fetched += bytes_read;
                        }

                        // We can return the client and data.
                        // Note: The client is created in this local runtime. If we move it out,
                        // we must ensure it doesn't depend on the runtime being active.
                        // ExecutorShuffleClient should be Send + Sync and independent of runtime once created (hopefully).
                        Ok((client, accumulated_data, bytes_fetched))
                    })
                })
                .join()
                .unwrap_or_else(|e| Err(DataFusionError::Execution(format!("Thread panicked: {:?}", e))))
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("Join error: {}", e)))??;

            // Unpack results
            let (client, accumulated_data, bytes_fetched) = init_result;

            // 5. Decode Batches (CPU intensive, can be done in main runtime or here, doesn't matter much)
            let mut buffered_batches = Vec::new();
            for (_header, data) in BatchIterator::new(&accumulated_data) {
                if !data.is_empty() {
                    let batch = read_ipc_compressed(data)?;
                    buffered_batches.push(batch);
                }
            }
            let batches_count = buffered_batches.len();

            Ok(InitializedData {
                client: Arc::new(client),
                buffered_batches,
                bytes_fetched,
                batches_count,
            })
        })
    }
}

impl Stream for CelebornShuffleStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Finished => {
                    return Poll::Ready(None);
                }

                StreamState::Uninitialized => {
                    // Start initialization
                    let config = self.config.clone();
                    let fut = Self::create_init_future(config);
                    self.state = StreamState::Initializing(fut);
                    // Continue to poll the future
                }

                StreamState::Initializing(fut) => {
                    match fut.poll_unpin(cx) {
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(data)) => {
                            // Record metrics
                            self.metrics.bytes_fetched.add(data.bytes_fetched);
                            self.metrics.batches_read.add(data.batches_count);

                            // Transition to Ready state
                            self.state = StreamState::Ready(ReadyState {
                                client: data.client,
                                buffered_batches: data.buffered_batches,
                                current_index: 0,
                            });
                            // Continue to return batches
                        }
                        Poll::Ready(Err(e)) => {
                            self.state = StreamState::Finished;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                StreamState::Ready(ready) => {
                    if ready.current_index < ready.buffered_batches.len() {
                        let batch = ready.buffered_batches[ready.current_index].clone();
                        ready.current_index += 1;
                        self.metrics.baseline.record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    } else {
                        self.state = StreamState::Finished;
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for CelebornShuffleStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Re-export ClientManager from celeborn_client for backward compatibility
pub type CelebornClientManager = ClientManager;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_celeborn_reader_config() {
        let config = CelebornShuffleReaderConfig {
            master_endpoints: vec!["localhost:9097".to_string()],
            app_id: "test-app".to_string(),
            shuffle_id: 0,
            partition_id: 0,
            attempt_number: 0,
            start_map_index: 0,
            end_map_index: 10,
            lifecycle_manager_host: "localhost".to_string(),
            lifecycle_manager_port: 9098,
        };

        assert_eq!(config.shuffle_id, 0);
        assert_eq!(config.partition_id, 0);
    }

    #[test]
    fn test_celeborn_client_manager() {
        let manager = CelebornClientManager::new();
        assert!(manager.clients.is_empty());
    }
}
