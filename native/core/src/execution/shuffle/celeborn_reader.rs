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

use crate::execution::shuffle::read_ipc_compressed;
use async_trait::async_trait;
use celeborn_client::{CelebornConfig, ExecutorShuffleClient};
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
use futures::Stream;
use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
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

/// Stream that reads shuffle data from Celeborn
struct CelebornShuffleStream {
    /// Schema
    schema: SchemaRef,
    /// Configuration
    config: CelebornShuffleReaderConfig,
    /// Metrics
    metrics: CelebornReaderMetrics,
    /// Client (lazily initialized)
    client: Option<Arc<ExecutorShuffleClient>>,
    /// Whether the stream is finished
    finished: bool,
    /// Buffered batches from Celeborn
    buffered_batches: Vec<RecordBatch>,
    /// Current index in buffered batches
    current_index: usize,
    /// Whether initialization is complete
    initialized: bool,
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
            client: None,
            finished: false,
            buffered_batches: Vec::new(),
            current_index: 0,
            initialized: false,
        }
    }

    /// Initialize the Celeborn client and fetch data
    async fn initialize(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // Create Celeborn client
        let celeborn_config = CelebornConfig::builder()
            .app_id(&self.config.app_id)
            .master_endpoints(self.config.master_endpoints.clone())
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let client = ExecutorShuffleClient::new(celeborn_config);

        // Setup connection to LifecycleManager
        client
            .setup_lifecycle_manager_ref(
                &self.config.lifecycle_manager_host,
                self.config.lifecycle_manager_port,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Read partition data
        let mut fetch_timer = self.metrics.fetch_time.timer();
        let input_stream = client
            .read_partition(
                self.config.shuffle_id,
                self.config.partition_id,
                self.config.attempt_number,
                self.config.start_map_index,
                self.config.end_map_index,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        fetch_timer.stop();

        // Read all data from the stream and decode
        let mut decode_timer = self.metrics.decode_time.timer();
        
        // CelebornInputStream uses read() method, not Stream trait
        // Read data in chunks until EOF
        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
        let mut accumulated_data = Vec::new();
        
        loop {
            let bytes_read = input_stream
                .read(&mut buffer)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            
            if bytes_read == 0 {
                break;
            }
            
            accumulated_data.extend_from_slice(&buffer[..bytes_read]);
            self.metrics.bytes_fetched.add(bytes_read);
        }
        
        // Process accumulated data - each batch has a header
        // Format: mapId (4) + attemptId (4) + batchId (4) + compressedSize (4) + data
        let mut offset = 0;
        while offset + 16 <= accumulated_data.len() {
            // Read header
            let compressed_size = i32::from_be_bytes([
                accumulated_data[offset + 12],
                accumulated_data[offset + 13],
                accumulated_data[offset + 14],
                accumulated_data[offset + 15],
            ]) as usize;
            
            let data_start = offset + 16;
            let data_end = data_start + compressed_size;
            
            if data_end > accumulated_data.len() {
                break;
            }
            
            let data = &accumulated_data[data_start..data_end];
            if !data.is_empty() {
                let batch = read_ipc_compressed(data)?;
                self.buffered_batches.push(batch);
                self.metrics.batches_read.add(1);
            }
            
            offset = data_end;
        }
        decode_timer.stop();

        self.client = Some(Arc::new(client));
        self.initialized = true;

        Ok(())
    }
}

impl Stream for CelebornShuffleStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // Initialize if needed (this is a simplified sync approach)
        // In production, this should be properly async
        if !self.initialized {
            // For now, we'll use a blocking approach
            // TODO: Implement proper async initialization
            let rt = tokio::runtime::Handle::current();
            match rt.block_on(self.initialize()) {
                Ok(()) => {}
                Err(e) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // Return buffered batches
        if self.current_index < self.buffered_batches.len() {
            let batch = self.buffered_batches[self.current_index].clone();
            self.current_index += 1;
            self.metrics.baseline.record_output(batch.num_rows());
            Poll::Ready(Some(Ok(batch)))
        } else {
            self.finished = true;
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for CelebornShuffleStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Celeborn shuffle client manager for reusing connections
pub struct CelebornClientManager {
    /// Cached clients by app_id
    clients: dashmap::DashMap<String, Arc<ExecutorShuffleClient>>,
}

impl CelebornClientManager {
    /// Create a new client manager
    pub fn new() -> Self {
        Self {
            clients: dashmap::DashMap::new(),
        }
    }

    /// Get or create a client for the given configuration
    pub async fn get_or_create_client(
        &self,
        app_id: &str,
        master_endpoints: Vec<String>,
        lifecycle_manager_host: &str,
        lifecycle_manager_port: i32,
    ) -> Result<Arc<ExecutorShuffleClient>> {
        if let Some(client) = self.clients.get(app_id) {
            return Ok(Arc::clone(&client));
        }

        // Create new client
        // Note: Disable compression in Rust client because the LZ4 format used by lz4_flex
        // is not compatible with Celeborn's Java LZ4 format (which includes magic, checksum, etc.)
        // The Java side will handle compression/decompression.
        let config = CelebornConfig::builder()
            .app_id(app_id)
            .master_endpoints(master_endpoints)
            .compression_codec(celeborn_client::CompressionCodec::None)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let client = ExecutorShuffleClient::new(config);

        client
            .setup_lifecycle_manager_ref(lifecycle_manager_host, lifecycle_manager_port)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let client = Arc::new(client);
        self.clients.insert(app_id.to_string(), Arc::clone(&client));

        Ok(client)
    }

    /// Remove a client
    pub fn remove_client(&self, app_id: &str) {
        self.clients.remove(app_id);
    }

    /// Clear all clients
    pub fn clear(&self) {
        self.clients.clear();
    }
}

impl Default for CelebornClientManager {
    fn default() -> Self {
        Self::new()
    }
}

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
