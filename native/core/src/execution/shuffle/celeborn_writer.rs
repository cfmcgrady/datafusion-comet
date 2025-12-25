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

//! Celeborn Shuffle Writer for Apache Comet
//!
//! This module provides integration with Apache Celeborn for distributed shuffle operations.
//! It implements a shuffle writer that pushes data to Celeborn workers instead of writing
//! to local disk.

use crate::execution::shuffle::{CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use crate::execution::tracing::with_trace_async;
use arrow::compute::interleave_record_batch;
use async_trait::async_trait;
use celeborn_client::{CelebornConfig, ExecutorShuffleClient};
use dashmap::DashMap;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::{
    arrow::{array::*, datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    error::{DataFusionError, Result},
    execution::{
        context::TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_plan::{
        metrics::{
            BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
        },
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        Statistics,
    },
};
use datafusion_comet_spark_expr::hash_funcs::murmur3::create_murmur3_hashes;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use std::io::Cursor;
use std::{
    any::Any,
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tokio::time::Instant;

/// Configuration for Celeborn shuffle writer
#[derive(Debug, Clone)]
pub struct CelebornShuffleConfig {
    /// Celeborn master endpoints (e.g., ["host1:9097", "host2:9097"])
    pub master_endpoints: Vec<String>,
    /// Application ID
    pub app_id: String,
    /// Shuffle ID
    pub shuffle_id: i32,
    /// Map ID (task partition)
    pub map_id: i32,
    /// Attempt ID
    pub attempt_id: i32,
    /// Number of mappers
    pub num_mappers: i32,
    /// Number of partitions
    pub num_partitions: i32,
    /// LifecycleManager host (Driver host)
    pub lifecycle_manager_host: String,
    /// LifecycleManager port
    pub lifecycle_manager_port: i32,
}

/// The Celeborn shuffle writer operator maps each input partition to M output partitions
/// and pushes data to Celeborn workers.
#[derive(Debug)]
pub struct CelebornShuffleWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: CometPartitioning,
    /// Celeborn configuration
    celeborn_config: CelebornShuffleConfig,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for expensive-to-compute plan properties
    cache: PlanProperties,
    /// The compression codec to use when compressing shuffle blocks
    codec: CompressionCodec,
    /// Whether tracing is enabled
    tracing_enabled: bool,
}

impl CelebornShuffleWriterExec {
    /// Create a new CelebornShuffleWriterExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: CometPartitioning,
        codec: CompressionCodec,
        celeborn_config: CelebornShuffleConfig,
        tracing_enabled: bool,
    ) -> Result<Self> {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(CelebornShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            celeborn_config,
            cache,
            codec,
            tracing_enabled,
        })
    }
}

impl DisplayAs for CelebornShuffleWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CelebornShuffleWriterExec: partitioning={:?}, compression={:?}, shuffle_id={}",
                    self.partitioning, self.codec, self.celeborn_config.shuffle_id
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CelebornShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "CelebornShuffleWriterExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(CelebornShuffleWriterExec::try_new(
                Arc::clone(&children[0]),
                self.partitioning.clone(),
                self.codec.clone(),
                self.celeborn_config.clone(),
                self.tracing_enabled,
            )?)),
            _ => panic!("CelebornShuffleWriterExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let metrics = CelebornShuffleMetrics::new(&self.metrics, 0);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                celeborn_shuffle(
                    input,
                    partition,
                    self.partitioning.clone(),
                    self.celeborn_config.clone(),
                    metrics,
                    context,
                    self.codec.clone(),
                    self.tracing_enabled,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }
}

/// Metrics for Celeborn shuffle operations
struct CelebornShuffleMetrics {
    /// Base metrics
    baseline: BaselineMetrics,
    /// Time to perform repartitioning
    repart_time: Time,
    /// Time encoding batches to IPC format
    encode_time: Time,
    /// Time spent pushing data to Celeborn workers
    push_time: Time,
    /// Number of input batches
    input_batches: Count,
    /// Total bytes pushed to Celeborn
    bytes_pushed: Count,
    /// Data size before compression
    data_size: Count,
}

impl CelebornShuffleMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            repart_time: MetricBuilder::new(metrics).subset_time("repart_time", partition),
            encode_time: MetricBuilder::new(metrics).subset_time("encode_time", partition),
            push_time: MetricBuilder::new(metrics).subset_time("push_time", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            bytes_pushed: MetricBuilder::new(metrics).counter("bytes_pushed", partition),
            data_size: MetricBuilder::new(metrics).counter("data_size", partition),
        }
    }
}

/// Execute Celeborn shuffle
#[allow(clippy::too_many_arguments)]
async fn celeborn_shuffle(
    mut input: SendableRecordBatchStream,
    partition: usize,
    partitioning: CometPartitioning,
    celeborn_config: CelebornShuffleConfig,
    metrics: CelebornShuffleMetrics,
    context: Arc<TaskContext>,
    codec: CompressionCodec,
    tracing_enabled: bool,
) -> Result<SendableRecordBatchStream> {
    with_trace_async("celeborn_shuffle", tracing_enabled, || async {
        let schema = input.schema();

        let mut repartitioner = CelebornShuffleRepartitioner::try_new(
            partition,
            Arc::clone(&schema),
            partitioning,
            celeborn_config,
            metrics,
            context.runtime_env(),
            context.session_config().batch_size(),
            codec,
            tracing_enabled,
        )
        .await?;

        while let Some(batch) = input.next().await {
            repartitioner.insert_batch(batch?).await?;
        }

        repartitioner.finish().await?;

        // Celeborn shuffle writer always has empty output
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(&schema))) as SendableRecordBatchStream)
    })
    .await
}

/// Celeborn shuffle repartitioner that pushes data to Celeborn workers
struct CelebornShuffleRepartitioner {
    /// Celeborn client
    client: Arc<ExecutorShuffleClient>,
    /// Shuffle ID
    shuffle_id: i32,
    /// Map ID
    map_id: i32,
    /// Attempt ID
    attempt_id: i32,
    /// Number of mappers
    num_mappers: i32,
    /// Buffered batches
    buffered_batches: Vec<RecordBatch>,
    /// Partition indices for each buffered batch
    partition_indices: Vec<Vec<(u32, u32)>>,
    /// Shuffle block writer for encoding
    shuffle_block_writer: ShuffleBlockWriter,
    /// Partitioning scheme
    partitioning: CometPartitioning,
    /// Runtime environment
    runtime: Arc<RuntimeEnv>,
    /// Metrics
    metrics: CelebornShuffleMetrics,
    /// Scratch space for computing partition indices
    scratch: ScratchSpace,
    /// Configured batch size
    batch_size: usize,
    /// Memory reservation
    reservation: MemoryReservation,
    /// Whether tracing is enabled
    tracing_enabled: bool,
    /// Partition buffers for accumulating data before push
    partition_buffers: DashMap<i32, Vec<u8>>,
    /// Buffer size threshold for pushing to Celeborn
    push_buffer_size: usize,
}

#[derive(Default)]
struct ScratchSpace {
    hashes_buf: Vec<u32>,
    partition_ids: Vec<u32>,
    partition_row_indices: Vec<u32>,
    partition_starts: Vec<u32>,
}

impl CelebornShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        partition: usize,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        config: CelebornShuffleConfig,
        metrics: CelebornShuffleMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        tracing_enabled: bool,
    ) -> Result<Self> {
        let num_output_partitions = partitioning.partition_count();

        // Initialize scratch space
        let scratch = ScratchSpace {
            hashes_buf: match partitioning {
                CometPartitioning::Hash(_, _) => vec![0; batch_size],
                _ => vec![],
            },
            partition_ids: vec![0; batch_size],
            partition_row_indices: vec![0; batch_size],
            partition_starts: vec![0; num_output_partitions + 1],
        };

        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec)?;

        // Create Celeborn client configuration
        let celeborn_config = CelebornConfig::builder()
            .app_id(&config.app_id)
            .master_endpoints(config.master_endpoints.clone())
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Create executor shuffle client
        let client = ExecutorShuffleClient::new(celeborn_config);

        // Setup connection to LifecycleManager in Driver
        client
            .setup_lifecycle_manager_ref(
                &config.lifecycle_manager_host,
                config.lifecycle_manager_port,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Register shuffle
        client
            .register_shuffle(config.shuffle_id, config.num_mappers, config.num_partitions)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let reservation = MemoryConsumer::new(format!("CelebornShuffleRepartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            client: Arc::new(client),
            shuffle_id: config.shuffle_id,
            map_id: config.map_id,
            attempt_id: config.attempt_id,
            num_mappers: config.num_mappers,
            buffered_batches: vec![],
            partition_indices: vec![vec![]; num_output_partitions],
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            scratch,
            batch_size,
            reservation,
            tracing_enabled,
            partition_buffers: DashMap::new(),
            push_buffer_size: 4 * 1024 * 1024, // 4MB default buffer size
        })
    }

    /// Insert a batch into the repartitioner
    pub async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        with_trace_async("celeborn_insert_batch", self.tracing_enabled, || async {
            let start_time = Instant::now();
            let mut start = 0;
            while start < batch.num_rows() {
                let end = (start + self.batch_size).min(batch.num_rows());
                let batch = batch.slice(start, end - start);
                self.partitioning_batch(batch).await?;
                start = end;
            }
            self.metrics.input_batches.add(1);
            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());
            Ok(())
        })
        .await
    }

    /// Partition a batch and buffer data for each partition
    async fn partitioning_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }

        self.metrics.data_size.add(input.get_array_memory_size());
        self.metrics.baseline.record_output(input.num_rows());

        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, partition_row_indices): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    let num_rows = arrays[0].len();
                    let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                    hashes_buf.fill(42_u32);

                    {
                        let partition_ids = &mut scratch.partition_ids[..num_rows];
                        create_murmur3_hashes(&arrays, hashes_buf)?
                            .iter()
                            .enumerate()
                            .for_each(|(idx, hash)| {
                                partition_ids[idx] = pmod(*hash, *num_output_partitions) as u32;
                            });
                    }

                    map_partition_ids_to_starts_and_indices(
                        &mut scratch,
                        *num_output_partitions,
                        num_rows,
                    );

                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        &scratch.partition_starts,
                        &scratch.partition_row_indices,
                    ))
                }?;

                // Push partitioned data to Celeborn
                self.push_partitioned_data(&input, partition_row_indices, partition_starts)
                    .await?;

                self.scratch = scratch;
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme for Celeborn: {other:?}"
                )));
            }
        }
        Ok(())
    }

    /// Push partitioned data to Celeborn workers
    async fn push_partitioned_data(
        &mut self,
        input: &RecordBatch,
        partition_row_indices: &[u32],
        partition_starts: &[u32],
    ) -> Result<()> {
        let num_partitions = partition_starts.len() - 1;

        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let row_indices = &partition_row_indices[start as usize..end as usize];

            // Create indices for interleave
            let indices: Vec<(usize, usize)> = row_indices
                .iter()
                .map(|&idx| (0usize, idx as usize))
                .collect();

            // Extract rows for this partition
            let partition_batch = interleave_record_batch(&[input], &indices)?;

            // Encode batch to IPC format
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            self.shuffle_block_writer
                .write_batch(&partition_batch, &mut cursor, &self.metrics.encode_time)?;

            // Push to Celeborn
            let mut push_timer = self.metrics.push_time.timer();
            self.client
                .push_data(
                    self.shuffle_id,
                    self.map_id,
                    self.attempt_id,
                    partition_id as i32,
                    &buffer,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            push_timer.stop();

            self.metrics.bytes_pushed.add(buffer.len());
        }

        Ok(())
    }

    /// Finish the shuffle and signal mapper end
    pub async fn finish(&mut self) -> Result<()> {
        with_trace_async("celeborn_finish", self.tracing_enabled, || async {
            // Signal mapper end to Celeborn
            self.client
                .mapper_end(
                    self.shuffle_id,
                    self.map_id,
                    self.attempt_id,
                    self.num_mappers,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            self.reservation.free();
            Ok(())
        })
        .await
    }
}

/// Map partition IDs to partition starts and row indices
fn map_partition_ids_to_starts_and_indices(
    scratch: &mut ScratchSpace,
    num_output_partitions: usize,
    num_rows: usize,
) {
    let partition_ids = &mut scratch.partition_ids[..num_rows];

    let partition_counters = &mut scratch.partition_starts;
    partition_counters.resize(num_output_partitions + 1, 0);
    partition_counters.fill(0);
    partition_ids
        .iter()
        .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

    let partition_ends = partition_counters;
    let mut accum = 0;
    partition_ends.iter_mut().for_each(|v| {
        *v += accum;
        accum = *v;
    });

    let partition_row_indices = &mut scratch.partition_row_indices;
    partition_row_indices.resize(num_rows, 0);
    for (index, partition_id) in partition_ids.iter().enumerate().rev() {
        partition_ends[*partition_id as usize] -= 1;
        let end = partition_ends[*partition_id as usize];
        partition_row_indices[end as usize] = index as u32;
    }
}

/// Compute partition ID using positive modulo (same as Spark)
fn pmod(hash: u32, n: usize) -> usize {
    let h = hash as i32;
    let n = n as i32;
    ((h % n + n) % n) as usize
}

impl Debug for CelebornShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelebornShuffleRepartitioner")
            .field("shuffle_id", &self.shuffle_id)
            .field("map_id", &self.map_id)
            .field("attempt_id", &self.attempt_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pmod() {
        assert_eq!(pmod(0, 10), 0);
        assert_eq!(pmod(5, 10), 5);
        assert_eq!(pmod(10, 10), 0);
        assert_eq!(pmod(15, 10), 5);
        // Test negative hash values (when cast to i32)
        assert_eq!(pmod(u32::MAX, 10), 5); // -1 % 10 = -1, (-1 + 10) % 10 = 9
    }
}
