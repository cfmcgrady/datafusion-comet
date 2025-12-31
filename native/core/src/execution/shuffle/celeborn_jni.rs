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

//! JNI APIs for Celeborn shuffle integration
//!
//! This module provides JNI functions that can be called from Java/Scala
//! to interact with Celeborn shuffle operations.

use crate::errors::{try_unwrap_or_throw, CometError};
use crate::execution::jni_api::get_runtime;
use celeborn_client::{ClientManager, CompressionCodec, ExecutorShuffleClient};
use jni::{
    objects::{JClass, JObject, JObjectArray, JString},
    sys::{jboolean, jint, jlong, JNI_FALSE, JNI_TRUE},
    JNIEnv,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// Global Celeborn client manager for reusing connections
static CELEBORN_CLIENT_MANAGER: Lazy<ClientManager> = Lazy::new(ClientManager::new);

/// Celeborn shuffle client context stored across JNI calls
struct CelebornContext {
    client: Arc<ExecutorShuffleClient>,
    app_id: String,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    num_mappers: i32,
    num_partitions: i32,
}

/// Parse compression codec from string.
/// Supported values: "none", "lz4", "zstd" (case-insensitive)
fn parse_compression_codec(codec_str: &str) -> CompressionCodec {
    match codec_str.to_lowercase().as_str() {
        "none" => CompressionCodec::None,
        "lz4" => CompressionCodec::Lz4,
        "zstd" => CompressionCodec::Zstd,
        _ => CompressionCodec::Zstd, // Default to Zstd
    }
}

/// Create a new Celeborn shuffle client and return a handle
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_createCelebornClient(
    e: JNIEnv,
    _class: JClass,
    app_id: JString,
    master_endpoints: JObjectArray,
    lifecycle_manager_host: JString,
    lifecycle_manager_port: jint,
    shuffle_id: jint,
    map_id: jint,
    attempt_id: jint,
    num_mappers: jint,
    num_partitions: jint,
    compression_codec: JString,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        eprintln!("[CELEBORN-JNI] createCelebornClient called");
        
        let app_id: String = env.get_string(&app_id)?.into();
        let lm_host: String = env.get_string(&lifecycle_manager_host)?.into();
        let codec_str: String = env.get_string(&compression_codec)?.into();
        let codec = parse_compression_codec(&codec_str);
        eprintln!("[CELEBORN-JNI] app_id={}, lm_host={}, lm_port={}, compression={:?}",
            app_id, lm_host, lifecycle_manager_port, codec);

        // Parse master endpoints
        let num_endpoints = env.get_array_length(&master_endpoints)?;
        let mut endpoints = Vec::with_capacity(num_endpoints as usize);
        for i in 0..num_endpoints {
            let endpoint = env.get_object_array_element(&master_endpoints, i)?;
            let endpoint_str: JString = endpoint.into();
            let endpoint: String = env.get_string(&endpoint_str)?.into();
            endpoints.push(endpoint);
        }

        // Create client using the runtime with compression
        let runtime = get_runtime();
        let client = runtime.block_on(async {
            CELEBORN_CLIENT_MANAGER
                .get_or_create_client_with_compression(
                    &app_id, endpoints, &lm_host, lifecycle_manager_port, codec
                )
                .await
                .map_err(|e| CometError::Internal(format!("Failed to create Celeborn client: {}", e)))
        })?;

        // Register shuffle
        eprintln!("[CELEBORN-JNI] Registering shuffle {} with {} mappers and {} partitions", shuffle_id, num_mappers, num_partitions);
        runtime.block_on(async {
            client
                .register_shuffle(shuffle_id, num_mappers, num_partitions)
                .await
                .map_err(|e| {
                    eprintln!("[CELEBORN-JNI] Failed to register shuffle: {}", e);
                    CometError::Internal(format!("Failed to register shuffle: {}", e))
                })
        })?;
        eprintln!("[CELEBORN-JNI] Shuffle {} registered successfully", shuffle_id);

        // Create context
        let context = Box::new(CelebornContext {
            client,
            app_id,
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers,
            num_partitions,
        });

        Ok(Box::into_raw(context) as jlong)
    })
}

/// Push data to Celeborn for a specific partition
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_celebornPushData(
    e: JNIEnv,
    _class: JClass,
    context_handle: jlong,
    partition_id: jint,
    data: jni::objects::JByteArray,
) -> jboolean {
    try_unwrap_or_throw(&e, |mut env| {
        let context = &*(context_handle as *const CelebornContext);
        let data_bytes = env.convert_byte_array(data)?;

        // Debug: write to file for debugging
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/celeborn_jni_debug.log")
        {
            let _ = writeln!(file, "[CELEBORN-JNI] celebornPushData: shuffle={}, map={}, attempt={}, partition={}, data_len={}",
                context.shuffle_id, context.map_id, context.attempt_id, partition_id, data_bytes.len());
            
            // Log first 32 bytes of data for debugging
            let preview: Vec<u8> = data_bytes.iter().take(32).cloned().collect();
            let _ = writeln!(file, "[CELEBORN-JNI] Data preview (first 32 bytes): {:02x?}", preview);
        }

        let runtime = get_runtime();
        runtime.block_on(async {
            context
                .client
                .push_data(
                    context.shuffle_id,
                    context.map_id,
                    context.attempt_id,
                    partition_id,
                    &data_bytes,
                )
                .await
                .map_err(|e| {
                    eprintln!("[CELEBORN-JNI] Failed to push data: {}", e);
                    CometError::Internal(format!("Failed to push data: {}", e))
                })
        })?;

        eprintln!("[CELEBORN-JNI] Push data successful for partition {}", partition_id);
        Ok(JNI_TRUE)
    })
}

/// Signal mapper end to Celeborn
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_celebornMapperEnd(
    e: JNIEnv,
    _class: JClass,
    context_handle: jlong,
) -> jboolean {
    try_unwrap_or_throw(&e, |_env| {
        let context = &*(context_handle as *const CelebornContext);

        let runtime = get_runtime();
        let result = runtime.block_on(async {
            context
                .client
                .mapper_end(
                    context.shuffle_id,
                    context.map_id,
                    context.attempt_id,
                    context.num_mappers,
                )
                .await
                .map_err(|e| CometError::Internal(format!("Failed to signal mapper end: {}", e)))
        })?;

        Ok(if result { JNI_TRUE } else { JNI_FALSE })
    })
}

/// Release Celeborn client context
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_releaseCelebornClient(
    e: JNIEnv,
    _class: JClass,
    context_handle: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        if context_handle != 0 {
            let context = Box::from_raw(context_handle as *mut CelebornContext);
            // Context will be dropped here, cleaning up resources
            drop(context);
        }
        Ok(())
    })
}

/// Cleanup shuffle from Celeborn
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_celebornCleanupShuffle(
    e: JNIEnv,
    _class: JClass,
    context_handle: jlong,
) -> jboolean {
    try_unwrap_or_throw(&e, |_env| {
        let context = &*(context_handle as *const CelebornContext);
        let result = context.client.cleanup_shuffle(context.shuffle_id);
        Ok(if result { JNI_TRUE } else { JNI_FALSE })
    })
}

/// Get partition location from Celeborn
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_celebornGetPartitionLocation(
    e: JNIEnv,
    _class: JClass,
    context_handle: jlong,
    partition_id: jint,
) -> jni::sys::jobjectArray {
    try_unwrap_or_throw(&e, |mut env| {
        let context = &*(context_handle as *const CelebornContext);

        let locations = context
            .client
            .get_partition_location(context.shuffle_id, partition_id)
            .map_err(|e| CometError::Internal(format!("Failed to get partition location: {}", e)))?;

        // Create string array for locations
        let string_class = env.find_class("java/lang/String")?;
        let result = env.new_object_array(locations.len() as i32, string_class, JObject::null())?;

        for (i, location) in locations.iter().enumerate() {
            let addr = env.new_string(location.push_address())?;
            env.set_object_array_element(&result, i as i32, addr)?;
        }

        Ok(result.into_raw())
    })
}

/// Clear all cached Celeborn clients
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_Native_celebornClearClients(e: JNIEnv, _class: JClass) {
    try_unwrap_or_throw(&e, |_env| {
        CELEBORN_CLIENT_MANAGER.clear();
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_manager_creation() {
        let manager = CelebornClientManager::new();
        assert!(manager.clients.is_empty());
    }
}
