#ifndef  __CTRIP_SWAP_SERVER_H__
#define  __CTRIP_SWAP_SERVER_H__
/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>

/* swap datatype flags*/
#define CMD_SWAP_DATATYPE_KEYSPACE (1ULL<<40)
#define CMD_SWAP_DATATYPE_STRING (1ULL<<41)
#define CMD_SWAP_DATATYPE_HASH (1ULL<<42)
#define CMD_SWAP_DATATYPE_SET (1ULL<<43)
#define CMD_SWAP_DATATYPE_ZSET (1ULL<<44)
#define CMD_SWAP_DATATYPE_LIST (1ULL<<45)
#define CMD_SWAP_DATATYPE_BITMAP (1ULL<<46)

/* CHECK: CLIENT_REPL_RDBONLY is the last CLIENT_xx flag */
#define CLIENT_SWAPPING (1ULL<<43) /* The client is waiting swap. */
#define CLIENT_SWAP_UNLOCKING (1ULL<<44) /* Client is releasing swap lock. */
#define CLIENT_CTRIP_MONITOR (1ULL<<45) /* Client for ctrip monitor. */
#define CLIENT_SWAP_REWINDING (1ULL<<46) /* The client is waiting rewind. */
#define CLIENT_SWAP_DISCARD_CACHED_MASTER (1ULL<<47) /* The client will not be saved as cached_master. */
#define CLIENT_SWAP_SHIFT_REPL_ID (1ULL<<48) /* shift repl id when this client (drainning master) drained. */
#define CLIENT_SWAP_DONT_RECONNECT_MASTER (1ULL<<49) /* shift repl id when this client (drainning master) drained. */

/* CHECK: SLAVE_CAPA_PSYNC2 is the last SLAVE_CAPA_xx flag */
#define SLAVE_CAPA_RORDB (1<<2) /* Can parse RORDB format. */
#define SLAVE_CAPA_SWAP_INFO (1<<3) /* Supports SWAP.INFO cmd. */

struct getKeyRequestsResult;
typedef void (*dataSwapFinishedCallback)(void *ctx, int action, char *rawkey, char *rawval, void *pd);

#define REFCOUNT_BITS 28

#define SWAP_REDIS_OBJECT \
    unsigned dirty_meta:1;     /* set to 1 if rocksdb and redis meta differs */ \
    unsigned dirty_data:1;     /* set to 1 if rocksdb and redis data differs */ \
    unsigned persistent:1; \
    unsigned persist_keep:1;   /* set to 1 if persist key should keep value in memory */ \
    int refcount:REFCOUNT_BITS;

#define SWAP_REDIS_DB \
    dict *meta;                 /* meta for rocksdb subkeys of big object. */ \
    dict *dirty_subkeys;        /* dirty subkeys. */ \
    list *evict_asap;           /* keys to be evicted asap. */ \
    long long cold_keys;        /* # of cold keys. */ \
    sds randomkey_nextseek;     /* nextseek for randomkey command */ \
    struct scanExpire *scan_expire; /* scan expire related */ \
    struct coldFilter *cold_filter; /* cold keys filter: absent cache & cuckoo filter. */

/* TODO: remove */
#define CLIENT_HOLD_MODE_CMD 0  /* Hold all key in cmd if any key in cmd needs swap. */
#define CLIENT_HOLD_MODE_EVICT 1  /* Hold key if needs swap. */
#define CLIENT_HOLD_MODE_REPL 2 /* Hold all key no matter what. */

typedef void (*voidfuncptr)(void);

#define SWAP_CLIENT \
    int keyrequests_count; \
    struct swapCmdTrace *swap_cmd;  \
    long swap_duration; /* microseconds used in swap */ \
    int swap_result;  \
    voidfuncptr client_swap_finished_cb;  \
    void *client_swap_finished_pd;  \
    int client_hold_mode; /* indicates how client should hold key */  \
    int CLIENT_DEFERED_CLOSING; \
    int CLIENT_REPL_SWAPPING; \
    long long cmd_reploff; /* Command replication offset when dispatch if this is a repl worker */  \
    struct client *repl_client; /* Master or peer client if this is a repl worker */  \
    list *swap_locks; /* swap locks */  \
    struct metaScanResult *swap_metas;  \
    int swap_errcode; \
    struct argRewrites *swap_arg_rewrites;  \
    int rate_limit_event_id; /* add time event when rate limit */

#define SWAP_TYPES_FORWARD 5
typedef struct swapBatchLimitsConfig {
    int count;
    unsigned long long mem;
} swapBatchLimitsConfig;

#define SWAP_REDIS_SERVER_ \
    list *clients_to_free;      /* Clients to close when swaps finished. */ \
    int swap_slow_expire_effort;    /* From -10 to 10, default -5, swap slow expire effort */ \
    int jemalloc_max_bg_threads;    /* Maximum jemalloc background threads num */ \
    time_t swap_lastsave;           /* Unix time of last successful save for ror */ \
    size_t swap_rdb_size;           /* Size of last successful save */ \
    time_t maxmemory_updated_time_last; \
    unsigned long long maxmemory_scale_from; \
    unsigned long long maxmemory_scaledown_rate; /* Number of bytes actually scale down maxmemory every seconds */ \
    unsigned long long swap_max_db_size;     /* Max number of disk bytes to use */ \
    /* accept ignore */ \
    int ctrip_ignore_accept;  \
    int ctrip_monitor_port; \
    int ctrip_monitorfd;  \
		/* rocksdb engine */  \
    int rocksdb_disk_error; \
    int rocksdb_disk_error_since; \
    int swap_rocksdb_stats_collect_interval_ms; \
    struct rocks *rocks;  \
    struct rocksdb_checkpoint_t* rocksdb_checkpoint;  \
    sds rocksdb_checkpoint_dir; \
    sds rocksdb_rdb_checkpoint_dir; /* checkpoint dir use for rdb saved */  \
    struct rocksdbInternalStats *rocksdb_internal_stats;  \
    redisAtomic size_t inflight_snapshot; \
    struct rocksdbUtilTaskManager* util_task_manager; \
    /* swap threads */  \
    int swap_threads_num; \
    int swap_defer_thread_idx;  \
    int swap_util_thread_idx; \
    int total_swap_threads_num; /* swap_threads_num + extra_swap_threads_num */ \
    struct swapThread *swap_threads;  \
    /* async */ \
    struct asyncCompleteQueue *CQ;  \
    /* parallel sync */ \
    struct parallelSync *parallel_sync; \
    unsigned long long rocksdb_disk_used; /* rocksd disk usage bytes, updated every 1 minute. */  \
		/* swaps */ \
    client **evict_clients; /* array of evict clients (one for each db). */ \
    client **expire_clients; /* array of rocks expire clients (one for each db). */ \
    client **scan_expire_clients; /* array of expire scan clients (one for each db). */ \
    client **ttl_clients; /* array of expire scan clients (one for each db). */ \
    client **load_clients;  \
    client *mutex_client; /* exec op needed global swap lock */ \
    struct rorStat *ror_stats;  \
    struct swapHitStat *swap_hit_stats; \
    struct swapDebugInfo *swap_debug_info;  \
    int swap_debug_evict_keys; /* num of keys to evict before calling cmd. */ \
    uint64_t req_submitted; /* whether request already submitted or not, request will be executed with global swap lock */ \
    /* swap rate limiting */  \
    redisAtomic size_t swap_inprogress_batch; /* swap request inprogress batch */ \
    redisAtomic size_t swap_inprogress_count; /* swap request inprogress count */ \
    redisAtomic size_t swap_inprogress_memory;  /* swap consumed memory in bytes */ \
    redisAtomic size_t swap_error_count;  /* swap error count */  \
    int swap_debug_rio_delay_micro; /* sleep swap_debug_rio_delay microsencods to simulate ssd delay. */  \
    int swap_debug_swapout_notify_delay_micro; /* sleep swap_debug_swapout_notify_delay microsencods  \
                                        to simulate notify queue blocked after swap out */  \
    int swap_debug_before_exec_swap_delay_micro; /* sleep swap_debug_before_exec_swap_delay microsencods before exec swap request */  \
    int swap_debug_init_rocksdb_delay_micro; /* sleep swap_debug_init_rocksdb_delay microsencods before init rocksdb */ \
    int swap_debug_rio_error; /* mock rio error */  \
    int swap_debug_rio_error_action;  \
    int swap_debug_trace_latency; \
    int swap_debug_bgsave_metalen_addition; \
    int swap_debug_compaction_filter_delay_micro; \
    int swap_debug_rdb_key_save_delay_micro;  \
    int swap_rordb_load_incremental_fsync;  \
    /* repl swap */ \
    int repl_workers;   /* num of repl worker clients */ \
    list *repl_worker_clients_free; /* free clients for repl(slaveof & peerof) swap. */ \
    list *repl_worker_clients_used; /* used clients for repl swap. */ \
    list *repl_swapping_clients; /* list of repl swapping clients. */ \
    /* rdb swap */ \
    int ps_parallism_rdb;  /* parallel swap parallelism for rdb save & load. */ \
    struct ctripRdbLoadCtx *rdb_load_ctx; /* parallel swap for rdb load */ \
    int swap_bgsave_fix_metalen_mismatch; \
    int swap_child_err_pipe[2]; \
    size_t swap_child_err_nread; \
    /* request wait */ \
    struct swapLock *swap_lock; \
    /* big object */ \
    int swap_evict_step_max_subkeys; /* max subkeys evict in one step. */ \
    unsigned long long swap_evict_step_max_memory; /* max memory evict in one step. */ \
    unsigned long long swap_repl_max_rocksdb_read_bps; /* max rocksdb iterator read bps. */  \
    int64_t swap_txid; /* swap txid. */ \
    int swap_rewind_type; \
    list *swap_torewind_clients; \
    list *swap_rewinding_clients; \
    uint64_t swap_key_version; \
    size_t swap_bitmap_subkey_size; \
    redisAtomic unsigned long long swap_bitmap_switched_to_string_count; \
    redisAtomic unsigned long long swap_string_switched_to_bitmap_count; \
    int swap_rdb_bitmap_encode_enabled; \
    int swap_bitmap_subkeys_enabled; \
    /* swap eviction */ \
    int swap_evict_inprogress_limit;  \
    int swap_evict_inprogress_growth_rate;  \
    int swap_evict_loop_check_interval; \
    struct swapEvictionCtx *swap_eviction_ctx;  \
    int swap_load_inprogress_count; \
    int swap_load_paused; \
    size_t swap_load_err_cnt; \
    /* swap scan session */ \
    struct swapScanSessions *swap_scan_sessions;  \
    int swap_scan_session_bits; \
    int swap_scan_session_max_idle_seconds; \
    /* rocksdb configs */ \
    unsigned long long rocksdb_meta_block_cache_size; \
    unsigned long long rocksdb_data_block_cache_size; \
    int rocksdb_max_open_files; \
    int rocksdb_WAL_ttl_seconds;  \
    int rocksdb_WAL_size_limit_MB;  \
    unsigned long long rocksdb_data_write_buffer_size;  \
    unsigned long long rocksdb_meta_write_buffer_size;  \
    unsigned long long rocksdb_data_target_file_size_base;  \
    unsigned long long rocksdb_meta_target_file_size_base;  \
    unsigned long long rocksdb_ratelimiter_rate_per_sec; \
    unsigned long long rocksdb_bytes_per_sync; \
    unsigned long long rocksdb_data_periodic_compaction_seconds; \
    unsigned long long rocksdb_meta_periodic_compaction_seconds; \
    unsigned long long rocksdb_data_max_bytes_for_level_base; \
    unsigned long long rocksdb_meta_max_bytes_for_level_base; \
    unsigned long long rocksdb_max_total_wal_size; \
    unsigned long long rocksdb_data_suggest_compact_sliding_window_size; \
    unsigned long long rocksdb_data_suggest_compact_num_dels_trigger; \
    unsigned long long rocksdb_meta_suggest_compact_sliding_window_size; \
    unsigned long long rocksdb_meta_suggest_compact_num_dels_trigger; \
    unsigned long long rocksdb_data_min_blob_size; \
    unsigned long long rocksdb_meta_min_blob_size; \
    unsigned long long rocksdb_data_blob_file_size; \
    unsigned long long rocksdb_meta_blob_file_size; \
    int rocksdb_data_max_bytes_for_level_multiplier; \
    int rocksdb_meta_max_bytes_for_level_multiplier; \
    int rocksdb_data_compaction_dynamic_level_bytes; \
    int rocksdb_meta_compaction_dynamic_level_bytes; \
    int rocksdb_data_suggest_compact_deletion_percentage; \
    int rocksdb_meta_suggest_compact_deletion_percentage; \
    int rocksdb_data_max_write_buffer_number; \
    int rocksdb_meta_max_write_buffer_number; \
    int rocksdb_max_background_compactions; \
    int rocksdb_max_background_flushes; \
    int rocksdb_max_background_jobs; \
    int rocksdb_max_subcompactions; \
    int rocksdb_data_block_size; \
    int rocksdb_meta_block_size; \
    int rocksdb_data_cache_index_and_filter_blocks; \
    int rocksdb_meta_cache_index_and_filter_blocks; \
    int rocksdb_enable_pipelined_write; \
    int rocksdb_data_level0_slowdown_writes_trigger;  \
    int rocksdb_meta_level0_slowdown_writes_trigger;  \
    int rocksdb_data_disable_auto_compactions;  \
    int rocksdb_meta_disable_auto_compactions;  \
    int rocksdb_data_compression; /* rocksdb compresssion type: no/snappy/zlib. */  \
    int rocksdb_meta_compression; \
    int rocksdb_data_enable_blob_files; \
    int rocksdb_meta_enable_blob_files; \
    int rocksdb_data_enable_blob_garbage_collection;  \
    int rocksdb_meta_enable_blob_garbage_collection;  \
    int rocksdb_data_blob_garbage_collection_age_cutoff_percentage; \
    int rocksdb_meta_blob_garbage_collection_age_cutoff_percentage; \
    int rocksdb_data_blob_garbage_collection_force_threshold_percentage;  \
    int rocksdb_meta_blob_garbage_collection_force_threshold_percentage;  \
    /* swap block*/ \
    struct swapUnblockCtx* swap_dependency_block_ctx; \
    /* absent cache */ \
    int swap_absent_cache_enabled; \
    int swap_absent_cache_include_subkey; \
    unsigned long long swap_absent_cache_capacity; \
    /* cuckoo filter */ \
    int swap_cuckoo_filter_enabled; \
    int swap_cuckoo_filter_bit_type; \
    unsigned long long swap_cuckoo_filter_estimated_keys; \
    /* swap batch */ \
    struct swapBatchCtx *swap_batch_ctx; \
    swapBatchLimitsConfig swap_batch_limits[SWAP_TYPES_FORWARD]; \
    /* swap ratelimit */ \
    int swap_ratelimit_maxmemory_percentage; \
    int swap_ratelimit_maxmemory_pause_growth_rate; \
    int swap_ratelimit_policy; \
    long long stat_swap_ratelimit_client_pause_ms; \
    long long stat_swap_ratelimit_client_pause_count; \
    long long stat_swap_ratelimit_rejected_cmd_count; \
    unsigned long long swap_compaction_filter_disable_until; \
    int swap_compaction_filter_skip_level; \
    int swap_dirty_subkeys_enabled; \
    /* swap persist */ \
    int swap_persist_enabled; \
    struct swapPersistCtx *swap_persist_ctx; \
    int swap_persist_lag_millis; \
    int swap_persist_inprogress_growth_rate; \
    int swap_ratelimit_persist_lag; \
    int swap_ratelimit_persist_pause_growth_rate; \
    uint64_t swap_persist_load_fix_version; \
    /* swap meta flush */ \
    int swap_flush_meta_deletes_percentage; \
    unsigned long long swap_flush_meta_deletes_num; \
    /* swap rordb */ \
    int swap_repl_rordb_sync; \
    unsigned long long swap_repl_rordb_max_write_bps; \
    client *swap_draining_master; \
    /* ttl compact, only compact default CF */ \
    int swap_ttl_compact_enabled; \
    unsigned int swap_ttl_compact_expire_percentile; \
    unsigned long long swap_ttl_compact_period; /* seconds */ \
    unsigned long long swap_sst_age_limit_refresh_period; /* seconds */ \
    struct swapTtlCompactCtx *swap_ttl_compact_ctx; \
    /* for swap.info command, which propagate system info to replica */ \
    int swap_swap_info_supported; \
    int swap_swap_info_propagate_mode; \
    unsigned long long swap_swap_info_slave_period;     /* Master send cmd swap.info to the slave every N seconds */

#ifdef __APPLE__
#define SWAP_REDIS_SERVER SWAP_REDIS_SERVER_
#else
#define SWAP_REDIS_SERVER SWAP_REDIS_SERVER_ \
    /* swap_cpu_usage */ \
    redisAtomic int swap_threads_initialized; \
    struct swapThreadCpuUsage *swap_cpu_usage;
#endif

#endif
