# !!! Note that ROR does not support Trocks-specific commands.

1. trocks secondary development command
    <table>
      <tr><th style="font-weight: normal">Command</th><th style="font-weight: normal">Desc</th></tr>
      <tr><th style="font-weight: normal">hsetex</th><th style="font-weight: normal">hset with expire</th></tr>
      <tr><th style="font-weight: normal">hmsetex</th><th style="font-weight: normal">hmset with expire</th></tr>
    </table>

2. Sortedint Commands（ROR recommends using redis’ set commands, such as scard, sadd...）
    <table>
      <tr><th style="font-weight: normal">Command</th><th style="font-weight: normal">Desc</th></tr>
      <tr><th style="font-weight: normal">sicard</th><th style="font-weight: normal">like scard</th></tr>
      <tr><th style="font-weight: normal">siadd</th><th style="font-weight: normal">like sadd, but member is int</th></tr>
      <tr><th style="font-weight: normal">sirem</th><th style="font-weight: normal">like srem, but member is int</th></tr>
      <tr><th style="font-weight: normal">sirange</th><th style="font-weight: normal">sirange key offset count cursor since_id</th></tr>
      <tr><th style="font-weight: normal">sirevrange</th><th style="font-weight: normal">sirevrange key offset count cursor max_id</th></tr>
    </table>

3. LOCK Commands (ROR recommends using multi transactions instead)
    <table>
      <tr><th style="font-weight: normal">Command</th><th style="font-weight: normal">Desc</th></tr>
      <tr><th style="font-weight: normal">getl</th><th style="font-weight: normal">lock key, act is similar with "get"</th></tr>
      <tr><th style="font-weight: normal">setfl</th><th style="font-weight: normal">release key, act is similar with "set"</th></tr>
      <tr><th style="font-weight: normal">abortfl</th><th style="font-weight: normal">release key, quit transaction anyway</th></tr>
      <tr><th style="font-weight: normal">setflex</th><th style="font-weight: normal">release key. act is similar with "setex"</th></tr>
      <tr><th style="font-weight: normal">showl</th><th style="font-weight: normal">monitor lock actions</th></tr>
      <tr><th style="font-weight: normal">freel</th><th style="font-weight: normal">release key, quit transaction by condition</th></tr>
      <tr><th style="font-weight: normal">hgetl</th><th style="font-weight: normal">lock key, act is similar with "hget"</th></tr>
      <tr><th style="font-weight: normal">hsetfl</th><th style="font-weight: normal">release key, act is similar with "hset"</th></tr>
      <tr><th style="font-weight: normal">hsetflex</th><th style="font-weight: normal">release key, act is similar with "hsetex"</th></tr>
      <tr><th style="font-weight: normal">hmgetl</th><th style="font-weight: normal">lock key, act is similar with "hmget"</th></tr>
      <tr><th style="font-weight: normal">hmsetfl</th><th style="font-weight: normal">release key, act is similar with "hmset"</th></tr>
      <tr><th style="font-weight: normal">hmsetflex</th><th style="font-weight: normal">release key, act is similar with "hmsetex"</th></tr>
    </table>

4. Commands supported by ROR
   1. Redis native commands
      | Command | Type | Whether to support | Supported version | Remark |
      | ---- | ---- | -------- | -------- | ---- |
      | get | string | √ | 1.0.0 |  |
      | getex | string | √ | 1.0.0 |  |
      | getdel | string | √ | 1.0.0 |  |
      | set | string | √ | 1.0.0 |  |
      | setnx | string | √ | 1.0.0 |  |
      | setex | string | √ | 1.0.0 |  |
      | psetex | string | √ | 1.0.0 |  |
      | append | string | √ | 1.0.0 |  |
      | strlen | string | √ | 1.0.0 |  |
      | del | string | √ | 1.0.0 |  |
      | unlink | string | √ | 1.0.0 |  |
      | exists | string | √ | 1.0.0 |  |
      | setbit | string | √ | 1.0.0 |  |
      | getbit | string | √ | 1.0.0 |  |
      | bitfield | string | √ | 1.0.0 |  |
      | bitfield_ro | string | √ | 1.0.0 |  |
      | setrange | string | √ | 1.0.0 |  |
      | getrange | string | √ | 1.0.0 |  |
      | substr | string | √ | 1.0.0 |  |
      | incr | string | √ | 1.0.0 |  |
      | decr | string | √ | 1.0.0 |  |
      | mget | string | √ | 1.0.0 |  |
      | incrby | string | √ | 1.0.0 |  |
      | decrby | string | √ | 1.0.0 |  |
      | incrbyfloat | string | √ | 1.0.0 |  |
      | getset | string | √ | 1.0.0 |  |
      | mset | string | √ | 1.0.0 |  |
      | msetnx | string | √ | 1.0.0 |  |
      | pfselftest | string | √ | 1.0.0 |  |
      | pfadd | string | √ | 1.0.0 |  |
      | pfcount | string | √ | 1.0.0 |  |
      | pfmerge | string | √ | 1.0.0 |  |
      | pfdebug | string | √ | 1.0.0 |  |
      | stralgo | string | √ | 1.0.0 |  |
      | bitop | string | √ | 1.0.0 |  |
      | bitcount | string | √ | 1.0.0 |  |
      | bitpos | string | √ | 1.0.0 |  |
      | rpush | list | √ | 1.0.0 |
      | lpush | list | √ | 1.0.0 |
      | rpushx | list | √ | 1.0.0 |
      | lpushx | list | √ | 1.0.0 |
      | linsert | list | √ | 1.0.0 |
      | rpop | list | √ | 1.0.0 |
      | lpop | list | √ | 1.0.0 |
      | brpop | list | √ | 1.0.0 |
      | brpoplpush | list | √ | 1.0.3 |
      | blmove | list | √ | 1.0.3 |
      | blpop | list | √ | 1.0.0 |
      | llen | list | √ | 1.0.0 |
      | lindex | list | √ | 1.0.0 |
      | lset | list | √ | 1.0.0 |
      | lrange | list | √ | 1.0.0 |
      | ltrim | list | √ | 1.0.0 |
      | lpos | list | √ | 1.0.0 |
      | lrem | list | √ | 1.0.0 |
      | rpoplpush | list | √ | 1.0.0 |
      | lmove | list | √ | 1.0.0 |
      | sadd | set | √ | 1.0.0 |
      | srem | set | √ | 1.0.0 |
      | smove | set | √ | 1.0.0 |
      | sismember | set | √ | 1.0.0 |
      | smismember | set | √ | 1.0.0 |
      | scard | set | √ | 1.0.0 |
      | spop | set | √ | 1.0.0 |
      | srandmember | set | √ | 1.0.0 |
      | sinter | set | √ | 1.0.0 |
      | sinterstore | set | √ | 1.0.0 |
      | sunion | set | √ | 1.0.0 |
      | sunionstore | set | √ | 1.0.0 |
      | sdiff | set | √ | 1.0.0 |
      | sdiffstore | set | √ | 1.0.0 |
      | smembers | set | √ | 1.0.0 |
      | sscan | set | √ | 1.0.0 |
      | zadd | zset | √ | 1.0.0 |
      | zincrby | zset | √ | 1.0.0 |
      | zrem | zset | √ | 1.0.0 |
      | zremrangebyscore | zset | √ | 1.0.0 |
      | zremrangebyrank | zset | √ | 1.0.0 |
      | zremrangebylex | zset | √ | 1.0.0 |
      | zunionstore | zset | √ | 1.0.0 |
      | zinterstore | zset | √ | 1.0.0 |
      | zdiffstore | zset | √ | 1.0.0 |
      | zunion | zset | √ | 1.0.0 |
      | zinter | zset | √ | 1.0.0 |
      | zdiff | zset | √ | 1.0.0 |
      | zrange | zset | √ | 1.0.0 |
      | zrangestore | zset | √ | 1.0.0 |
      | zrangebyscore | zset | √ | 1.0.0 |
      | zrevrangebyscore | zset | √ | 1.0.0 |
      | zrangebylex | zset | √ | 1.0.0 |
      | zrevrangebylex | zset | √ | 1.0.0 |
      | zcount | zset | √ | 1.0.0 |
      | zlexcount | zset | √ | 1.0.0 |
      | zrevrange | zset | √ | 1.0.0 |
      | zcard | zset | √ | 1.0.0 |
      | zscore | zset | √ | 1.0.0 |
      | zmscore | zset | √ | 1.0.0 |
      | zrank | zset | √ | 1.0.0 |
      | zrevrank | zset | √ | 1.0.0 |
      | zscan | zset | √ | 1.0.0 |
      | zpopmin | zset | √ | 1.0.0 |
      | zpopmax | zset | √ | 1.0.0 |
      | bzpopmin | zset | √ | 1.0.0 |
      | bzpopmax | zset | √ | 1.0.0 |
      | zrandmember | zset | √ | 1.0.0 |
      | hset | hash | √ | 1.0.0 |
      | hsetnx | hash | √ | 1.0.0 |
      | hget | hash | √ | 1.0.0 |
      | hmset | hash | √ | 1.0.0 |
      | hmget | hash | √ | 1.0.0 |
      | hincrby | hash | √ | 1.0.0 |
      | hincrbyfloat | hash | √ | 1.0.0 |
      | hdel | hash | √ | 1.0.0 |
      | hlen | hash | √ | 1.0.0 |
      | hstrlen | hash | √ | 1.0.0 |
      | hkeys | hash | √ | 1.0.0 |
      | hvals | hash | √ | 1.0.0 |
      | hgetall | hash | √ | 1.0.0 |
      | hexists | hash | √ | 1.0.0 |
      | hrandfield | hash | √ | 1.0.0 |
      | hscan | hash | √ | 1.0.0 |
      | dbsize | keyspace | √ | 1.0.0 |
      | randomkey | keyspace | √ | 1.0.0 |
      | expire | keyspace | √ | 1.0.0 |
      | expireat | keyspace | √ | 1.0.0 |
      | pexpire | keyspace | √ | 1.0.0 |
      | pexpireat | keyspace | √ | 1.0.0 |
      | ttl | keyspace | √ | 1.0.0 |
      | touch | keyspace | √ | 1.0.0 |
      | pttl | keyspace | √ | 1.0.0 |
      | persist | keyspace | √ | 1.0.0 |
      | type | keyspace | √ | 1.0.0 |
      | scan | keyspace | √* | 1.0.0 | The cursor must start from 0, and the next cursor must be the cursor returned by ROR last time (arbitrary starting of the cursor is not supported). In other words, if the client disconnects and reconnects, the cursor must start from 0 again.
      | dump | keyspace | √ | 1.0.0 |
      | object | keyspace | √ | 1.0.0 |
      | flushdb | keyspace | √ | 1.0.0 |
      | flushall | keyspace | √ | 1.0.0 |
      | select | keyspace | √* | 1.0.0 | Select in eval is not supported (but select in multi/exec is supported)
      | move | keyspace | √ | 1.0.0 |
      | copy | keyspace | √ | 1.0.0 |
      | rename | keyspace | √ | 1.0.0 |
      | renamenx | keyspace | √ | 1.0.0 |
      | keys | keyspace | × | |
      | swapdb | keyspace | × | |
      | auth | server | √ | 1.0.0 |
      | ping | server | √ | 1.0.0 |
      | echo | server | √ | 1.0.0 |
      | save | server | √ | 1.0.0 |
      | bgsave | server | √ | 1.0.0 |
      | refullsync | server | √ | 1.0.0 |
      | bgrewriteaof | server | √ | 1.0.0 |
      | shutdown | server | √ | 1.0.0 |
      | lastsave | server | √ | 1.0.0 |
      | multi | transaction | √ | 1.0.0 |
      | exec | transaction | √ | 1.0.0 |
      | discard | transaction | √ | 1.0.0 |
      | watch | transaction | √* | 1.0.0 |The impact of the flushdb command on watch Redis and ROR behave differently: <br/>* Redis: If the key to be watched does not exist in the db, flushdb will not cause subsequent exec to terminate. <br/>* ROR: Even if the watched key does not exist in the db, flushdb will cause subsequent exec to fail. <br/>The main reason is: ror does not contain the full amount of keys, flushdb cannot determine whether the watched key does not exist, and handles it according to the existence of the key, terminating the current transaction with the watch key.
      | unwatch | transaction | √ | 1.0.0 |
      | eval | transaction | √ | 1.0.0 |
      | evalsha | transaction | √ | 1.0.0 |
      | subscribe | pubsub | √ | 1.0.0 |
      | unsubscribe | pubsub | √ | 1.0.0 |
      | psubscribe | pubsub | √ | 1.0.0 |
      | punsubscribe | pubsub | √ | 1.0.0 |
      | publish | pubsub | √ | 1.0.0 |
      | pubsub | pubsub | √ | 1.0.0 |
      | cluster | cluster | √ | 1.0.0 |
      | restore | cluster | √ | 1.0.0 |
      | restore-asking | cluster | √ | 1.0.0 |
      | migrate | cluster | √ | 1.0.0 |
      | asking | cluster | √ | 1.0.0 |
      | readonly | cluster | √ | 1.0.0 |
      | readwrite | cluster | √ | 1.0.0 |
      | geoadd | geo | √ | 1.0.0 |
      | georadius | geo | √ | 1.0.0 |
      | georadius_ro | geo | √ | 1.0.0 |
      | georadiusbymember | geo | √ | 1.0.0 |
      | georadiusbymember_ro | geo | √ | 1.0.0 |
      | geohash | geo | √ | 1.0.0 |
      | geopos | geo | √ | 1.0.0 |
      | geodist | geo | √ | 1.0.0 |
      | geosearch | geo | √ | 1.0.0 |
      | geosearchstore | geo | √ | 1.0.0 |
      | xadd | stream | √ | 1.0.0 |
      | xrange | stream | √ | 1.0.0 |
      | xrevrange | stream | √ | 1.0.0 |
      | xlen | stream | √ | 1.0.0 |
      | xread | stream | √ | 1.0.0 |
      | xreadgroup | stream | √ | 1.0.0 |
      | xgroup | stream | √ | 1.0.0 |
      | xsetid | stream | √ | 1.0.0 |
      | xack | stream | √ | 1.0.0 |
      | xpending | stream | √ | 1.0.0 |
      | xclaim | stream | √ | 1.0.0 |
      | xautoclaim | stream | √ | 1.0.0 |
      | xinfo | stream | √ | 1.0.0 |
      | xdel | stream | √ | 1.0.0 |
      | xtrim | stream | √ | 1.0.0 |
      | wait | server | √ | 1.0.0 |
      | command | server | √ | 1.0.0 |
      | latency | server | √ | 1.0.0 |
      | acl | server | √ | 1.0.0 |
      | reset | server | √ | 1.0.0 |
      | failover | server | √ | 1.0.0 |
      | sync | server | √ | 1.0.0 |
      | psync | server | √ | 1.0.0 |
      | replconf | server | √ | 1.0.0 |
      | sort | server | √ | 1.0.0 |
      | info | server | √ | 1.0.0 |
      | monitor | server | √ | 1.0.0 |
      | slaveof | server | √ | 1.0.0 |
      | xslaveof | server | √ | 1.0.0 |
      | replicaof | server | √ | 1.0.0 |
      | role | server | √ | 1.0.0 |
      | debug | server | √ | 1.0.0 |
      | config | server | √ | 1.0.0 |
      | client | server | √ | 1.0.0 |
      | hello | server | √ | 1.0.0 |
      | slowlog | server | √ | 1.0.0 |
      | script | server | √ | 1.0.0 |
      | time | server | √ | 1.0.0 |
      | memory | server | √ | 1.0.0 |

   2. SWAP Related commands

      | Command | Supported version | Grammer | Description | Remark |
      | ---- | ------- | ---- | ---- | ---- |
      | swap | 1.0.1 | 1) SWAP <subcommand> [\<arg\> \[value\] \[opt\] ...]. Subcommands are:<br/> 2) OBJECT <key><br/> 3)     Show info about `key` and assosiated value.<br/> 4) ENCODE-META-KEY <key><br/> 5)     Encode meta key.<br/> 6) DECODE-META-KEY <rawkey><br/> 7)     Decode meta key.<br/> 8) ENCODE-DATA-KEY <key> <version> <subkey><br/> 9)     Encode data key.<br/>10) DECODE-DATA-KEY <rawkey><br/> 11)     Decode data key.<br/>12) RIO-GET meta\|data \<rawkey\> \<rawkey\> ...<br/> 13)     Get raw value from rocksdb.<br/>14) RIO-SCAN meta\|data \<prefix\><br/>15)     Scan rocksdb with prefix.<br/>16) RIO-ERROR \<count\><br/>17)     Make next count rio return error.<br/>18) RESET-STATS<br/>19)     Reset swap stats.<br/>20) COMPACT<br/>21)    COMPACT rocksdb<br/>22) ROCKSDB-PROPERTY-INT <rocksdb-prop-name> [<cfname,cfname...>]<br/>23)     Get rocksdb property value (int type)<br/>24) ROCKSDB-PROPERTY-VALUE <rocksdb-prop-name> [<cfname,cfname...>]<br/>25)     Get rocksdb property value (string type)<br/>26) HELP<br/>27)     Prints this help.| Display information about keys and associated values | swap function auxiliary command |
      | swap.evict | 1.0.1 | swap.evict key1 key2 ... | Internal command that can be used to manually remove keys | |
      | swap.slowlog | 1.0.1 | 1) SWAP.SLOWLOG <subcommand> [\<arg\> \[value] \[opt] ...]. Subcommands are:<br/>2) GET [\<count\>]<br/>3)     Return top \<count\> entries from the slowlog (default: 10). Entries are<br/>4)     made of:<br/>5)     id, timestamp, time in microseconds, swap cnt, swap time in microseconds, arguments array, <br/>6)     client IP and port, client nasme,<br/>7)     swap debug traces(need open swap-debug-trace-latency before).<br/>8) HELP<br/>9)     Prints this help. | slowlog displays additional swap-related trace information |
      | swap.expired | 1.0.1 | ~ | Internal command | Manual call has no effect |
      | swap.scanexpire | 1.0.1 | ~ | Internal command | Manual call has no effect |
      | swap.mutexop | 1.0.1 | ~ | Internal command | Manual call has no effect |
    
   3. GTID Related commands

      | Command | Supported version | Grammer | Description | Remark |
      | ---- | ------- | ---- | ---- | ---- |
      | gtid | 1.0.0 | ~ | Internal command | |
      | gtidx | 1.0.5 | 1) GTIDX <subcommand> [\<arg\> \[value\] \[opt\] ...]. Subcommands are:<br/>2) LIST [\<uuid\>]<br/>3)     List gtid or uuid(if specified) gaps.<br/>4) STAT [\<uuid\>]<br/>5)     Show gtid or uuid(if specified) stat.<br/>6) REMOVE \<uuid\><br/>7)     Return uuid.<br/>8) HELP<br/>9)     Prints this help. | gtidx function auxiliary command | |


5. ROR Supported Configurations
   1. Redis Native Configuration
    ROR inherits all the native configurations of Redis, and only a few configuration semantics have changed. The specific list is as follows:

      | Features | redis | ROR production default (domestic) | ROR production default (overseas) | ror |
      | -------- | ----- | ------------------- | ----------------- | -------------------------- |
      | maxmemory | The maximum amount of memory (i.e. the maximum capacity of keyspace) | 2.5G | 2.2G total memory:<br/>256MB rocksdb writebuffer (max_write_buffer_num * write_buffer_size \*3 (CF can consider setting different write_buffer*)) + blockcache<br />maxmemory: 2G setting | The maximum amount of memory, but the maximum capacity of the keyspace can exceed maxmemory (the excess is persisted to SSD). The maximum keyspace capacity of ROR is specified by swap-max-db-size. |
      | sanitize-dump-payload | yes/no/client | no | no | ror loads directly from rdb to rocksdb, skipping deep inspection. |

   2. swap Related Configuration
   
      | Option name | ROR version | Value range | Default value | ROR production default (domestic) | ROR production default (overseas) | Can it be modified dynamically | trocks option name | trocks domestic | trocks overseas | Description |
      | -------------------------------- | ------- | ---------------- | ------ | ----------------- | ----------------- | ------------ | --------------------------- | ---------- | ---------- | ----------------------------------------------------------------------------------------- |
      | swap-mode | 1.0.1 | disk, memory | memory | disk | disk | No | ~ | ~ | ~ | swap mode, disk: disk mode, enable hot and cold separation; memory: memory mode, the same as redis. |
       | swap-threads | 1.0.1 | 4~64 | 4 | 4 | 4 | No | workers | 4 | 4 | The number of swap threads. |
       | swap-max-db-size | 1.0.1 | 0~LLONG_MAX | 0 | 40G | 24G | Yes | max-db-size | 40(G) | 40(G) | Maximum disk usage. |
       | swap-inprogress-memory-slowdown | 1.0.1 | 0~LLONG_MAX | 256mb | 256mb | 256mb | Yes | ~ | ~ | ~ | When the memory occupied by the key and value being swapped exceeds swap-memory-slowdown, then ror Client write speed will begin to be throttled. |
       | swap-inprogress-memory-stop | 1.0.1 | 0~LLONG_MAX | 512mb | 512mb | 512mb | Yes | ~ | ~ | ~ | When the memory occupied by the key and value being swapped exceeds swap-memory-slowdown, then ror Will start limiting client write speeds while pausing ejection of hot keys to disk. |
       | swap-maxmmeory-oom-percentage | 1.0.1 | 100~INT_MAX | 200 | 200 | 200 | Yes | ~ | ~ | ~ | When the current used_memory of redis far exceeds maxmemory, ror will reject all write commands. |
       | swap-evict-step-max-subkeys | 1.0.1 | 0~LLONG_MAX | 1024 | 1024 | 1024 | Yes | ~ | ~ | ~ | The maximum number of subkeys for a single evict (hot or cold) of a single key. |
       | swap-evict-step-max-memory | 1.0.1 | 0~LLONG_MAX | 1mb | 1mb | 1mb | Yes | ~ | ~ | ~ | The maximum memory for a single evict (hot or cold) of a single key. |
      | swap-repl-max-rocksdb-read-bps | 1.0.1 | 0~LLONG_MAX | 0 | 100mb | 100mb | Yes | max-replication-mb | 100 | 100 | When saving RDB in full copy, bgsave, etc. The maximum rate at which disk files can be read (i.e. the maximum rate at which iterates rocksdb data). |
       | swap-rocksdb-stats-collect-interval-ms | 1.0.1 | 1~INT_MAX | 2000 | 2000 | 2000 | Yes | ~ | ~ | ~ | The interval for updating rocksdb stats cache. |
       | swap-evict-inprogress-limit | 1.0.1 | 4~INT_MAX | 64 | 64 | 64 | Yes | ~ | ~ | ~ | The hard limit of the maximum concurrency of evict (hot and cold). The maximum concurrency of evict (hot and cold) is positively related to used_memory-maxmemory. The more memory exceeds the limit, the higher the concurrency of evict, but the concurrency of evict will not exceed swap-evict-inprogress-limit. |
       | swap-evict-inprogress-growth-rate | 1.0.1 | 1~INT_MAX | 10mb | 10mb | 10mb | Yes | ~ | ~ | ~ | evict (hot and cold) concurrency growth rate, min (used_memory-maxmemory / swap-evict-inprogress-growth-rate, swap-evict-inprogress-limit) is the current evict concurrency limit |
       | swap-debug-trace-latency | 1.0.1 | yes,no | no | no | no | yes | ~ | ~ | ~ | Whether to enable the time statistics function of each stage of swap. |
       | swap-debug-evict-keys | 1.0.1 | -1~INT_MAX | 0 | 0 | 0 | Yes | ~ | ~ | ~ | The maximum number of evict (hot and cold) keys after the command ends, -1 means evict All hot keys. |
       | swap-debug-rio-delay-micro | 1.0.1 | -1~INT_MAX | 0 | 0 | 0 | Yes | ~ | ~ | ~ | The time (microseconds) for each RIO sleep. |
       | swap-debug-swapout-notify-delay-micro | 1.0.1 | -1~INT_MAX | 0 | 0 | 0 | Yes | ~ | ~ | ~ | The delay time (microseconds) for each notify. |
       | swap-debug-before-exec-swap-delay-micro | 1.0.1 | 0~INT_MAX | 0 | 0 | 0 | Yes | ~ | ~ | ~ | The delay in microseconds before exec. |
       | swap-debug-init-rocksdb-delay-micro | 1.0.1 | 0~INT_MAX | 0 | 0 | 0 | Yes | ~ | ~ | ~ | Open rocksdb delay time in microseconds. |
       | swap-perflog-sample-ratio | 1.0.4 | 0~100 | 0 | 0 | 0 | yes | profiling-sample-ratio | 0 | 0 | perflog sampling rate |
      | swap-perflog-max-len | 1.0.4 | 0~LONG_MAX | 128 | 128 | 128 | Yes | profiling-sample-record-max-len | 256 | 256 | The maximum number of perflog records |
       | swap-perflog-log-slower-than-us | 1.0.4 | 0~LONG_MAX | 10000 | 10000 | 10000 | Yes | profiling-sample-record-threshold-ms | 100 | 100 | The threshold for perflog to retain samples, default When the swap operation takes more than 10ms, it will be recorded in perlog |
       | rocksdb.data.compression | 1.0.3 | no, snappy, zlib,optimized_for_compaction | snappy | snappy | snappy | yes | rocksdb.compression | snappy | snappy | Whether rocksdb enables compression feature. |
       | rocksdb.meta.compression | 1.0.3 | no, snappy, zlib,optimized_for_compaction | snappy | ~ | ~ | yes | ~ | ~ | ~ | |
       | rocksdb.meta.block_cache_size | 1.0.1 | 0~ULLONG_MAX | 512mb | 512mb | ? | No | rocksdb.metadata_block_cache_size | 128 | 128 | meta CF blockcache capacity |
       | rocksdb.data.block_cache_size | 1.0.1 | 0~ULLONG_MAX | 8mb | 8mb | 8mb(x2) | No | rocksdb.subkey_block_cache_size | 128 | 128 | data CF blockcache capacity |
       | rocksdb.max_open_files | 1.0.1 | -1~INT_MAX | -1 | -1 | -1 | No | rocksdb.max_open_files | 2048 | 2048 | rocksdb max_open_files parameter value |
       | rocksdb.data.write_buffer_size | 1.0.3 | 0~ULLONG_MAX | 64mb | 64mb | ?(x3) | Yes | rocksdb.write_buffer_size | 128 (actual 64) | 16 (actual 64) | rocksdb write_buffer_size parameter value |
       | rocksdb.meta.write_buffer_size | 1.0.3 | 0~ULLONG_MAX | 64mb | ~ | ~ | Yes | ~ | ~ | ~ | rocksdb write_buffer_size parameter value |
       | rocksdb.data.target_file_size_base | 1.0.3 | 0~ULLONG_MAX | 32mb | 32mb | ? | Yes | rocksdb.target_file_size_base | 128 | 16 (actual 128) | rocksdb target_file_size_base parameter value |
      | rocksdb.meta.target_file_size_base | 1.0.3 | 0~ULLONG_MAX | 32mb | ~ | ~ | Yes | ~ | ~ | ~ | rocksdb target_file_size_base parameter value |
       | rocksdb.data.max_write_buffer_number | 1.0.3 | 1~256 | 3 | 3 | 4 | Yes | rocksdb.max_write_buffer_number | 4 | 4 | rocksdb max_write_buffer_number parameter value |
       | rocksdb.meta.max_write_buffer_number | 1.0.3 | 1~256 | 3 | ~ | ~ | Yes | ~ | ~ | ~ | rocksdb max_write_buffer_number parameter value |
       | rocksdb.max_background_compactions | 1.0.1 | 1~64 | 4 | 4 | 4 | No | rocksdb.max_background_compactions | 4 | 4 | rocksdb max_background_compactions parameter value |
       | rocksdb.max_background_flushes | 1.0.1 | -1~64 | -1 | -1 | -1 | No | rocksdb.max_background_flushes | 4 | 4 | rocksdb max_background_flushes parameter value |
       | rocksdb.max_subcompactions | 1.0.1 | 1~64 | 1 | 1 | 1 | No | rocksdb.max_sub_compactions | 4 | 4 | rocksdb max_subcompactions parameter value |
       | rocksdb.data.block_size | 1.0.3 | 512~INT_MAX | 8192 | 8192 | 8192 | No | rocksdb.block_size | 4096 | 4096 | rocksdb block_size parameter value |
       | rocksdb.meta.block_size | 1.0.3 | 512~INT_MAX | 8192 | ~ | ~ | No | ~ | ~ | ~ | rocksdb block_size parameter value |
       | rocksdb.cache_index_and_filter_blocks | 1.0.3 | yes|no | no | no | no | no | rocksdb.cache_index_and_filter_blocks | no | no | rocksdb cache_index_and_filter_blocks parameter value |
       | rocksdb.meta.cache_index_and_filter_blocks | 1.0.3 | yes|no | no | ~ | ~ | no | ~ | ~ | ~ | rocksdb cache_index_and_filter_blocks parameter value |
       | rocksdb.enable_pipelined_write | 1.0.1 | yes\|no | no | no | no | No | rocksdb.enable_pipelined_write | no | no | Whether to enable the pipelined write function of RocksDB. |
      | rocksdb.data.level0_slowdown_writes_trigger | 1.0.3 | 1~INT_MAX | 20 | 20 | 20 | Yes | rocksdb.level0_slowdown_writes_trigger | 24 | 24 | Threshold to trigger Level 0 write slowdown. |
       | rocksdb.meta.level0_slowdown_writes_trigger | 1.0.3 | 1~INT_MAX | 20 | ~ | ~ | Yes | ~ | ~ | ~ | Threshold to trigger Level 0 write slowdown. |
       | rocksdb.data.disable_auto_compactions | 1.0.3 | yes\|no | no | no | no | Yes | rocksdb.disable_auto_compactions | no | no | Whether to disable automatic compaction. |
       | rocksdb.meta.disable_auto_compactions | 1.0.3 | yes\|no | no | ~ | ~ | Yes | rocksdb.disable_auto_compactions | no | no | Whether to disable automatic compaction. |
       | rocksdb.ratelimiter.rate_per_sec | 1.0.3 | 0~ULLONG_MAX | 512mb | ~ | ~ | No | rocksdb.ratelimiter.rate_per_sec | no | no | Maximum IO rate for compaction and flushing. If set, the delayed write rate (maximum DB write rate after slowing down) will be set to the same value. |
       | rocksdb.bytes_per_sync | 1.0.3 | 0~ULLONG_MAX | 1mb | ~ | ~ | No | ~ | no | no | Perform fsync every bytes_per_sync during SST file creation. |
       | rocksdb.data.max_bytes_for_level_base | 1.0.3 | 0~ULLONG_MAX | 256mb | ~ | ~ | Yes | ~ | ~ | ~ | Maximum file size for Level 1. |
       | rocksdb.meta.max_bytes_for_level_base | 1.0.3 | 0~ULLONG_MAX | 256mb | ~ | ~ | Yes | ~ | ~ | ~ | Maximum file size for Level 1. |
      | rocksdb.data.max_bytes_for_level_multiplier | 1.0.5 | 1~INT_MAX | 10 | ~ | ~ | Yes | ~ | ~ | ~ | Multiplier for the maximum file size for each level. Ln+1 = Ln * multiplier. |
       | rocksdb.meta.max_bytes_for_level_multiplier | 1.0.5 | 1~INT_MAX | 10 | ~ | ~ | Yes | ~ | ~ | ~ | Multiplier for the maximum file size for each level. Ln+1 = Ln * multiplier. |
       | rocksdb.data.compaction_dynamic_level_bytes | 1.0.5 | yes\|no | no | ~ | ~ | No | ~ | ~ | ~ | Dynamic level capacity mode, dynamically calculate the maximum capacity of each level based on the data set size. Optimization space can be extended to 1.111111. |
       | rocksdb.meta.compaction_dynamic_level_bytes | 1.0.5 | yes\|no | no | ~ | ~ | No | ~ | ~ | ~ | Dynamic level capacity mode, dynamically calculate the maximum capacity of each level based on the data set size. Optimization space can be extended to 1.111111. |
       | rocksdb.data.suggest_compact_deletion_percentage | 1.0.5 | 0~100 | 0.95 | ~ | ~ | No | ~ | ~ | ~ | Mark SST files when RocksDB is idle if the percentage of deletion marks in the SST file is greater than this configuration and perform compaction. 0 disables this feature. |
       | rocksdb.meta.suggest_compact_deletion_percentage | 1.0.5 | 0~100 | 0.95 | ~ | ~ | No | ~ | ~ | ~ | Mark SST files when RocksDB is idle if the percentage of deletion marks in the SST file is greater than this configuration and perform compaction. 0 disables this feature. |
       | rocksdb.data.periodic_compaction_seconds | 1.0.5 | 0~ULLONG_MAX | 86400 | ~ | ~ | Yes | ~ | ~ | ~ | Automatically perform compaction at certain intervals after the SST file is generated. This compaction does not push SST files to lower levels, it is only used for triggering
       | swap-scan-session-bits | 1.0.5 | 1~16 | 7 | | | No | | ||The maximum number of scan sessions is (1\<\<swap\-scan-session-bits\)|                                                                                    |
      | swap-scan-session-max-idle-seconds | 1.0.5 | 1~INT_MAX | 60 | | | | | | |Scan session maximum idle time (session exceeding this idle time can be used again) |
       | ctrip-monitor-port | 1.0.7 | 0~65535 | 6380 | | | | | | | Monitoring port |
       | swap-absent-cache-enabled | 1.0.7 | yes|no | yes | | | | | | Whether to start absent cache. (Absent cache caches recently accessed non-existent keys in LRU mode, which is used to solve the delay problem of hotspot access to non-existent keys) |
       | swap-absent-cache-capacity | 1.0.7 | 1~LLONG_MAX | 65536 | | | | | | | The capacity of the absent cache, indicating the number of keys that can be cached. |
       | swap-batch-limit | 1.1.0 | "IN 16 1mb OUT 16 1mb DEL 16 1mb" | | | | | | | | batch size |
       | swap-cuckoo-filter-enabled | 1.1.0 | yes|no | yes | | | | | | cuckoo filter switch |
       | swap-cuckoo-filter-bit-per-key | 1.1.0 | 8|12|16|32 | 8 | | | | The bits occupied by each key in cuckoo filter |
       | swap-cuckoo-filter-estimated-keys | 1.1.0 | 1~LLONG_MAX | 32000000 | | | | | | | Estimated number of keys |
       | swap-absent-cache-include-subkey | 1.1.0 | yes\|no | yes | yes | ~ | ~ | ~ | ~ | ~ | absent cache includes subkey switch |
       | rocksdb.max_background_jobs | 1.1.0 | -1~64 | 4 | No | ~ | ~ | ~ | ~ | ~ | rocksdb maximum number of background jobs |
       | swap-ratelimit-maxmemory-percentage | 1.1.1 | 100~INT_MAX | 200 | yes | ~ | -ratelimit-policydecision |
       | swap-ratelimit-policy | 1.1.1 | pause\|reject_oom\|reject_all\|disabled | pause | Yes | ~ | ~ | ~ | ~ | ~ | ror's rate limiting behavior on the client, including 4 types: pause, reject_oom, reject_all, disabled |
       | swap-ratelimit-maxmemory-pause-growth-rate | 1.1.1 | 1~INT_MAX | 20mb | Yes | ~ | ~ | ~ | ~ | ~ | When ror uses the pause policy to limit the speed of the client, the speed limit time growth rate |
       | swap-compaction-filter-skip-level | ~ | -1~INT_MAX | 0 | Yes | ~ | ~ | ~ | ~ | ~ | The level skipped by compaction filter |
      | swap-compaction-filter-disable-until | 1.1.1 | 0~LLONG_MAX | 0 | Yes | ~ | ~ | ~ | ~ | ~ | Temporarily turn off compaction-filter until the specified timestamp, in unix timestamps (seconds level) |
       | swap-persist-enabled | 1.2.0 | yes\|no | no | no | ~ | ~ | ~ | ~ | ~ | Turn on persistence function |
       | swap-ratelimit-persist-lag | 1.2.0 | 0~INT_MAX | 60 | Yes | ~ | ~ | ~ | ~ | ~ | Persistence delay rate limit threshold, after exceeding swap-ratelimit-persist-lag, it will be based on Selected swap-ratelimit-policy for rate limiting |
       | swap-ratelimit-persist-pause-growth-rate | 1.2.0 | 0~INT_MAX | 10 | is | ~ | ~ | ~ | ~ | ~ | lag per growth swap-ratelimit-persist-pause-growth-rate, The speed limit time is increased by 1ms. |
       | rocksdb.WAL_ttl_seconds | 1.2.0 | 0~INT_MAX | 18000 | No | ~ | ~ | ~ | ~ | ~ | rocksdb WAL maximum retention time |
       | rocksdb.WAL_size_limit_MB | 1.2.0 | 0~INT_MAX | 16384 | No | ~ | ~ | ~ | ~ | ~ | rocksdb WAL maximum reserved capacity |
       | rocksdb.max_total_wal_size | 1.2.0 | 0~ULONGLONG_MAX | 536870912 | No | ~ | ~ | ~ | ~ | ~ | When the wal capacity exceeds this value, the cf flush that depends on the wal is triggered. |
       | swap-debug-rdb-key-save-delay-micro | 1.2.0 | -1~INT_MAX | 0 | Yes | ~ | ~ | ~ | ~ | ~ | rdbsave cold key delay time |
       | rocksdb.data.suggest_compact_sliding_window_size | 1.2.1 | 0~ULONGLONG_MAX | 100000 | No | ~ | ~ | rocksdb.suggest_compact_sliding_window_size | ~ | ~ | suggest compaction trigger sliding window size |
       | rocksdb.meta.suggest_compact_sliding_window_size | 1.2.1 | 0~ULONGLONG_MAX | 100000 | No | ~ | ~ | ~ | ~ | ~ | suggest compaction trigger sliding window size |
       | rocksdb.data.suggest_compact_num_dels_trigger | 1.2.1 | 0~ULONGLONG_MAX | 80000 | No | ~ | ~ | rocksdb.suggest_compact_num_dels_trigger | ~ | ~ | The number of deletes in the sliding window that triggers suggestion compaction |
       | rocksdb.meta.suggest_compact_num_dels_trigger | 1.2.1 | 0~ULONGLONG_MAX | 80000 | No | ~ | ~ | ~ | ~ | ~ | The number of deletes in the sliding window that triggers suggestion compaction |
       | swap-flush-meta-deletes-percentage | 1.2.1 | 0~100 | 40 | Yes | ~ | ~ | ~ | ~ | ~ | The delete proportion that triggers METACF memtable flush (swap-flush-meta-deletes-percentage && swap-flush-meta-deletes-num will trigger flush only when they exceed the threshold) |
       | swap-flush-meta-deletes-num | 1.2.1 | 1~LLONG_MAX | 200000 | Yes | ~ | ~ | ~ | ~ | ~ | The delete proportion that triggers METACF memtable flush (swap-flush-meta-deletes-percentage && swap-flush-meta-deletes-num will trigger flush only when they exceed the threshold) |
      | rocksdb.data.enable_blob_files | 1.2.3 | yes\|no | no | Yes | ~ | ~ | rocksdb.enable_blob_files | ~ | ~ | Whether to enable blobdb |
       | rocksdb.meta.enable_blob_files | 1.2.3 | yes\|no | no | Yes | ~ | ~ | ~ | ~ | ~ | Whether to enable blobdb |
       | rocksdb.data.enable_blob_garbage_collection | 1.2.3 | yes\|no | no | yes | ~ | ~ | rocksdb.enable_blob_garbage_collection | ~ | ~ | Whether to enable blobdb force GC |
       | rocksdb.meta.enable_blob_garbage_collection | 1.2.3 | yes\|no | no | yes | ~ | ~ | ~ | ~ | ~ | Whether to enable blobdb force GC |
       | rocksdb.data.blob_garbage_collection_age_cutoff_percentage | 1.2.3 | 0~100 | 5 | Yes | ~ | ~ | ~ | ~ | ~ | blobdb forces GC selection range (by default, only the oldest 5% of blobs will be selected by GC ) |

6. ROR INFO information
    1. Redis native INFO
       ROR inherits all native info indicators of Redis, and its meaning remains the same as Redis.

   2. SWAP Related INFO
      <table>
        <thread style="font-weight: normal">
          <tr>
            <th style="font-weight: normal">section</th>
            <th style="font-weight: normal">version</th>
            <th style="font-weight: normal">field</th>
            <th style="font-weight: normal">description</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td rowspan="3">Cpu</td>
            <td>1.1.0</td>
            <td>swap_main_thread_cpu_usage</td>
            <td>ror main thread cpu usage</td>
          </tr>
          <tr>
            <td>1.1.0</td>
            <td>swap_swap_threads_cpu_usage</td>
            <td>ror swap thread cpu usage</td>
          </tr>
          <tr>
            <td>1.1.0</td>
            <td>swap_other_threads_cpu_usage</td>
            <td>ror other thread cpu usage</td>
          </tr>
          <tr>
            <td rowspan="3">Memory</td>
            <td>1.0.1</td>
            <td>swap_mem_rocksdb</td>
            <td>rocksdb amount of memory occupied</td>
          </tr>
          <tr>
            <td>1.0.1</td>
            <td>swap_rectified_frag_ratio</td>
            <td>eliminate the memory fragmentation rate of rocksdb occupied memory</td>
          </tr>
          <tr>
            <td>1.0.1</td>
            <td>swap_rectified_frag_bytes</td>
            <td>eliminate the amount of memory fragmentation occupied by rocksdb</td>
          </tr>
          <tr>
            <td>Keyspace</td>
            <td>1.0.1</td>
            <td>Keyspace:"db%d:keys=%lld,evicts=%lld,metas=%lld,expires=%lld,avg_ttl=%lld\r\n"</td>
            <td>evicts:number of cold keys metas:number of objectMeta</td>
          </tr>
          
        </tbody>
      </table>

# RoR configuration document
## Redis native configuration
1. ROR inherits all the native configurations of Redis, and only a few configuration semantics have changed. The specific list is as follows:
    <table>
        <tr>
            <th style="font-weight: normal" colspan="5">Redis configuration for semantic changes</th>
        </tr>
        <tr>
            <th style="font-weight: normal">Characteristics</th>
            <th style="font-weight: normal">redis semantics</th>
            <th style="font-weight: normal">ror production default (domestic)</th>
            <th style="font-weight: normal">ror production default (overseas)</th>
            <th style="font-weight: normal">ror semantics</th>
        </tr>
        <tr>
            <th style="font-weight: normal">sanitize-dump-payload</th>
            <th style="font-weight: normal">yes no client three options</th>
            <th style="font-weight: normal">no</th>
            <th style="font-weight: normal">no</th>
            <th style="font-weight: normal">ror is loaded directly from rdb to rocksdb, skipping depth inspection. </th>
        </tr>
        <tr>
            <th style="font-weight: normal">maxmemory</th>
            <th style="font-weight: normal">Maximum amount of memory (i.e. maximum capacity of keyspace)</th>
            <th style="font-weight: normal">2.5G</th>
            <th style="font-weight: normal">2.2G total memory:<br/>256MB rocksdb<br/>writebuffer(max_write_buffer_num * write_buffer_size *3 (CF can consider setting different write_buffer*))+blockcache<br/ >maxmemory: 2G settings</th>
            <th style="font-weight: normal">The maximum amount of memory, but the maximum capacity of the keyspace can exceed maxmemory (the excess is persisted to SSD). The maximum keyspace capacity of ROR is specified by swap-max-db-size. </th>
        </tr>
    </table>

2. SWAP related configuration
     <table>
         <tr>
             <th style="font-weight: normal">Option name</th>
             <th style="font-weight: normal">ROR version</th>
             <th style="font-weight: normal">Value range</th>
             <th style="font-weight: normal">Default value</th>
             <th style="font-weight: normal">ROR production default (domestic)</th>
             <th style="font-weight: normal">ROR production default (overseas)</th>
             <th style="font-weight: normal">Can it be modified dynamically</th>
             <th style="font-weight: normal">Description</th>
         </tr>
         <tr>
             <th style="font-weight: normal">ctrip-monitor-port</th>
             <th style="font-weight: normal">1.0.7</th>
             <th style="font-weight: normal">0~65535</th>
             <th style="font-weight: normal">6380</th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal">No</th>
             <th style="font-weight: normal">Monitoring port</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.bytes_per_sync</th>
             <th style="font-weight: normal">1.0.3</th>
             <th style="font-weight: normal">0~ULLONG_MAX</th>
             <th style="font-weight: normal">1mb</th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal">No</th>
             <th style="font-weight: normal">During the creation of rocksdb SST files, fsync is executed every bytes_per_sync</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.cache_index_and_filter_blocks</th>
             <th style="font-weight: normal">1.0.3</th>
             <th style="font-weight: normal">yes|no</th>
             <th style="font-weight: normal">no</th>
             <th style="font-weight: normal">no</th>
             <th style="font-weight: normal">no</th>
             <th style="font-weight: normal">No</th>
             <th style="font-weight: normal">rocksdb cache_index_and_filter_blocks parameter value</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.data.blob_file_size</th>
             <th style="font-weight: normal">1.2.3</th>
             <th style="font-weight: normal">0 ~ ULLONG_MAX</th>
             <th style="font-weight: normal">256mb</th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal">Yes</th>
             <th style="font-weight: normal">blobdb blob file size</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.data.blob_garbage_collection_age_cutoff_percentage</th>
             <th style="font-weight: normal">1.2.3</th>
             <th style="font-weight: normal">0~100</th>
             <th style="font-weight: normal">5</th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal">Yes</th>
             <th style="font-weight: normal">blobdb forces GC selection range (by default, only the oldest 5% of blobs will be selected by GC)</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.data.blob_garbage_collection_force_threshold_percentage</th>
             <th style="font-weight: normal">1.2.3</th>
             <th style="font-weight: normal">0~100</th>
             <th style="font-weight: normal">90</th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal"></th>
             <th style="font-weight: normal">Yes</th>
             <th style="font-weight: normal">The threshold for blobdb to force GC (by default, only blobs with more than 90% garbage data will force GC)</th>
         </tr>
         <tr>
             <th style="font-weight: normal">rocksdb.data.block_cache_size</th>
             <th style="font-weight: normal">1.0.1</th>
             <th style="font-weight: normal">0~ULLONG_MAX</th>
             <th style="font-weight: normal">8mb</th>
             <th style="font-weight: normal">8mb</th>
             <th style="font-weight: normal">8mb(x2)</th>
             <th style="font-weight: normal">No</th>
             <th style="font-weight: normal">data CF blockcache capacity</th>
         </tr>
     </table>
