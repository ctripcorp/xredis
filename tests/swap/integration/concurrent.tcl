

start_server {tags {"swap string"}} {
    r config set debug-evict-keys 0
    set host [srv 0 host]
    set port [srv 0 port]
    test {random exec string command} {
        set load_handler [start_run_load  $host $port 0 400 {
            randpath {
                $r set k v
            } {
                $r get k 
            } {
                $r evict k
            } {
                $r del k
            } {
                $r flushdb
            } 
            after 10
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r set k v2
            } {
                $r get k 
            } {
                $r evict k
            } {
                $r del k
            } {
                $r flushdb
            }
            after 10
        }]
        after 4000
        stop_write_load $load_handler
        stop_write_load $load_handler2
        after 2000
        wait_load_handlers_disconnected
        r info keyspace
    }
} 




start_server {tags {"swap hash"}} {
    r config set debug-evict-keys 0
    set host [srv 0 host]
    set port [srv 0 port]
    test {random exec hash command} {
        set load_handler [start_run_load  $host $port 0 400 {
            randpath {
                $r hset h k1 v1 
            } {
                $r hget h k1
            } {
                $r evict h
            } {
                $r del h
            } {
                $r hdel h k1
            } {
                $r flushdb
            }
            after 10
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r hset h k1 v2 
            } {
                $r hget h k1
            } {
                $r evict h
            } {
                $r del h
            } {
                $r hdel h k1
            } {
                $r flushdb
            } 
            after 10
        }]
        after 4000
        stop_write_load $load_handler
        stop_write_load $load_handler2
        after 2000
        wait_load_handlers_disconnected
        r info keyspace
    }
} 


start_server {tags {"swap zset"}} {
    r config set debug-evict-keys 0
    set host [srv 0 host]
    set port [srv 0 port]
    test {random exec zset command} {
        set load_handler [start_run_load  $host $port 0 400 {
            randpath {
                $r zadd z 10 k1
            } {
                $r zscore z k1
            } {
                $r evict z
            } {
                $r del z
            } {
                $r zrem z k1
            } {
                $r flushdb
            }
            after 10
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r zadd z 20 k1
            } {
                $r zscore z k1
            } {
                $r evict z
            } {
                $r del z
            } {
                $r zrem z k1
            } {
                $r flushdb
            }
            after 10
        }]
        after 4000
        stop_write_load $load_handler
        stop_write_load $load_handler2
        after 2000
        wait_load_handlers_disconnected
        r info keyspace
    }
} 
