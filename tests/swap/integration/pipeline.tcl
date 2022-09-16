
start_server {tags {"swap string"}} {
    r config set debug-evict-keys 0
    test {pipeline random exec string command} {
        set host [srv 0 host]
        set port [srv 0 port]
        set load_handler [start_run_load  $host $port 0 400 {
            # random 2 command pipeline
            randpath {
                $r write [format_command set k v]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command set k v]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command flushdb]
            } 
            after 10
            $r flush
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r write [format_command set k v]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command set k v]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command flushdb]
            } 
            after 10
            $r flush
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
    test {pipeline random exec hash command} {
        set host [srv 0 host]
        set port [srv 0 port]
        set load_handler [start_run_load  $host $port 0 400 {
            # random 2 command pipeline
            randpath {
                $r write [format_command hset h k v k1 v1]
            } {
                $r write [format_command hget h k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command hdel h k]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command hset h k v k1 v1]
            } {
                $r write [format_command hget h k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command hdel h k]
            } {
                $r write [format_command del h ]
            } {
                $r write [format_command flushdb]
            } 
            $r flush
            after 10
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r write [format_command set k v k1 v1]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command hdel h k]
            }  {
                $r write [format_command del k]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command set k v k1 v1]
            } {
                $r write [format_command get k]
            } {
                $r write [format_command evict k]
            } {
                $r write [format_command del k]
            } {
                $r write [format_command hdel h k]
            } {
                $r write [format_command flushdb]
            } 
            $r flush
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
    test {pipeline random exec zset command} {
        set host [srv 0 host]
        set port [srv 0 port]
        set load_handler [start_run_load  $host $port 0 400 {
            # random 2 command pipeline
            randpath {
                $r write [format_command zadd z 10 k1 20 k2]
            } {
                $r write [format_command zscore z k1]
            } {
                $r write [format_command evict z]
            } {
                $r write [format_command del z]
            } {
                $r write [format_command zrem z k1]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command zadd z 10 k1 20 k2]
            } {
                $r write [format_command zscore z k1]
            } {
                $r write [format_command evict z]
            } {
                $r write [format_command del z]
            } {
                $r write [format_command zrem z k1]
            } {
                $r write [format_command flushdb]
            } 
            $r flush
            after 10
        }]
        set load_handler2 [start_run_load  $host $port 0 400 {
            randpath {
                $r write [format_command zadd z 10 k1 20 k2]
            } {
                $r write [format_command zscore z k1]
            } {
                $r write [format_command evict z]
            } {
                $r write [format_command del z]
            } {
                $r write [format_command zrem z k1]
            } {
                $r write [format_command flushdb]
            } 
            randpath {
                $r write [format_command zadd z 10 k1 20 k2]
            } {
                $r write [format_command zscore z k1]
            } {
                $r write [format_command evict z]
            } {
                $r write [format_command del z]
            } {
                $r write [format_command zrem z k1]
            } {
                $r write [format_command flushdb]
            } 
            $r flush
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