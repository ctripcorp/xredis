start_server {tags {"swap bgsave"} overrides {swap-bgsave-fix-metalen-mismatch yes}} {
    set redis_host [srv 0 host]
    set redis_port [srv 0 port]
    r config set swap-debug-evict-keys 0

    test {fix all type meta.len mismatch} {
        r hmset h1 f1 v1 f2 v2 f3 v3
        r sadd s1 f1 f2 f3
        r lpush l1 f1 f2 f3
        r zadd z1 1 f1 2 f2 3 f3

        r swap.evict h1 s1 l1 z1
        wait_key_cold r h1
        wait_key_cold r s1
        wait_key_cold r l1
        wait_key_cold r z1

        r config set swap-debug-bgsave-metalen-addition 1
        r bgsave
        waitForBgsave r

        wait_key_hot r h1
        wait_key_hot r s1
        wait_key_hot r l1
        wait_key_hot r z1
    }
}

proc make_big_list {host port db key size vlen} {
    set r [redis $host $port 1 0]
    $r client setname LOAD_HANDLER
    $r select $db
    for {set i 0} {$i < $size} {incr i} {
        set v [randstring $vlen $vlen]
        $r rpush $key $v
    }
}

proc kill_load_clients {r} {
    set clients [regexp -inline -line -all {id\=[\d]+.*name\=LOAD_HANDLER} [$r client list]]
    foreach client $clients {
        set raw [regexp -inline {id\=[\d]+} $client]
        set id [string range $raw 3 end]
        $r client kill ID $id
    }
}

start_server {tags {"swap bgsave"} overrides {swap-bgsave-fix-metalen-mismatch yes}} {
    set redis_host [srv 0 host]
    set redis_port [srv 0 port]
    set used_memory [getInfoProperty [r info memory] used_memory]
    # maxmemory = used_memory + 1MB
    r config set maxmemory [expr $used_memory + 1000000]
    r config set swap-debug-evict-keys -1
    set maxmemory [r config get maxmemory]

    test {swap.load err for swap oom} {
        # fill in 5MB data
        make_big_list $redis_host $redis_port 0 test_list 5000 1024
        kill_load_clients r
        wait_load_handlers_disconnected

        set load_err [getInfoProperty [r info swap] swap_load_error_count]
        r config set swap-debug-evict-keys 0
        r config set swap-debug-bgsave-metalen-addition 1
        r config set swap-ratelimit-maxmemory-percentage 100
        r bgsave
        wait_for_condition 10 1000 {
            [getInfoProperty [r info swap] swap_load_error_count] eq [expr $load_err + 1]
        } else {
            fail "no load err found [getInfoProperty [r info swap] swap_load_error_count]"
        }

        r config set swap-debug-bgsave-metalen-addition 0
        waitForBgsave r
    }

}
