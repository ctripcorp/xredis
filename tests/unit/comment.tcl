start_server {tags {"comment"}} {
    set slave [srv 0 client]
    set slave_host [srv 0 host]
    set slave_port [srv 0 port]
    set slave [redis $slave_host $slave_port]
    
    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [redis $master_host $master_port]

        set m_r [redis $master_host $master_port 0 0]
        set s_r [redis $slave_host $slave_port 0 0]

        test {Set slave} {
            eval $slave "\"/* comment hello */\" SLAVEOF $master_host $master_port"
            wait_for_condition 50 100 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }
        
        test {no txn} {
            assert_match OK [$master "/*comment*/" FLUSHDB]
            assert_match OK [$master "/*comment*/" SET k v]
            assert_match 1 [$master "/*comment*/" HSET myhash field1 HELLO]
            assert_match HELLO [$slave "/*comment*/" HGET myhash field1]
            assert_match v [$slave "/*comment*/" GET k]
        }

        test {txn} {
            assert_match OK [$master "/*comment*/" FLUSHDB]
            assert_match OK [$master "/*comment*/" MULTI]
            assert_match QUEUED [$master "/*comment*/" SET k v]
            assert_match QUEUED [$master "/*comment*/" HSET myhash field1 HELLO]
            assert_match "OK 1" [$master "/*comment*/" EXEC]
            assert_match HELLO [$slave "/*comment*/" HGET myhash field1]
            assert_match v [$slave "/*comment*/" GET k]

            assert_match OK [$master "/*comment*/" FLUSHDB]
            assert_match OK [$master "/*comment*/" MULTI]
            assert_match QUEUED [$master "/*comment*/" SET k v]
            assert_match QUEUED [$master "/*comment*/" HSET myhash field1 HELLO]
            assert_match "OK" [$master "/*comment*/" discard]
            assert_match "" [$slave "/*comment*/" HGET myhash field1]
            assert_match "" [$slave "/*comment*/" GET k]
        }

        test {lua notxn} {
            catch {[$master eval "return redis.call('/*comment*/', 'set','foo',12345)" 1 foo]} {err}
            assert_match "*Unknown Redis command called from Lua script*" $err
        }
    }
}