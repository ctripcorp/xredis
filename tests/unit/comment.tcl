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
            eval $slave "\"/* comment hello */\" slaveof $master_host $master_port"
            wait_for_condition 50 100 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }
        
        test {no txn} {
            assert_match OK [eval $master "/*comment*/ flushdb"]
            assert_match OK [eval $master "/*comment*/ set k v"]
            assert_match 1 [eval $master "/*comment*/ HSET myhash field1 HELLO"]
            assert_match HELLO [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match v [eval $slave "/*comment*/ get k"]
        }

        test {txn} {
            assert_match OK [eval $master "/*comment*/ flushdb"]
            assert_match OK [eval $master "/*comment*/ multi"]
            assert_match QUEUED [eval $master "/*comment*/ set k v"]
            assert_match QUEUED [eval $master "/*comment*/ HSET myhash field1 HELLO"]
            assert_match "OK 1" [eval $master "/*comment*/ exec"]
            assert_match HELLO [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match v [eval $slave "/*comment*/ get k"]

            assert_match OK [eval $master "/*comment*/ flushdb"]
            assert_match OK [eval $master "/*comment*/ multi"]
            assert_match QUEUED [eval $master "/*comment*/ set k v"]
            assert_match QUEUED [eval $master "/*comment*/ HSET myhash field1 HELLO"]
            assert_match "OK" [eval $master "/*comment*/ discard"]
            assert_match "" [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match "" [eval $slave "/*comment*/ get k"]
        }

        test {lua notxn} {
            assert_match OK [$m_r eval {return redis.call('/*comment*/', KEYS[1])} 1 flushdb]
            assert_match OK [$m_r eval {return redis.call('/*comment*/', KEYS[1], ARGV[1], ARGV[2])} 1 set k v]
            assert_match 1 [$m_r eval {return redis.call('/*comment*/', KEYS[1], ARGV[1], ARGV[2], ARGV[3])} 1 HSET myhash field1 HELLO]
            assert_match HELLO [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match v [eval $slave "/*comment*/ get k"]
        }

        test {lua txn} {
            assert_match OK [$m_r eval {return redis.call('/*comment*/', KEYS[1])} 1 flushdb]
            
            assert_match OK [eval $master "/*comment*/ flushdb"]
            assert_match OK [eval $master "/*comment*/ multi"]
            assert_match QUEUED [eval $master "/*comment*/ set k v"]
            assert_match QUEUED [eval $master "/*comment*/ HSET myhash field1 HELLO"]
            assert_match "OK 1" [eval $master "/*comment*/ exec"]
            assert_match HELLO [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match v [eval $slave "/*comment*/ get k"]

            assert_match OK [eval $master "/*comment*/ flushdb"]
            assert_match OK [eval $master "/*comment*/ multi"]
            assert_match QUEUED [eval $master "/*comment*/ set k v"]
            assert_match QUEUED [eval $master "/*comment*/ HSET myhash field1 HELLO"]
            assert_match "OK" [eval $master "/*comment*/ discard"]
            assert_match "" [eval $slave "/*comment*/ HGET myhash field1"]
            assert_match "" [eval $slave "/*comment*/ get k"]
        }
    }
}