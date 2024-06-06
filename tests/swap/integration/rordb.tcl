start_server {tags {"rordb replication"} overrides {}} {
    start_server {overrides {}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set expire_time [expr [clock seconds]+3600]

        $master config set swap-debug-evict-keys 0; # evict manually
        $slave  config set swap-debug-evict-keys 0; # evict manually

        test {data is consistent using fullresync by rordb} {
            # string
            $master set mystring0 myval0
            $master set mystring1 myval1
            $master set mystring2 myval2
            $master expireat mystring2 $expire_time
            $master swap.evict mystring1 mystring2
            # hash
            $master hmset myhash0 0 0 1 1 a a b b
            $master hmset myhash1 0 0 1 1 a a b b
            $master hmset myhash2 0 0 1 1 a a b b
            $master expireat myhash2 $expire_time
            $master swap.evict myhash1 myhash2
            # set
            $master sadd myset0 0 1 a b
            $master sadd myset1 0 1 a b
            $master sadd myset2 0 1 a b
            $master expireat myset2 $expire_time
            $master swap.evict myset1 myset2
            # zset
            $master zadd myzset0 0 0 1 1 2 a 3 b
            $master zadd myzset1 0 0 1 1 2 a 3 b
            $master zadd myzset2 0 0 1 1 2 a 3 b
            $master expireat myzset2 $expire_time
            $master swap.evict myzset1 myzset2
            # list
            $master rpush mylist0 0 1 a b
            $master rpush mylist1 0 1 a b
            $master rpush mylist2 0 1 a b
            $master expireat mylist2 $expire_time
            $master swap.evict mylist1 mylist2
            # bitmap
            $master setbit mybitmap0 32767 1
            $master setbit mybitmap0 65535 1
            $master setbit mybitmap0 98303 1
            $master setbit mybitmap0 131071 1
            $master setbit mybitmap0 163839 1
            $master setbit mybitmap0 196607 1
            $master setbit mybitmap0 229375 1
            $master setbit mybitmap0 262143 1
            $master setbit mybitmap0 294911 1
            $master setbit mybitmap0 327679 1
            $master setbit mybitmap0 335871 1
            $master setbit mybitmap1 32767 1
            $master setbit mybitmap1 65535 1
            $master setbit mybitmap1 98303 1
            $master setbit mybitmap1 131071 1
            $master setbit mybitmap1 163839 1
            $master setbit mybitmap1 196607 1
            $master setbit mybitmap1 229375 1
            $master setbit mybitmap1 262143 1
            $master setbit mybitmap1 294911 1
            $master setbit mybitmap1 327679 1
            $master setbit mybitmap1 335871 1
            $master setbit mybitmap2 32767 1
            $master setbit mybitmap2 65535 1
            $master setbit mybitmap2 98303 1
            $master setbit mybitmap2 131071 1
            $master setbit mybitmap2 163839 1
            $master setbit mybitmap2 196607 1
            $master setbit mybitmap2 229375 1
            $master setbit mybitmap2 262143 1
            $master setbit mybitmap2 294911 1
            $master setbit mybitmap2 327679 1
            $master setbit mybitmap2 335871 1
            $master expireat mybitmap2 $expire_time
            $master swap.evict mybitmap1 mybitmap2

            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            assert_equal [$slave dbsize] 18
            # string
            assert [object_is_hot $slave mystring0]
            assert_equal [$slave get mystring0] myval0
            assert [object_is_cold $slave mystring1]
            assert_equal [$slave get mystring1] myval1
            assert [object_is_cold $slave mystring2]
            assert {[$slave ttl mystring2] > 0}
            assert_equal [$slave get mystring2] myval2
            # hash
            assert [object_is_hot $slave myhash0]
            assert_equal [$slave hmget myhash0 0 1 a b] {0 1 a b}
            assert [object_is_cold $slave myhash1]
            assert_equal [$slave hmget myhash1 0 1 a b] {0 1 a b}
            assert [object_is_cold $slave myhash2]
            assert {[$slave ttl myhash2] > 0}
            assert_equal [$slave hmget myhash2 0 1 a b] {0 1 a b}
            # set
            assert [object_is_hot $slave myset0]
            assert_equal [lsort [$slave smembers myset0]] {0 1 a b}
            assert [object_is_cold $slave myset1]
            assert_equal [lsort [$slave smembers myset1]] {0 1 a b}
            assert [object_is_cold $slave myset2]
            assert {[$slave ttl myset2] > 0}
            assert_equal [lsort [$slave smembers myset2]] {0 1 a b}
            # zset
            assert [object_is_hot $slave myzset0]
            assert_equal [$slave zrangebyscore myzset0 -inf +inf] {0 1 a b}
            # assert [object_is_cold $slave myzset1]
            assert_equal [$slave zrangebyscore myzset1 -inf +inf] {0 1 a b}
            # assert [object_is_cold $slave myzset2]
            assert {[$slave ttl myzset2] > 0}
            assert_equal [$slave zrangebyscore myzset2 -inf +inf] {0 1 a b}
            # list
            assert [object_is_hot $slave mylist0]
            assert_equal [$slave lrange mylist0 0 -1] {0 1 a b}
            assert [object_is_cold $slave mylist1]
            assert_equal [$slave lrange mylist1 0 -1] {0 1 a b}
            assert [object_is_cold $slave mylist2]
            assert {[$slave ttl mylist2] > 0}
            assert_equal [$slave lrange mylist2 0 -1] {0 1 a b}
            # bitmap
            assert [object_is_hot $slave mybitmap0]
            assert_equal [$slave bitcount mybitmap0] {11}
            assert [object_is_cold $slave mybitmap1]
            assert_equal [$slave bitcount mybitmap1] {11}
            assert [object_is_cold $slave mybitmap2]
            assert {[$slave ttl mybitmap2] > 0}
            assert_equal [$slave bitcount mybitmap2] {11}
        }
    }
}

