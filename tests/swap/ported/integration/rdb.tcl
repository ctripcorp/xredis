start_server {} {
    test {Test FLUSHALL aborts bgsave} {
        # 1000 keys with 1ms sleep per key shuld take 1 second
        r config set rdb-key-save-delay 1000
        r debug populate 1000
        r bgsave
        assert_equal [s rdb_bgsave_in_progress] 1
        after 200
        r flushall
        # wait half a second max
        wait_for_condition 5 100 {
            [s rdb_bgsave_in_progress] == 0
        } else {
            fail "bgsave not aborted"
        }
        # veirfy that bgsave failed, by checking that the change counter is still high
        assert_lessthan 999 [s rdb_changes_since_last_save]
        # make sure the server is still writable
        catch {r set x xx}
    }
}

