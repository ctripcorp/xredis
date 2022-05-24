
# test cmd->getkeyrequests_proc

# flushdb
start_server { tags {"flushdb"}} {

    set master [srv 0 client]
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    test "small key" {
        $master set k v 
        $master hset h k v 
        $master flushdb 
        assert_equal [$master get k] {}
        assert_equal [$master hget h k] {}
        assert_equal [$master dbsize] 0
    }

    test "bighash" {
        
    }

    test "multi" {
        $master multi 
        $master set k v 
        $master hset h k v 
        $master flushdb
        $master exec 
        assert_equal [$master get k] {}
        assert_equal [$master hget h k] {}
        assert_equal [$master dbsize] 0
    }
}