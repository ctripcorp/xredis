test "can't start with aof enabled when swap enabled" {
    set server_path [tmpdir "server.swap_mode-test"]
    set stdout [format "%s/%s" $server_path "stdout"]
    set stderr [format "%s/%s" $server_path "stderr"]
    set srv [exec src/redis-server --appendonly yes >> $stdout 2>> $stderr &]
    wait_for_condition 100 20 {
        [regexp -- "FATAL CONFIG FILE ERROR" [exec cat $stderr]] == 1 &&
        [regexp -- ">>> 'appendonly \"yes\"'" [exec cat $stderr]] == 1
    } else {
        fail "swap disk + aof start success"
        kill_server $srv
    }
}

test "can't enable aof when swap enabled" {
    start_server {overrides {appendonly {no}}} {
        catch {r config set appendonly yes} error 
        assert_equal [string match {*ERR Invalid argument 'yes' for CONFIG SET 'appendonly'*} $error] 1
        assert_equal [r dbsize] 0
    }
}
