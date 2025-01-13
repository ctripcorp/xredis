# overwrite functions
if {!$::swap} {
    error "swap/ported/support/util.tcl should not be use in memory mode"
}

if {[info commands wait_for_sync] == "" || [info commands wait_for_ofs_sync] == "" || [info commands createComplexDataset] == "" || [info commands csvdump ] == ""} {
    error "support/util.tcl should be sourced before swap/ported/support/util.tcl"
}

proc wait_for_sync r {
    # 100 => 300
    wait_for_condition 50 300 {
        [status $r master_link_status] eq "up"
    } else {
        fail "replica didn't sync in time"
    }
}

proc wait_for_ofs_sync {r1 r2} {
    # 50 => 500
    wait_for_condition 500 100 {
        [status $r1 master_repl_offset] eq [status $r2 master_repl_offset]
    } else {
        fail "replica didn't sync in time"
    }
}

proc createComplexDataset {r ops {opt {}}} {
    for {set j 0} {$j < $ops} {incr j} {
        # if {$j % 100 == 0} { puts "$j: [$r dbsize]" }
        set k [randomKey]
        set k2 [randomKey]
        set f [randomValue]
        set v [randomValue]

        if {[lsearch -exact $opt useexpire] != -1} {
            if {rand() < 0.1} {
				{*}$r expire [randomKey] [expr 1+[randomInt 2]]
            }
        }

        randpath {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            randpath {set d +inf} {set d -inf}
        }
        set t [{*}$r type $k]

        if {$t eq {none}} {
            randpath {
                {*}$r set $k $v
            } {
                {*}$r lpush $k $v
            } {
                {*}$r sadd $k $v
            } {
                {*}$r zadd $k $d $v
            } {
                {*}$r hset $k $f $v
            } {
                {*}$r del $k
            }
            set t [{*}$r type $k]
        }

        switch $t {
            {string} {
                # Nothing to do
            }
            {list} {
                randpath {{*}$r lpush $k $v} \
                        {{*}$r rpush $k $v} \
                        {{*}$r lrem $k 0 $v} \
                        {{*}$r rpop $k} \
                        {{*}$r lpop $k}
            }
            {set} {
                randpath {{*}$r sadd $k $v} \
                        {{*}$r srem $k $v} \
                        {
                            set otherset [findKeyWithType {*}$r set]
                            if {$otherset ne {}} {
                                randpath {
                                    {*}$r sunionstore $k2 $k $otherset
                                } {
                                    {*}$r sinterstore $k2 $k $otherset
                                } {
                                    {*}$r sdiffstore $k2 $k $otherset
                                }
                            }
                        }
            }
            {zset} {
                randpath {{*}$r zadd $k $d $v} \
                        {{*}$r zrem $k $v} \
                        {
                            set otherzset [findKeyWithType {*}$r zset]
                            if {$otherzset ne {}} {
                                randpath {
                                    {*}$r zunionstore $k2 2 $k $otherzset
                                } {
                                    {*}$r zinterstore $k2 2 $k $otherzset
                                }
                            }
                        }
            }
            {hash} {
                randpath {{*}$r hset $k $f $v} \
                        {{*}$r hdel $k $f}
            }
        }
    }
}

# swap specific functions
proc wait_slave_online {master maxtries delay elsescript} {
    set retry $maxtries
    while {$retry} {
        set info [$master info]
        if {[string match {*slave0:*state=online*} $info]} {
            break
        } else {
            incr retry -1
            after $delay
        }
    }
    if {$retry == 0} {
        # https://github.com/google/sanitizers/issues/774 for more detail.
        puts "wait slave online timeout, that's may ok on ASan open. For ASan may hang fork child process."
        set errcode [catch [uplevel 1 $elsescript] result]
        return -code $errcode $result
    }
}

proc start_write_load_ignore_err {host port seconds} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/gen_write_load.tcl $host $port $seconds $::tls $::target_db 2> /dev/null &
}

proc press_enter_to_continue {{message "Hit Enter to continue ==> "}} {
    puts -nonewline $message
    flush stdout
    gets stdin
}

proc start_run_load {host port seconds counter code} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/swap/helpers/gen_run_load.tcl $host $port $seconds $counter $::tls $::target_db $code &
}

proc get_info_property {r section line property} {
    set str [$r info $section]
    if {[regexp ".*${line}:\[^\r\n\]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        set _ $submatch
    }
}

proc swap_object_property {str section property} {
    if {[regexp ".*${section}:\[^\r\n]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        set _ $submatch
    } else {
        set _ ""
    }
}

proc keyspace_is_empty {r} {
    if {[regexp ".*db0.*" [$r info keyspace] match]} {
        set _ 0
    } else {
        set _ 1
    }
}

proc object_is_data_dirty {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value dirty_data] == 1} {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_meta_dirty {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value dirty_meta] == 1} {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_dirty {r key} {
    if {[object_is_data_dirty $r $key] || [object_is_meta_dirty $r $key]} {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_cold {r key} {
    set str [$r swap object $key]
    if { [swap_object_property $str value at] == "" && [swap_object_property $str cold_meta swap_type] != "" } {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_warm {r key} {
    set str [$r swap object $key]
    if { [swap_object_property $str value at] == ""} {
        set _ 0
    } else {
        set sw_type [swap_object_property $str hot_meta swap_type]
        if {$sw_type == ""} {
            set _ 0
        } elseif {$sw_type == 7} {
            # maybe meta is bitmap marker
            if {[swap_object_property $str hot_meta pure_cold_subkeys_num] == 0} {
                set _ 0
            } else {
                set _ 1
            }
        } else {
            set _ 1
        }
    }
}

proc object_is_hot {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value at] != "" } {
        set sw_type [swap_object_property $str hot_meta swap_type]
        if {$sw_type == ""} {
            # no hot meta
            set _ 1
        } elseif {$sw_type == 0} {
            # string
            set _ 0
        } elseif {$sw_type == 1} {
            # list
            set _ 0
        } elseif {$sw_type == 7} {
            if {[swap_object_property $str hot_meta pure_cold_subkeys_num] == 0} {
                set _ 1
            } else {
                set _ 0
            }
        } else {
            # hash/set/zset
            if {[swap_object_property $str hot_meta len] == 0} {
                set _ 1
            } else {
                set _ 0
            }
        }
    } else {
        set _ 0
    }
}

proc object_is_bitmap {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value at] != "" } {
        if { [swap_object_property $str hot_meta swap_type] == 7 } {
            set _ 1
        } else {
            set _ 0
        }
    } else {
        if { [swap_object_property $str cold_meta swap_type] == 7 } {
            set _ 1
        } else {
            set _ 0
        }
    }
}

proc object_is_string {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value at] != "" } {
        if { [swap_object_property $str value type] == "string"} {
            if { [swap_object_property $str hot_meta swap_type] == 7 } {
                set _ 0
            } else {
                set _ 1
            }
        } else {
            set _ 0
        }
    } else {
        if { [swap_object_property $str cold_meta swap_type] == 0 } {
            set _ 1
        } else {
            set _ 0
        }
    }
}

proc bitmap_object_is_pure_hot {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value at] != "" } {
        set sw_type [swap_object_property $str hot_meta swap_type]
        if {$sw_type == 7} {
            if { [swap_object_property $str cold_meta swap_type] == "" && [swap_object_property $str hot_meta marker] == "true" } {
                set _ 1
            }
        } else {
            set _ 0
        }
    } else {
        set _ 0
    }
}

proc object_not_persisted {r key} {
    set str [$r swap object $key]
    if { [swap_object_property $str value at] != "" && [swap_object_property $str cold_meta swap_type] == ""} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_key_persist_deleted {r key} {
    wait_for_condition 500 40 {
        [object_not_persisted $r $key]
    } else {
        fail "wait $key hot failed."
    }
}

proc wait_key_cold {r key} {
    wait_for_condition 500 40 {
        [object_is_cold $r $key]
    } else {
        fail "wait $key cold failed."
    }
}

proc keyspace_is_cold {r} {
    if {[get_info_property r keyspace db0 keys] == "0"} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_key_clean {r key} {
    wait_for_condition 500 100 {
        ![object_is_dirty $r $key]
    } else {
        fail "wait $key clean failed."
    }
}

proc wait_keyspace_cold {r} {
    wait_for_condition 500 40 {
        [keyspace_is_cold $r]
    } else {
        fail "wait keyspace cold failed."
    }
}

proc wait_key_warm {r key} {
    wait_for_condition 500 40 {
        [object_is_warm $r $key]
    } else {
        fail "wait $key warm failed."
    }
}

proc object_cold_meta_len {r key} {
    set str [$r swap object $key]
    set meta_len [swap_object_property $str cold_meta len]
    if {$meta_len == ""} {
        set _ 0
    } else {
        set _ $meta_len
    }
}

proc object_hot_meta_len {r key} {
    set str [$r swap object $key]
    set meta_len [swap_object_property $str hot_meta len]
    if {$meta_len == ""} {
        set _ 0
    } else {
        set _ $meta_len
    }
}

proc object_meta_len {r key} {
    set str [$r swap object $key]
    set meta_len [swap_object_property $str hot_meta len]
    if {$meta_len == ""} {
        set meta_len [swap_object_property $str cold_meta len]
    }
    if {$meta_len != ""} {
        set _ $meta_len
    } else {
        set _ 0
    }
}

proc object_meta_pure_cold_subkeys_num {r key} {
    set str [$r swap object $key]
    set subkeys_num [swap_object_property $str hot_meta pure_cold_subkeys_num]
    if {$subkeys_num == ""} {
        set subkeys_num [swap_object_property $str cold_meta pure_cold_subkeys_num]
    }
    if {$subkeys_num != ""} {
        set _ $subkeys_num
    } else {
        set _ 0
    }
}

proc object_meta_subkey_size {r key} {
    set str [$r swap object $key]
    set subkey_size [swap_object_property $str hot_meta subkey_size]
    if {$subkey_size == ""} {
        set subkey_size [swap_object_property $str cold_meta subkey_size]
    }
    if {$subkey_size != ""} {
        set _ $subkey_size
    } else {
        set _ 0
    }
}

proc object_meta_version {r key} {
    if { [catch {$r swap object $key} e] } {
        set _ 0
    } else {
        set str [$r swap object $key]
        set meta_version [swap_object_property $str hot_meta version]
        if {$meta_version == ""} {
            set meta_version [swap_object_property $str cold_meta version]
        }
        if {$meta_version != ""} {
            set _ $meta_version
        } else {
            set _ 0
        }
    }
}

proc rio_get_meta {r key} {
    lindex [$r swap rio-get meta [$r swap encode-meta-key $key ]] 0
}

proc rio_get_data {r key version subkey} {
    lindex [$r swap rio-get data [$r swap encode-data-key $key $version $subkey]] 0
}

proc get_info {r section line} {
    set str [$r info $section]
    if {[regexp ".*${line}:(\[^\r\n\]*)\r\n" $str match submatch]} {
        set _ $submatch
    }
}

proc scan_all_keys {r} {
    set keys {}
    set cursor 0
    while {1} {
        set res [$r scan $cursor]
        set cursor [lindex $res 0]
        lappend keys {*}[split [lindex $res 1] " "]
        if {$cursor == 0} {
            break
        }
    }
    set _ $keys
}

proc data_conflict {type key subkey v1 v2} {
    if {$subkey ne ""} {
        assert_failed "\[data conflict\]\[$type\]\[$key\]\[$subkey\] '$v1' - '$v2'" ""
    } else {
        assert_failed "\[data conflict\]\[$type\]\[$key\] '$v1' - '$v2'" ""
    }
}

proc swap_data_comp {r1 r2} {
    assert_equal [$r1 dbsize] [$r2 dbsize]
    set keys [scan_all_keys $r1]
    foreach key $keys {
        set t [$r1 type {*}$key]
        set t2 [$r2 type {*}$key]
        if {$t != $t2} {
            assert_failed "key '$key' type mismatch '$t' - '$t2'" ""
        }
        switch $t {
            {string} {
                set v1 [$r1 get {*}$key]
                set v2 [$r2 get {*}$key]
                if {$v1 != $v2} {
                    data_conflict $t $key '' $v1 $v2
                }
            }
            {list} {
                set len [$r1 llen {*}$key]
                set len2 [$r2 llen {*}$key]
                if {$len != $len2} {
                    data_conflict $t $key '' 'LLEN:$len' 'LLEN:$len2'
                }
                for {set i 0} {$i < $len} {incr i} {
                    set v1 [$r1 lindex {*}$key $i]
                    set v2 [$r2 lindex {*}$key $i]
                    if {$v1 != $v2} {
                        data_conflict $t $key $i $v1 $v2
                    }
                }
            }
            {set} {
                set len [$r1 scard {*}$key]
                set len2 [$r2 scard {*}$key]
                if {$len != $len2} {
                    data_conflict $t $key '' 'SLEN:$len' 'SLEN:$len2'
                }
                set skeys [r smembers k1]
                foreach skey $skeys {
                    if {0 == [$r2 sismember $skey]} {
                        data_conflict $t $key $skey "1" "0"
                    }
                }
            }
            {zset} {
                set len [$r1 zcard {*}$key]
                set len2 [$r2 zcard {*}$key]
                if {$len != $len2} {
                    data_conflict $t $key '' 'SLEN:$len' 'SLEN:$len2'
                }
                set zcursor 0
                while 1 {
                    set res [$r1 zscan {*}$key $zcursor]
                    set zcursor [lindex $res 0]
                    set zdata [lindex $res 1]
                    set dlen [llength $zdata]
                    for {set i 0} {$i < $dlen} {incr i 2} {
                        set zkey [lindex $zdata $i]
                        set zscore [lindex $zdata [expr $i+1]]
                        set zscore2 [$r2 zscore {*}$key $zkey]
                        if {$zscore != $zscore2} {
                            data_conflict $t $key $zkey $zscore $zscore2
                        }
                    }
                    if {$zcursor == 0} {
                        break
                    }
                }
            }
            {hash} {
                set len [$r1 hlen {*}$key]
                set len2 [$r2 hlen {*}$key]
                if {$len != $len2} {
                    data_conflict $t $key '' 'HLEN:$len' 'HLEN:$len2'
                }
                set hkeys [$r1 hkeys {*}$key]
                foreach hkey $hkeys {
                    set v1 [$r1 hget {*}$key $hkey]
                    set v2 [$r2 hget {*}$key $hkey]
                    if {$v1 != $v2} {
                        data_conflict $t $key $hkey $v1 $v2
                    }
                }
            }
        }
    }
}
