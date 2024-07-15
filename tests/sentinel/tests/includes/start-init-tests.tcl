test "(start-init) Flush config and compare rewrite config file lines" {
    foreach_sentinel_id id {
        assert_match "OK" [S $id SENTINEL FLUSHCONFIG]
        set file1 ../tests/includes/sentinel.conf
        set file2 [file join "sentinel_${id}" "sentinel.conf"] 
        set fh1 [open $file1 r]
        set fh2 [open $file2 r]
        set file2_contents [read $fh2]

        while {[gets $fh1 line1]} {
            if {![regexp $line1 $file2_contents]} {
                fail "sentinel config missed"
            }
        }
        close $fh1
        close $fh2  
    }
}