start_server {tags {"swap.info"}} {
    test "SST-AGE-LIMIT" {
        r swap.info SST-AGE-LIMIT 99 1111
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 1111

        r swap.info SST-AGE-LIMIT 50 0
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 0

        r swap.info SST-AGE-LIMIT 10 -2222
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit -2222
    }

}