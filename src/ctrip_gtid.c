#include "server.h"
#include "sds.h"
#include "zmalloc.h"

gtidInterval *gtidIntervalNew(rpl_gno gno) {
    gtidInterval *interval = zmalloc(sizeof(*interval));
    interval->gno_start = gno;
    interval->gno_end = gno;
    interval->next = NULL;
    return interval;
}

uuidSet *uuidSetNew(const char* rpl_sid, rpl_gno gno) {
    uuidSet *uuid_set = zmalloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = sdsnew(rpl_sid);
    uuid_set->intervals = gtidIntervalNew(gno);
    return uuid_set;
}

void uuidSetFree(uuidSet *uuid_set) {
    sdsfree(uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(NULL != cur) {
        gtidInterval *next = cur->next;
        zfree(cur);
        cur = next;
    }
    zfree(uuid_set);
}

int uuidSetAdd(uuidSet *uuid_set, rpl_gno gno) {
    return 0;
}

int gtidIntervalAdd(gtidInterval* interval, rpl_gno gno) {
    if (gno < 0) {
        return 0;
    }
    if (gno == interval->gno_start - 1) {
        interval->gno_start = gno;
        return 1;
    }
    if (gno == interval->gno_end + 1) {
        interval->gno_end = gno;
        return 1;
    }
    return 0;
}


gtidSet* gtidSetNew() {
    gtidSet *gtid_set = zmalloc(sizeof(*gtid_set));
    gtid_set->uuid_sets = NULL;
    return gtid_set;
}

sds gtidEncode(gtidSet *gtid_set) {
    if (NULL == gtid_set->uuid_sets) {
        return sdsempty();
    }
    return sdsnew("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1-7");
}

gtidSet* gtidAdd(gtidSet *gtid_set, const char *gtid) {

    return NULL;
}

gtidSet* gtidRaise(gtidSet *gtid_set, const char *rpl_sid, rpl_gno watermark) {

    return NULL;
}

#if defined(GTID_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)
int gtidTest(void) {
    {
        /* gtid unit tests*/

        gtidInterval *interval = gtidIntervalNew(9);
        test_cond("Create an new gtid interval with 9",
            interval->gno_start == 9 && interval->gno_end == 9);

        gtidIntervalAdd(interval, 10);
        gtidIntervalAdd(interval, 11);

        test_cond("And then add 10 & 11 to interval",
            interval->gno_start == 9 && interval->gno_end == 11);

        gtidSet *gtid_set = gtidSetNew();

        test_cond("Create an empty gtid set",
            memcmp(gtidEncode(gtid_set), "\0", 1) == 0);


        /* uuid unit tests*/

        uuidSet *uuid_set = uuidSetNew("A", 9);

        test_cond("Create an new uuid set with 9",
            uuid_set->intervals->gno_start == 9 && uuid_set->intervals->gno_end == 9 && uuid_set->rpl_sid == "A");

        uuidSetFree(uuid_set);
    }
    return 0;
}
#endif

#ifdef GTID_TEST_MAIN
int main(void) {
    return gtidTest();
}
#endif
