#include <string.h>
#include "ctrip_gtid.h"
#include "sds.h"
#include "zmalloc.h"
#include "util.h"

gtidInterval *gtidIntervalNew(rpl_gno gno) {
    return gtidIntervalNewRange(gno, gno);
}

gtidInterval *gtidIntervalNewRange(rpl_gno start, rpl_gno end) {
    gtidInterval *interval = zmalloc(sizeof(*interval));
    interval->gno_start = start;
    interval->gno_end = end;
    interval->next = NULL;
    return interval;
}

gtidInterval *gtidIntervalNewSds(sds interval_sds) {
    const char *hyphen = "-";
    int count = 0;
    rpl_gno gno = 0;

    gtidInterval *interval = zmalloc(sizeof(*interval));
    interval->gno_start = 0;
    interval->gno_end = 0;
    interval->next = NULL;

    sds *v = sdssplitlen(interval_sds, sdslen(interval_sds), hyphen, 1, &count);
    if (count == 2) {
        string2ll(v[0], sdslen(v[0]), &gno);
        interval->gno_start = gno;
        string2ll(v[1], sdslen(v[1]), &gno);
        interval->gno_end = gno;
    }
    if (count == 1) {
        string2ll(v[0], sdslen(v[0]), &gno);
        interval->gno_start = interval->gno_end = gno;
    }
    sdsfreesplitres(v, count);
    return interval;
}

uuidSet *uuidSetNew(const char *rpl_sid, rpl_gno gno) {
    return uuidSetNewRange(rpl_sid, gno, gno);
}

uuidSet *uuidSetNewRange(const char *rpl_sid, rpl_gno start, rpl_gno end) {
    uuidSet *uuid_set = zmalloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = sdsnew(rpl_sid);
    uuid_set->intervals = gtidIntervalNewRange(start, end);
    uuid_set->next = NULL;
    return uuid_set;
}

uuidSet *uuidSetNewSds(sds uuid_set_sds) {
    const char *colon = ":";
    int count = 0;
    sds *v = sdssplitlen(uuid_set_sds, sdslen(uuid_set_sds), colon, 1, &count);
    if (count <= 1) {
        sdsfreesplitres(v, count);
        return NULL;
    }

    uuidSet *uuid_set = zmalloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = sdsdup(v[0]);
    uuid_set->intervals = NULL;
    uuid_set->next = NULL;

    for (int i = count - 1; i > 0; i--) {
        gtidInterval* interval = gtidIntervalNewSds(v[i]);
        interval->next = uuid_set->intervals;
        uuid_set->intervals = interval;
    }
    sdsfreesplitres(v, count);
    return uuid_set;
}

void uuidSetFree(uuidSet *uuid_set) {
    sdsfree(uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        gtidInterval *next = cur->next;
        zfree(cur);
        cur = next;
    }
    zfree(uuid_set);
}

sds uuidSetEncode(uuidSet *uuid_set, sds src) {
    src = sdscat(src, uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        src = sdscat(src, ":");
        if (cur->gno_start == cur->gno_end) {
            src = sdscatfmt(src, "%I", cur->gno_start);
        } else {
            src = sdscatfmt(src, "%I-%I", cur->gno_start, cur->gno_end);
        }
        cur = cur->next;
    }
    return src;
}

int uuidSetAdd(uuidSet *uuid_set, rpl_gno gno) {
    gtidInterval *cur = uuid_set->intervals;
    gtidInterval *next = cur->next;
    if (gno < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalNew(gno);
        uuid_set->intervals->next = cur;
        return 1;
    }
    if (gno == cur->gno_start - 1) {
        cur->gno_start = gno;
        return 1;
    }
    while(next != NULL) {
        if (gno == cur->gno_end + 1) {
            if (gno == next->gno_start - 1) {
                cur->gno_end = next->gno_end;
                cur->next = next->next;
                zfree(next);
                return 1;
            } else {
                cur->gno_end = gno;
                return 1;
            }
        }
        if (gno == next->gno_start - 1) {
            next->gno_start = gno;
            return 1;
        }
        if (gno < next->gno_start - 1 && gno > cur->gno_end + 1) {
            cur->next = gtidIntervalNew(gno);
            cur->next->next = next;
            return 1;
        }
        if (gno <= next->gno_end) {
            return 0;
        }
        cur = next;
        next = cur->next;
    }
    if (gno == cur->gno_end + 1) {
        cur->gno_end = gno;
        return 1;
    }
    if (gno > cur->gno_end + 1) {
        cur->next = gtidIntervalNew(gno);
        return 1;
    }
    return 0;
}

void uuidSetRaise(uuidSet *uuid_set, rpl_gno watermark) {
    gtidInterval *cur = uuid_set->intervals;
    if (watermark < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
        uuid_set->intervals->next = cur;
        return;
    }

    while (cur != NULL) {
        if (watermark > cur->gno_end + 1) {
            gtidInterval *temp = cur;
            cur = cur->next;
            zfree(temp);
            continue;
        }

        if (watermark == cur->gno_end + 1) {
            if (cur->next == NULL) {
                cur->gno_start = 1;
                cur->gno_end = watermark;
                uuid_set->intervals = cur;
                break;
            }
            if (watermark == cur->next->gno_start - 1) {
                gtidInterval *prev = cur;
                cur = cur->next;
                zfree(prev);
                cur->gno_start = 1;
                break;
            } else {
                cur->gno_end = watermark;
                cur->gno_start = 1;
                break;
            }
        }
        if (watermark < cur->gno_start - 1) {
            gtidInterval *temp = cur;
            cur = gtidIntervalNewRange(1, watermark);
            cur->next = temp;
            break;
        } else {
            cur->gno_start = 1;
            break;
        }
    }
    if (cur == NULL) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
    } else {
        uuid_set->intervals = cur;
    }
}

int uuidSetContains(uuidSet *uuid_set, rpl_gno gno) {
    gtidInterval *cur = uuid_set->intervals;
    while (cur != NULL) {
        if (gno >= cur->gno_start && gno <= cur->gno_end) {
            return 1;
        }
        cur = cur->next;
    }
    return 0;
}

rpl_gno uuidSetNext(uuidSet *uuid_set, int updateBeforeReturn) {
    if (uuid_set->intervals == NULL) {
        if (updateBeforeReturn) {
            uuid_set->intervals = gtidIntervalNew(1);
        }
        return 1;
    }

    rpl_gno next;
    if (uuid_set->intervals->gno_start > 1) {
        next = 1;
    } else {
        next = uuid_set->intervals->gno_end + 1;
    }
    if (updateBeforeReturn) {
        uuidSetAdd(uuid_set, next);
    }
    return next;
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = zmalloc(sizeof(*gtid_set));
    gtid_set->uuid_sets = NULL;
    gtid_set->tail = NULL;
    return gtid_set;
}

void gtidSetAppendUuidSet(gtidSet *gtid_set, uuidSet *uuid_set) {
    if (gtid_set->uuid_sets == NULL) {
        gtid_set->uuid_sets = uuid_set;
        gtid_set->tail = uuid_set;
    } else {
        gtid_set->tail->next = uuid_set;
        gtid_set->tail = uuid_set;
    }
}

gtidSet *gtidSetNewSds(sds src) {
    gtidSet* gtid_set = gtidSetNew();
    const char *split = ",";
    int count = 0;
    sds *v = sdssplitlen(src, sdslen(src), split, 1, &count);
    for(int i = 0; i < count; i++) {
        uuidSet *uuid_set = uuidSetNewSds(v[i]);
        gtidSetAppendUuidSet(gtid_set, uuid_set);
    }
    sdsfreesplitres(v, count);
    return gtid_set;
}

void gtidSetFree(gtidSet *gtid_set) {
    uuidSet *next;
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        next = cur->next;
        uuidSetFree(cur);
        cur = next;
    }
    zfree(gtid_set);
}

sds gtidSetEncode(gtidSet *gtid_set, sds src) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        src = uuidSetEncode(cur, src);
        cur = cur->next;
        if (cur != NULL) {
            src = sdscat(src, ",");
        }
    }
    return src;
}

int gtidSetAdd(gtidSet *gtid_set, const char *rpl_sid, rpl_gno gno) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    if (cur == NULL) {
        cur = uuidSetNew(rpl_sid, gno);
        gtidSetAppendUuidSet(gtid_set, cur);
        return 1;
    } else {
        return uuidSetAdd(cur, gno);
    }
}

int gtidSetAddGtidSds(gtidSet *gtid_set, sds gtid_sds) {
    const char *split = ":";
    rpl_gno gno = 0;
    int result = 0;
    int count = 0;
    sds *v = sdssplitlen(gtid_sds, sdslen(gtid_sds), split, 1, &count);
    if(count == 2) {
        string2ll(v[1], sdslen(v[1]), &gno);
        result = gtidSetAdd(gtid_set, v[0], gno);
    }
    sdsfreesplitres(v, count);
    return result;
}

void gtidSetRaise(gtidSet *gtid_set, const char *rpl_sid, rpl_gno watermark) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    if (cur == NULL) {
        cur = uuidSetNewRange(rpl_sid, 1, watermark);
        gtidSetAppendUuidSet(gtid_set, cur);
    } else {
        uuidSetRaise(cur, watermark);
    }
}

sds gtidSetNext(gtidSet *gtid_set, const char *rpl_sid, sds src, int updateBeforeReturn) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    src = sdscat(src, rpl_sid);
    src = sdscat(src, ":");

    rpl_gno gno;
    if (cur == NULL) {
        gno = 1;
        if (updateBeforeReturn) {
            cur = uuidSetNew(rpl_sid, gno);
            gtidSetAppendUuidSet(gtid_set, cur);
        }
    } else {
        gno = uuidSetNext(cur, updateBeforeReturn);
    }
    src = sdscatfmt(src, "%I", gno);
    return src;
}

int gtidSetContains(gtidSet *gtid_set, sds gtid_sds) {
    const char *split = ":";
    sds rpl_sid;
    rpl_gno gno = 0;
    int count = 0;
    int result = 0;
    sds *v = sdssplitlen(gtid_sds, sdslen(gtid_sds), split, 1, &count);
    if(count != 2) {
        goto END;
    }
    rpl_sid = v[0];
    string2ll(v[1], sdslen(v[1]), &gno);

    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    if (cur == NULL) {
        goto END;
    }
    result = uuidSetContains(cur, gno);

    END:
    sdsfreesplitres(v, count);
    return result;
}

#if defined(GTID_TEST_MAIN)
#include <stdio.h>
#include <stdlib.h>
#include "testhelp.h"
#include "limits.h"

int gtidTest(void) {
    {
        /* gtid unit tests*/

        gtidSet *gtid_set = gtidSetNew();
        sds gtidset = sdsnew("");
        gtidset = gtidSetEncode(gtid_set, gtidset);
        test_cond("Create an empty gtid set",
            memcmp(gtidset, "\0", 1) == 0);

        sdsfree(gtidset);
        gtidSetAdd(gtid_set, "A", 1);
        gtidSetAdd(gtid_set, "A", 2);
        gtidSetAdd(gtid_set, "B", 3);
        test_cond("Add A:1 A:2 B:3 to empty gtid set",
            memcmp(gtid_set->uuid_sets->rpl_sid, "A\0", 1) == 0
            && gtid_set->uuid_sets->intervals->gno_start == 1 && gtid_set->uuid_sets->intervals->gno_end == 2
            && memcmp(gtid_set->uuid_sets->next->rpl_sid, "B\0", 1) == 0
            && gtid_set->uuid_sets->next->intervals->gno_start == 3 && gtid_set->uuid_sets->next->intervals->gno_end == 3);

        gtidset = sdsnew("");
        gtidset = gtidSetEncode(gtid_set, gtidset);
        test_cond("Add A:1 A:2 B:3 to empty gtid set (encode)",
            strcmp(gtidset, "A:1-2,B:3") == 0);

        sdsfree(gtidset);
        gtidSetAdd(gtid_set, "B", 7);
        gtidSetRaise(gtid_set, "A", 5);
        gtidSetRaise(gtid_set, "B", 5);
        gtidSetRaise(gtid_set, "C", 10);
        gtidset = sdsnew("");
        gtidset = gtidSetEncode(gtid_set, gtidset);
        test_cond("Raise A & B to 5, C to 10, towards C:1-10,B:3:7,A:1-2",
            strcmp(gtidset, "A:1-5,B:1-5:7,C:1-10") == 0);

        sds gtid_sds = sdsnew("A:5");
        test_cond("add A:5 to A:1-5,B:1-5:7,C:1-10",
             gtidSetAddGtidSds(gtid_set, gtid_sds) == 0);

        sdsfree(gtid_sds);
        sdsfree(gtidset);
        gtidSetFree(gtid_set);
        gtidset = sdsnew("A:1-7,B:9:11-13:20");
        gtid_set = gtidSetNewSds(gtidset);
        sds expected = sdsnew("");
        expected = gtidSetEncode(gtid_set, expected);
        test_cond("encode & decode A:1-7,B:9:11-13:20",
            strcmp(expected, gtidset) == 0);

        sdsfree(gtidset);
        sdsfree(expected);
        gtidSetFree(gtid_set);
        gtidset = sdsnew("A:1-7,B:9:11-13:20");
        gtid_set = gtidSetNewSds(gtidset);
        sds next = sdsnew("");
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:9:11-13:20 & Update",
            strcmp("B:1", next) == 0 && gtidSetContains(gtid_set, next));

        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:1:9:11-13:20 & Update",
            strcmp("B:2", next) == 0 && gtidSetContains(gtid_set, next));

        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:1-2:9:11-13:20 5 times & Update",
            strcmp("B:7", next) == 0 && gtidSetContains(gtid_set, next));

        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:1-7:9:11-13:20 & Update",
            strcmp("B:8", next) == 0 && gtidSetContains(gtid_set, next));

        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:1-9:11-13:20 & Update",
            strcmp("B:10", next) == 0 && gtidSetContains(gtid_set, next));

        sdsclear(next);
        next = gtidSetNext(gtid_set, "B", next, 1);
        test_cond("B next of A:1-7,B:1-13:20 & Update",
            strcmp("B:14", next) == 0 && gtidSetContains(gtid_set, next));

        gtid_sds = sdsnew("C:1");
        test_cond("C:1 not in A:1-7,B:1-14:20",
            gtidSetContains(gtid_set, gtid_sds) == 0);

        sdsfree(gtid_sds);
        gtid_sds = sdsnew("B:15");
        test_cond("C:1 not in A:1-7,B:1-14:20",
            gtidSetContains(gtid_set, gtid_sds) == 0);

        sdsfree(gtid_sds);
        sdsclear(next);
        sdsfree(next);
        gtidSetFree(gtid_set);

        /* uuid unit tests*/
        uuidSet *uuid_set;

        uuid_set = uuidSetNew("A", 9);
        test_cond("Create an new uuid set with 9",
            uuid_set->intervals->gno_start == 9 && uuid_set->intervals->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 7);
        test_cond("Add 7 to 9",
            uuid_set->intervals->gno_start == 7 && uuid_set->intervals->gno_end == 7
            && uuid_set->intervals->next->gno_start == 9 && uuid_set->intervals->next->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 9);
        uuidSetAdd(uuid_set, 8);
        test_cond("Add 8 to 9",
            uuid_set->intervals->gno_start == 8 && uuid_set->intervals->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 6);
        test_cond("Add 6 to 8-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next->gno_start == 8 && uuid_set->intervals->next->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        test_cond("Add 8 to 8-9",
            uuidSetAdd(uuid_set, 8) == 0);

        uuidSetAdd(uuid_set, 7);
        test_cond("Add 7 to 6,8-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 9
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 100);
        test_cond("Add 100 to 6-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 9
            && uuid_set->intervals->next->gno_start == 100 && uuid_set->intervals->next->gno_end == 100
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 1);
        uuidSetAdd(uuid_set, 5);
        uuidSetAdd(uuid_set, 6);
        uuidSetAdd(uuid_set, 11);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 20);
        uuidSetAdd(uuid_set, 19);
        uuidSetAdd(uuid_set, 1);
        uuidSetAdd(uuid_set, 12);
        uuidSetAdd(uuid_set, 3);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 14);
        uuidSetAdd(uuid_set, 12);
        test_cond("Manual created case: result should be A:1:3:5-6:11-14:19-20",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 1
            && uuid_set->intervals->next->gno_start == 3 && uuid_set->intervals->next->gno_end == 3
            && uuid_set->intervals->next->next->gno_start == 5 && uuid_set->intervals->next->next->gno_end == 6
            && uuid_set->intervals->next->next->next->gno_start == 11 && uuid_set->intervals->next->next->next->gno_end == 14
            && uuid_set->intervals->next->next->next->next->gno_start == 19 && uuid_set->intervals->next->next->next->next->gno_end == 20
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        sds uuidset = sdsnew("");
        uuidset = uuidSetEncode(uuid_set, uuidset);
        test_cond("Manual created case (encode): result should be A:1:3:5-6:1-14:19-20",
            strcmp(uuidset, "A:1:3:5-6:11-14:19-20") == 0);

        sdsfree(uuidset);
        uuidSetRaise(uuid_set, 30);
        test_cond("Manual created case (raise to 30)",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 30
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 6);
        uuidSetAdd(uuid_set, 8);
        uuidSetAdd(uuid_set, 9);
        test_cond("add 9 to 5-6,8-9",
            uuidSetAdd(uuid_set, 9) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 6);
        test_cond("next of A:5-6 is 1", uuidSetNext(uuid_set, 0) == 1);

        test_cond("add 6 to 5-6",
            uuidSetAdd(uuid_set, 6) == 0);

        uuidSetNext(uuid_set, 1);
        uuidset = sdsnew("");
        uuidset = uuidSetEncode(uuid_set, uuidset);
        test_cond("update next of A:5-6, will be A:1:5-6",
            strcmp(uuidset, "A:1:5-6") == 0);

        sdsfree(uuidset);
        uuidSetNext(uuid_set, 1);
        uuidset = sdsnew("");
        uuidset = uuidSetEncode(uuid_set, uuidset);
        test_cond("update next of A:1:5-6, will be A:1-2:5-6",
            strcmp(uuidset, "A:1-2:5-6") == 0);

        sdsfree(uuidset);
        uuidSetNext(uuid_set, 1);
        uuidSetNext(uuid_set, 1);
        uuidSetNext(uuid_set, 1);
        uuidset = sdsnew("");
        uuidset = uuidSetEncode(uuid_set, uuidset);
        test_cond("update next 3 times of A:1-2:5-6, will be A:1-7",
            strcmp(uuidset, "A:1-7") == 0);

        sdsfree(uuidset);
        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetRaise(uuid_set, 3);
        test_cond("raise to 3 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 3
            && uuid_set->intervals->next->gno_start == 5 && uuid_set->intervals->next->gno_end == 5
            && uuid_set->intervals->next->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        test_cond("next of A:5 is 1", uuidSetNext(uuid_set, 0) == 1);

        uuidSetRaise(uuid_set, 4);
        test_cond("raise to 4 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 5
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetRaise(uuid_set, 6);
        test_cond("raise to 6 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 7);
        uuidSetRaise(uuid_set, 6);
        test_cond("raise to 6 towards A:5:7",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 7
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);
        test_cond("next of A:1-7 is 8", uuidSetNext(uuid_set, 0) == 8);

        uuidSetFree(uuid_set);
        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 8);
        uuidSetRaise(uuid_set, 6);
        test_cond("raise to 6 towards A:5:8",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next->gno_start == 8 && uuid_set->intervals->next->gno_end == 8
            && uuid_set->intervals->next->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);
        test_cond("next of A:1-6:8 is 7", uuidSetNext(uuid_set, 0) == 7);
        test_cond("1 is in A:1-6:8", uuidSetContains(uuid_set, 1) == 1);
        test_cond("3 is in A:1-6:8", uuidSetContains(uuid_set, 3) == 1);
        test_cond("6 is in A:1-6:8", uuidSetContains(uuid_set, 6) == 1);
        test_cond("7 is not in A:1-6:8", uuidSetContains(uuid_set, 7) == 0);
        test_cond("8 is in A:1-6:8", uuidSetContains(uuid_set, 8) == 1);
        test_cond("30 is not in A:1-6:8", uuidSetContains(uuid_set, 30) == 0);

        uuidSetFree(uuid_set);

        sds uuid_set_sds = sdsnew("A:1-6:8");
        uuid_set = uuidSetNewSds(uuid_set_sds);
        test_cond("new uuid_set from A:1-6:8",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next->gno_start == 8 && uuid_set->intervals->next->gno_end == 8
            && uuid_set->intervals->next->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
        sdsfree(uuid_set_sds);

        /* interval unit tests*/

        gtidInterval *interval = gtidIntervalNew(9);
        test_cond("Create an new gtid interval with 9",
            interval->gno_start == 9 && interval->gno_end == 9);

        zfree(interval);
        interval = gtidIntervalNewRange(1,9);
        test_cond("Create an new gtid interval with 1 to 9",
            interval->gno_start == 1 && interval->gno_end == 9);

        zfree(interval);
        sds interval_sds = sdsnew("1-9");
        interval = gtidIntervalNewSds(interval_sds);
        test_cond("Create an new gtid interval with 1-9",
            interval->gno_start == 1 && interval->gno_end == 9);

        sdsfree(interval_sds);
        zfree(interval);
        interval_sds = sdsnew("7");
        interval = gtidIntervalNewSds(interval_sds);
        test_cond("Create an new gtid interval with 7",
            interval->gno_start == 7 && interval->gno_end == 7);

        sdsfree(interval_sds);
        zfree(interval);

    }
    test_report()
    return 0;
}
#endif

#ifdef GTID_TEST_MAIN
int main(void) {
    return gtidTest();
}
#endif
