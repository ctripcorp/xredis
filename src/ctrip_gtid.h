#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H

#include "sds.h"

typedef struct gtidInterval gtidInterval;
typedef struct uuidSet uuidSet;
typedef struct gtidSet gtidSet;

typedef long long int rpl_gno;

struct gtidInterval {
    rpl_gno gno_start;
    rpl_gno gno_end;
    gtidInterval *next;
};

struct uuidSet {
    sds rpl_sid;
    gtidInterval* intervals;
};

struct gtidSet {
    uuidSet* uuid_sets;
};

/* uuid set implementation */
gtidInterval *gtidIntervalNew(rpl_gno);
uuidSet *uuidSetNew(const char*, rpl_gno);
void uuidSetFree(uuidSet*);
int uuidSetAdd(uuidSet*, rpl_gno);

/* gtid set implementation */
gtidSet *gtidSetNew();
sds gtidEncode(gtidSet*);
gtidSet *gtidAdd(gtidSet*, const char*);
gtidSet *gtidRaise(gtidSet*, const char*, rpl_gno);

#endif  /* __REDIS_CTRIP_GTID_H */
