#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H

typedef struct gtidInterval gtidInterval;
typedef struct uuidSet uuidSet;
typedef struct gtidSet gtidSet;

typedef long long int rpl_gno;

struct gtidInterval {

    rpl_gno gno_start;

    rpl_gno gno_end;
};

struct uuidSet {

    char* rpl_sid;

    gtidInterval* gtid_intervals;
};

struct gtidSet {

    uuidSet* uuid_sets;
};

char *gtidEncode(gtidSet*);

gtidSet *gtidDecode(char*);

gtidSet *gtidAdd(gtidSet*, char*);

gtidSet *gtidRaise(gtidSet*, char*, rpl_gno);

#endif  /* __REDIS_CTRIP_GTID_H */
