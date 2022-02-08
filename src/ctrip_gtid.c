#include "server.h"

char* gtidEncode(gtidSet *gtid_set) {

    return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1-7";
}

gtid_set* gtidDecode(char* gtid) {

    return NULL;
}

gtid_set* gtidAdd(gtidSet *gtid_set, char *gtid) {

    return NULL;
}

gtidSet* gtidRaise(gtidSet *gtid_set, char *rpl_sid, rpl_gno) {

    return NULL;
}