#define _GNU_SOURCE
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>

#include <hiredis.h>
#include "sds.h"
#include "dict.h"
#include "zmalloc.h"


static struct config {
    char *srchostip;
    int srchostport;
    char *dsthostip;
    int dsthostport;
    int verbose;
    int trocks;
} config;

static void usage(void) {
    fprintf(stderr,
"Usage: redis-monitor-pipe --dst <hostname> [OPTIONS]\n"
"  --src <hostname>   Src server hostname (default: 127.0.0.1).\n"
"  --srcport <port>   Src server port (default: 6379).\n"
"  --dst <hostname>   Dst server hostname.\n"
"  --dstport <port>   Dst server port (default: 6379).\n"
"  --verbose          Output verbose info (default: 0).\n"
"  --trocks           Src is trocks, we use dirty compactable tricks.\n"
"  --help             Output this help and exit.\n"
);
}

static void parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;

        if (!strcmp(argv[i],"--src") && !lastarg) {
            sdsfree(config.srchostip);
            config.srchostip = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"--srcport") && !lastarg) {
            config.srchostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--dst") && !lastarg) {
            sdsfree(config.dsthostip);
            config.dsthostip = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"--dstport") && !lastarg) {
            config.dsthostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--verbose")) {
            config.verbose = 1;
        } else if (!strcmp(argv[i],"--trocks")) {
            config.trocks = 1;
        } else if (!strcmp(argv[i],"--help")) {
            usage();
            exit(0);
        } else {
            usage();
            exit(1);
        }
    }

    if (config.dsthostip == NULL) {
        usage();
        exit(1);
    }
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

/* _serverAssert is needed by dict */
void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "=== ASSERTION FAILED ===");
    fprintf(stderr, "==> %s:%d '%s' is not true",file,line,estr);
    *((char*)-1) = 'x';
}

dictType dstConnectionsType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

int commandIgnored(const char *cmd) {
    if (!strcasecmp(cmd, "replconf") ||
            !strcasecmp(cmd, "publish") ||
            !strcasecmp(cmd, "info") ||
            !strcasecmp(cmd, "config") ||
            !strcasecmp(cmd, "role") ||
            !strcasecmp(cmd, "ping") ||
            !strcasecmp(cmd, "select") ||
            !strcasecmp(cmd, "scan") ||
            !strcasecmp(cmd, "sync") ||
            !strcasecmp(cmd, "psync") ||
            !strcasecmp(cmd, "_db_name") ||
            !strcasecmp(cmd, "command") ) {
        return 1;
    } else {
        return 0;
    }
}

#define ARGS_INIT_SIZE 16

sds* splitTrocksArgs(char *line, int *pargc) {
    int argc = 0, err = 0, capacity = ARGS_INIT_SIZE;
    sds *argv = zmalloc(sizeof(sds)*ARGS_INIT_SIZE);
    char *p = line, *end = line+strlen(line), *delim = " \"";

    while (p < end) {
        char *found = strstr(p, delim);
        if (!found) {
            found = end;
        }

        if (found <= p || found[-1] != '"') {
            err = 1;
            break;
        }

        if (argc >= capacity) {
            capacity *= 2;
            argv = zrealloc(argv,sizeof(sds)*capacity);
        }

        argv[argc++] = sdsnewlen(p,found-p-1);
        p = found+2;
    }

    if (err) {
        for (int i = 0; i < argc; i++) sdsfree(argv[i]);
        zfree(argv);
        argc = 0;
        argv = NULL;
    }
    *pargc = argc;
    return argv;
}

int main(int argc, char **argv) {
    redisContext *src, *dst;
    redisReply *reply;
    dict* conns = dictCreate(&dstConnectionsType,NULL);

    long long piped_commands = 0, ignored_commands = 0;
    time_t prev_logtime = 0;

    config.srchostip = sdsnew("127.0.0.1");
    config.srchostport = 6379;
    config.dsthostip = NULL;
    config.dsthostport = 6379;
    config.verbose = 0;
    config.trocks = 0;

    parseOptions(argc,argv);

    src = redisConnect(config.srchostip,config.srchostport);
    if (src == NULL || src->err) {
        printf("connect src redis(%s:%d) failed\n",
                config.srchostip, config.srchostport);
        exit(1);
    }

    reply = redisCommand(src, "MONITOR");
    if (reply == NULL) {
        printf("monitor command failed.\n");
        exit(1);
    }

    if (reply->type != REDIS_REPLY_STATUS ||
            sdscmp(reply->str, "OK")) {
        printf("monitor reply unexpected: type=%d, str=%s.\n",
                reply->type, reply->str);
        exit(1);
    }

    freeReplyObject(reply);

    while (1) {
        sds *argv;
        int argc;
        size_t *argv_len;
        time_t now;
        void *r;
        char *p, *e, *client_start = NULL, *client_end = NULL;
        sds client_addr = NULL;

        if (redisGetReply(src,&r) == REDIS_ERR) {
            printf("waiting for monitor feeds failed.\n");
            break;
        }

        reply = (redisReply*)r;
        if (reply->type != REDIS_REPLY_STATUS) {
            printf("unpexected msg format: type=%d, str=(%s)\n", reply->type, reply->str);
            break;
        }

        p = reply->str, e = reply->str + reply->len;
        for (; p < e; p++) {
            if (*p == '[') client_start = p;
            if (*p == ']') {
                p++;
                client_end = p;
                break;
            }
        }
        if (client_start == NULL || client_end == NULL) {
            printf("client addr not found in msg: %s", reply->str);
            break;
        }

        if (!config.trocks) {
            argv = sdssplitargs(p,&argc);
        } else {
            if (p[0] != ' ' || p[1] != '"') {
                argv = NULL;
            } else {
                p += 2; /* skip first delim */
                sds line = sdsnew(p);
                argv = splitTrocksArgs(line,&argc);
                sdsfree(line);
            }
        }

        if (argv == NULL) {
            printf("unexpected msg format:%s",p);
            break;
        }

        freeReplyObject(reply);

        client_addr = sdsnewlen(client_start, client_end - client_start);
        dst = dictFetchValue(conns, client_addr);
        if (dst == NULL) {
            printf("connecting dst redis. client: %s\n", client_addr);
            dst = redisConnect(config.dsthostip, config.dsthostport);
            if (dst == NULL) {
                printf("connect dst redis failed.\n");
                break;
            }
            dictAdd(conns, client_addr, dst);
            printf("connect dst redis ok. total connections(%ld)\n", dictSize(conns));
        } else {
            sdsfree(client_addr);
        }


        if (commandIgnored(argv[0])) {
            ignored_commands++;
        } else {
            argv_len = zmalloc(sizeof(size_t)*argc);

            if (config.verbose) printf("execute:");
            for (int i = 0; i < argc; i++) {
                argv_len[i] = sdslen(argv[i]);
                if (config.verbose) printf("%s ",argv[i]);
            }
            if (config.verbose) printf("\n");

            reply = redisCommandArgv(dst, argc, (const char **)argv, argv_len);
            if (reply == NULL) {
                printf("dst reply null for command:%s.\n", argv[0]);
                break;
            }
            if (reply->type == REDIS_REPLY_ERROR) {
                printf("dst reply error(%s) for command:%s.\n",reply->str, argv[0]);
                break;
            }
            freeReplyObject(reply);

            zfree(argv_len);
            piped_commands++;
        }

        sdsfreesplitres(argv,argc);

        now = time(NULL);
        if (now > prev_logtime) {
            printf("piped(%lld), ignored(%lld), connections(%ld)\n",
                    piped_commands, ignored_commands, dictSize(conns));
            prev_logtime = now;
        }
    }
}
