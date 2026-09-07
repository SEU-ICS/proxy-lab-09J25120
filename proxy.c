/*
 * proxy.c - CSAPP Proxy Lab 完整实现
 *
 * 组成:
 *   main        : 监听端口 + 每连接一线程 (并发)
 *   doit        : 转发核心 (含缓存命中检查)
 *   parse_uri   : 解析完整 URL -> hostname / port
 *   get_path    : 从 URL 提取路径
 *   read_requesthdrs : 读取并丢弃其余请求头
 *   clienterror : 错误响应 (教材 tiny.c 标准实现)
 *   缓存        : 读者-写者锁 + LRU 淘汰 (教材 12.5.2 模式)
 *
 * 用法: ./proxy <port>
 */
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include "csapp.h"

/* Recommended max cache and object sizes */
#define MAX_CACHE_SIZE 1049000
#define MAX_OBJECT_SIZE 102400
#define MAX_CACHE_NUM 10

/* 请求头模板 */
static const char *user_agent_hdr = "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:10.0.3) Gecko/20120305 Firefox/10.0.3\r\n";
static const char *accept_hdr = "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n";

/* ---- 函数声明 ---- */
void doit(int connfd);
void read_requesthdrs(rio_t *rp);
int parse_uri(char *uri, char *hostname, int *port);
void get_path(char *uri, char *path);
void clienterror(int fd, char *cause, char *errnum, char *shortmsg, char *longmsg);
void *thread(void *vargp);

/* ---- 缓存数据结构 ---- */
typedef struct {
    int valid;                 /* 是否有效 */
    char uri[MAXLINE];         /* 缓存键: 完整 URI */
    char obj[MAX_OBJECT_SIZE]; /* 缓存内容 */
    int size;                  /* 内容长度 */
    int lastuse;               /* LRU 时间戳 */
} cache_line_t;

static cache_line_t cache[MAX_CACHE_NUM];
static int readcnt = 0;        /* 读者数量 */
static sem_t w_mutex, cnt_mutex; /* 写者锁 / 保护 readcnt */
static int gclock = 0;         /* LRU 全局时钟 */

void init_cache(void);
int cache_lookup(char *uri, char *obj, int *size);
void cache_store(char *uri, char *obj, int size);

/* ================= main: 并发服务器 ================= */
int main(int argc, char **argv)
{
    int listenfd, *connfd;
    socklen_t clientlen;
    struct sockaddr_storage clientaddr;
    char hostname[MAXLINE], port[MAXLINE];
    pthread_t tid;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    signal(SIGPIPE, SIG_IGN);  /* 忽略 SIGPIPE, 避免写已关闭连接时进程被杀 */
    init_cache();
    listenfd = Open_listenfd(argv[1]);

    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Malloc(sizeof(int));       /* 每个连接独立分配, 传给线程 */
        *connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        Getnameinfo((SA *)&clientaddr, clientlen,
                    hostname, MAXLINE, port, MAXLINE, 0);
        printf("Accepted connection from (%s, %s)\n", hostname, port);
        Pthread_create(&tid, NULL, thread, connfd);
    }
    return 0;
}

/* 线程例程: 处理一个连接后自动释放 */
void *thread(void *vargp)
{
    int connfd = *((int *)vargp);
    Pthread_detach(pthread_self());
    Free(vargp);
    doit(connfd);
    Close(connfd);
    return NULL;
}

/* ================= doit: 转发核心 ================= */
void doit(int connfd)
{
    char buf[MAXLINE], method[MAXLINE], uri[MAXLINE], version[MAXLINE];
    char hostname[MAXLINE], port[16], path[MAXLINE];
    char request[MAXLINE];
    char response[MAX_OBJECT_SIZE];  /* 用于缓存 */
    char chunk[MAXLINE];             /* 回传缓冲 */
    int serverfd, port_int;
    rio_t rio, server_rio;
    int cache_size = 0, hit_size = 0;
    ssize_t n;

    /* 读请求行 */
    Rio_readinitb(&rio, connfd);
    if (Rio_readlineb(&rio, buf, MAXLINE) <= 0)
        return;
    printf("%s", buf);
    sscanf(buf, "%s %s %s", method, uri, version);

    /* 只支持 GET */
    if (strcasecmp(method, "GET")) {
        clienterror(connfd, method, "501", "Not Implemented",
                    "Proxy does not implement this method");
        return;
    }
    read_requesthdrs(&rio);  /* 丢弃其余请求头 */

    /* 缓存命中则直接返回 */
    if (cache_lookup(uri, response, &hit_size)) {
        printf("Cache hit for %s\n", uri);
        Rio_writen(connfd, response, hit_size);
        return;
    }

    /* 解析 URI, 连接源服务器 */
    parse_uri(uri, hostname, &port_int);
    sprintf(port, "%d", port_int);
    get_path(uri, path);

    serverfd = Open_clientfd(hostname, port);

    /* 构造转发请求 (HTTP/1.0 + Connection: close, 避免 keep-alive 挂住) */
    sprintf(request,
            "GET %s HTTP/1.0\r\n"
            "Host: %s\r\n"
            "%s"
            "%s"
            "Connection: close\r\n"
            "\r\n",
            path, hostname, user_agent_hdr, accept_hdr);
    Rio_writen(serverfd, request, strlen(request));

    /* 接收响应: 回传客户端, 同时缓冲 (不超过 MAX_OBJECT_SIZE) */
    Rio_readinitb(&server_rio, serverfd);
    while ((n = Rio_readnb(&server_rio, chunk, MAXLINE)) > 0) {
        Rio_writen(connfd, chunk, n);
        if (cache_size + n <= MAX_OBJECT_SIZE) {
            memcpy(response + cache_size, chunk, n);
            cache_size += n;
        }
    }
    Close(serverfd);

    /* 缓存整个响应 (含头), 供后续命中 */
    if (cache_size > 0)
        cache_store(uri, response, cache_size);
}

/* ================= URI 解析 ================= */
/* 解析完整 URL, 提取 hostname 和端口(默认 80) */
int parse_uri(char *uri, char *hostname, int *port)
{
    char *hostbegin, *hostend;
    int len;

    *port = 80;
    hostbegin = strstr(uri, "//");
    if (hostbegin)
        hostbegin += 2;
    else
        hostbegin = uri;

    hostend = hostbegin;
    while (*hostend && *hostend != ':' && *hostend != '/')
        hostend++;

    len = hostend - hostbegin;
    strncpy(hostname, hostbegin, len);
    hostname[len] = '\0';

    if (*hostend == ':')
        *port = atoi(hostend + 1);

    return 0;
}

/* 从完整 URL 提取路径 (无路径则返回 "/") */
void get_path(char *uri, char *path)
{
    char *hostbegin, *p;

    hostbegin = strstr(uri, "//");
    if (hostbegin) {
        p = strchr(hostbegin + 2, '/');
        if (p)
            strcpy(path, p);
        else
            strcpy(path, "/");
    } else {
        p = strchr(uri, '/');
        if (p)
            strcpy(path, p);
        else
            strcpy(path, "/");
    }
}

/* 读取并丢弃其余请求头 (教材 tiny.c) */
void read_requesthdrs(rio_t *rp)
{
    char buf[MAXLINE];
    Rio_readlineb(rp, buf, MAXLINE);
    while (strcmp(buf, "\r\n")) {
        Rio_readlineb(rp, buf, MAXLINE);
    }
}

/* 错误响应 (教材 tiny.c 标准实现) */
void clienterror(int fd, char *cause, char *errnum, char *shortmsg, char *longmsg)
{
    char buf[MAXLINE], body[MAXBUF];

    sprintf(body, "<html><title>Proxy Error</title>");
    sprintf(body, "%s<body bgcolor=""ffffff"">\r\n", body);
    sprintf(body, "%s%s: %s\r\n", body, errnum, shortmsg);
    sprintf(body, "%s<p>%s: %s\r\n", body, longmsg, cause);
    sprintf(body, "%s<hr><em>The Proxy Web server</em>\r\n", body);

    sprintf(buf, "HTTP/1.0 %s %s\r\n", errnum, shortmsg);
    Rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-type: text/html\r\n");
    Rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-length: %d\r\n\r\n", (int)strlen(body));
    Rio_writen(fd, buf, strlen(buf));
    Rio_writen(fd, body, strlen(body));
}

/* ================= 缓存: 读者-写者锁 + LRU ================= */
void init_cache(void)
{
    int i;
    Sem_init(&w_mutex, 0, 1);
    Sem_init(&cnt_mutex, 0, 1);
    for (i = 0; i < MAX_CACHE_NUM; i++) {
        cache[i].valid = 0;
        cache[i].size = 0;
        cache[i].lastuse = 0;
        cache[i].uri[0] = '\0';
    }
}

/* 查找缓存: 命中返回 1 并复制内容, 未命中返回 0 */
int cache_lookup(char *uri, char *obj, int *size)
{
    int i, hit = -1;

    P(&cnt_mutex);
    readcnt++;
    if (readcnt == 1) P(&w_mutex);   /* 第一个读者拿写者锁 */
    V(&cnt_mutex);

    for (i = 0; i < MAX_CACHE_NUM; i++) {
        if (cache[i].valid && strcmp(cache[i].uri, uri) == 0) {
            hit = i;
            break;
        }
    }
    if (hit >= 0) {
        memcpy(obj, cache[hit].obj, cache[hit].size);
        *size = cache[hit].size;
        cache[hit].lastuse = gclock++;
    }

    P(&cnt_mutex);
    readcnt--;
    if (readcnt == 0) V(&w_mutex);   /* 最后一个读者释放写者锁 */
    V(&cnt_mutex);

    return hit >= 0;
}

/* 写入缓存: 对象过大不存; 满则 LRU 淘汰 */
void cache_store(char *uri, char *obj, int size)
{
    int i, victim = -1;
    int minuse, minidx = 0;

    if (size <= 0 || size > MAX_OBJECT_SIZE)
        return;

    P(&w_mutex);

    /* 找空闲行 */
    for (i = 0; i < MAX_CACHE_NUM; i++) {
        if (!cache[i].valid) {
            victim = i;
            break;
        }
    }
    /* 全满: LRU 淘汰 (lastuse 最小者) */
    if (victim < 0) {
        minuse = cache[0].lastuse;
        for (i = 1; i < MAX_CACHE_NUM; i++) {
            if (cache[i].lastuse < minuse) {
                minuse = cache[i].lastuse;
                minidx = i;
            }
        }
        victim = minidx;
    }

    strcpy(cache[victim].uri, uri);
    memcpy(cache[victim].obj, obj, size);
    cache[victim].size = size;
    cache[victim].valid = 1;
    cache[victim].lastuse = gclock++;

    V(&w_mutex);
}
