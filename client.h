/*******************************************************************************
FILE NAME
	client.h

FILE DESCRIPTION
	Header file for client application

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

	Mon Jun 20 14:01:43 EDT 2005 - Added TTL option and ignore .dot files

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/

#ifndef CLIENT_H
#define CLIENT_H

static char Sccsid_client_h[]= "@(#)client.h 0.7 06/20/2005 13:59:36";

#include <time.h>

#include "share.h"

#define DFLT_TIMEOUT	5*60
#define DFLT_INTERVAL	3
#define DFLT_WINSIZE	100
#define DFLT_REFRESH	20
#define DFLT_RETRY		3
#define DFLT_MAX_QUEUE	2000
#define DFLT_SENT_COUNT	1000

#define DISCARD_PORT	9

#define INPUT_SUBDIR_NAME	"input"
#define SENT_SUBDIR_NAME	"sent"
#define FAIL_SUBDIR_NAME	"fail"
#define TEMP_DIR_NAME		"/tmp"

/* command line options -- global */
struct {
	unsigned int	port;			/* port number for listen/connect */
	char **			host_list;		/* list of destination host and alt names */
	char *			host;			/* current destination host */
	char			debug;			/* debug flag, do not daemonize */
	char			verbosity;		/* debugging verbosity level */
	unsigned int	timeout;		/* timeout interval (on socket) */
	time_t			poll_interval;	/* input polling interval (when idle) */
	time_t			queue_ttl;		/* time to live in queue */
	int				window_size;	/* maximum outstanding acks */
	int				max_retry;		/* max number of send retries per prod */
	size_t			bufsize;		/* max size to read/write from socket */
	char **			indir_list;		/* null-terminated list of input dirs */
	char *			sent_dir;		/* holding directory for queued files */
	char *			fail_dir;		/* holding directory for failed files */
	char			wait_last_file;	/* don't send the last file */
	char			strip_ccb;		/* strip ccb headers (1 or 0) */
	time_t			refresh_interval;/* queue refresh/resort interval */
	int				max_queue_len;	/* maximum number of items to poll/sort */
	int				sent_count;		/* maximum number of files in sent dir */
	char *			connect_wmo;	/* send a connection msg with this wmo */
	char *			source;			/* string identifying this data source */
	int				shm_region;		/* shared memory region for acq_stats */
	int				host_id;		/* host_id for this datastream */
	int				link_id;		/* link_id for this datastream */
} ClientOpt;

typedef struct {
	int count;
	prod_info_t *p_head;
	prod_info_t *p_tail;
} prod_list_t;

typedef struct {
	prod_info_t *prod;
	prod_list_t	free_list;
	prod_list_t	ack_list;
	prod_list_t	retr_list;
} prod_tbl_t;

/* prototypes */
int poll_and_send(void);
int get_next_file(prod_tbl_t *p_tbl, prod_info_t *p_prod);
void retry_send(prod_info_t *p_prod);
void abort_send(prod_info_t *p_prod);
void finish_send(prod_info_t *p_prod);
int client_init(void);
int client_close(void);

#endif
