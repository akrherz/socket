/*******************************************************************************
FILE NAME
	server.h

FILE DESCRIPTION
	Header file for server application

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/

#ifndef SERVER_H
#define SERVER_H

static char Sccsid_server_h[] = "@(#)server.h 0.3 09/24/2003 17:05:24";

#include <unistd.h>
#include <time.h>

#include "share.h"

#define SHORT_RETRY_SLEEP	3
#define LONG_RETRY_SLEEP	30
#define MAX_BUFSIZE			(1024*1024)

#define DFLT_TIMEOUT		(30*60)
#define DFLT_MAX_WORKER		99
#define DFLT_FILE_PERMS		(S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)

#define OVER_WRITE_FLAG		1
#define TOGGLE_PERMS_FLAG	2

#define OUTPUT_SUBDIR_NAME	"output"

struct {
	unsigned int	listen_port;
	char			debug;
	char			verbosity;
	int				max_worker;
	time_t			timeout;
	size_t			bufsize;
	char			outdir[FILENAME_LEN];
	int				outfile_flags;
	int				shm_region;
	char *			connect_wmo;	/* expect a connection msg with this wmo */
} ServOpt;

struct {
	char			wmo_ttaaii[WMO_TTAAII_LEN+1];
	char			wmo_cccc[WMO_CCCC_LEN+1];
	char			source[SOURCE_MAX_LEN+1];
	char			remotehost[HOSTNAME_MAX_LEN+1];
	int				link_id;
} ConnInfo;

char *	RemoteHost;			/* (remote) host name for client process */
int		WorkerIndex;		/* unique index for this worker */

int dispatcher(void);
void kill_workers(void);
void wait_for_worker(void);
int service(int sock_sd, char *rhost);
int get_out_path(prod_info_t *p_prod);
int finish_recv(prod_info_t *p_prod);
int abort_recv(prod_info_t *p_prod);
int serv_init(void);
int serv_close(void);

#endif
