/*******************************************************************************
NAME
	serv_init.c

DESCRIPTION
	Initialization and shutdown routines for server.

FUNCTIONS
	serv_init			- initialization routines for server
	serv_close			- closing routines for server

HISTORY
	Last delta date and time:  06/03/2003 12:00:00
		 SCCS identifier:  0.1

*******************************************************************************/
static char Sccsid_serv_init_c[]= "@(#)serv_init.c 0.4 03/10/2004 10:28:02";

#include "share.h"
#include "server.h"

#if defined INCLUDE_CP_LOG_LIB || defined ACQ_WMO_FILE_TBL
	char	PNAME[40];
#endif

#ifdef INCLUDE_CP_LOG_LIB
#	define	LOGMAIN
#	include <fcntl.h>
	long	client_logpipe_flag;		/* global logpipe flag */
	long	client_logconsole_flag;		/* global logconsole flag */
	char	log_buff[MAX_LOG_DATA];		/* global log buffer */
	int		global_flush_duration;		/* global flush duration */
	int		global_max_prod_per_sec;	/* global max prod per sec */
#endif

int serv_init(void)
{
#	if defined INCLUDE_CP_LOG_LIB || defined ACQ_WMO_FILE_TBL
		strcpy(PNAME, Program);
#	endif

#	ifdef INCLUDE_CP_LOG_LIB
		global_origin_code = P_NWSTG_MISC;
		/* set default logging levels */
		com_log_enable(L_START|L_TERM|L_ERROR|L_DEBUG|L_STATS);
		com_log_set_lvl(global_logpipe_flag,L_ERROR|L_LVL7);
		if (ServOpt.verbosity > 0) {
			com_log_set_lvl(global_logpipe_flag,L_DEBUG|(int)ServOpt.verbosity);
		}
		if (com_log_init(CP_LOG_PIPE_NAME,O_NONBLOCK,&global_logpipe_fd) < 0) {
	   		fprintf(stderr, "%s: Fail open Log_Pipe %s\n",
	    					LOG_PREFIX, CP_LOG_PIPE_NAME);
		}
#	endif

	return 0;
}

int serv_close(void)
{
	return 0;
}

