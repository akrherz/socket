/*******************************************************************************
FILE NAME
	serv_symlink.c

FILE DESCRIPTION
	Product storage and handling routines

FUNCTIONS
	get_out_path	- get output path for product
	finish_recv		- product post-processing
	abort_recv		- product abort processing

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_serv_symlink_c[]= "@(#)serv_symlink.c 0.2 08/04/2003 13:13:12";

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <co/include/acq_wmo_file_tbl.h>
#include <co/include/cp_product.h>
#include <co/include/sbnstruct.h>

#define LOGMAIN
#include <co/include/cp_log.h>
#undef LOG_ERROR

#include "share.h"
#include "server.h"

#define LOG_PREFIX	Program
#define LOG_ERROR	com_vlog
#define ERROR_FP	(L_ERROR|L_LVL0)
#define LOG_DEBUG	com_vlog
#define DEBUG_FP	(L_DEBUG|L_LVL0)

#define MAX_WMO_LEN	64

long	client_logpipe_flag;	/* global logpipe flag */
long	client_logconsole_flag;	/* global logconsole flag */
char	log_buff[MAX_LOG_DATA];	/* global log buffer */
int		global_flush_duration;	/* global flush duration */
int		global_max_prod_per_sec;	/* global max prod per sec */

WMO_FILE_TABLE *p_WFtab;
WMO_FILE_INFO WFinfo;

char *PNAME = Program;

/*******************************************************************************
FUNCTION NAME
	int get_out_path(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Create pathname for output file based on product information.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			O	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	int				outdir			I	output directory
	int				shm_region		I	shared memory region

RETURNS
	 0	Normal return
*******************************************************************************/
int get_out_path(prod_info_t *p_prod)
{
	static char wmo_buf[MAX_WMO_LEN];
	static char subdir_buf[MAX_FILE_NAME];
	static char link_buf[MAX_FILE_NAME];
	static char name_buf[MAX_FILE_NAME];
	static int prod_type;
	static int sbn_type;
	static int debug_buff_flag = 0;
	static int debug_prod_flag = 0;

	while (!p_WFtab) {
	
		if (!(p_WFtab = acq_wmo_attach_tbl(ServOpt.shm_region))) {
			LOG_ERROR(ERROR_FP,
					"%s: FAIL attach to wmo file table shmem at region %d\n",
					LOG_PREFIX, ServOpt.shm_region);
			sleep(LONG_RETRY_SLEEP);
		}

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
	}

	name_buf[0] = '\0';
	subdir_buf[0] = '\0';

	sprintf(wmo_buf, "%s %s %s %s %s",
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb, p_prod->wmo_nnnxxx);
	prod_type = PROD_TYPE_NWSTG;
	sbn_type = SBN_TYP_NMC;

	WFinfo.basedir = ServOpt.outdir;
	WFinfo.link_id = WorkerIndex;
	WFinfo.group_id = 0;
	WFinfo.host_id = 0;
	WFinfo.p_prod_type = &prod_type;
	WFinfo.p_sbn_type = &sbn_type;
	WFinfo.wmo_string = wmo_buf;
	WFinfo.file_path = p_prod->filename;
	WFinfo.link_path = link_buf;
	WFinfo.file_subdir = subdir_buf;
	WFinfo.file_name = name_buf;
	WFinfo.sbnhdr_flag = 0;
	WFinfo.match_entry_id = -1;
	WFinfo.p_debug_buff_flag = &debug_buff_flag;
	WFinfo.p_debug_prod_flag = &debug_prod_flag;

	if (acq_wmo_set_filename(p_WFtab, &WFinfo) < 0) {
		LOG_ERROR(ERROR_FP,
				"%s: FAIL set filename for prod %d wmo [%s]\n",
				LOG_PREFIX, p_prod->seqno, wmo_buf);
		return -1;
	}

	if (ServOpt.verbosity > 1) {
		LOG_DEBUG(DEBUG_FP, "%s: set filename to %s for wmo [%s]\n", LOG_PREFIX,
				name_buf, wmo_buf);
	}

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	int finish_recv(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Product receipt handling function.  Log product.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			O	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level

RETURNS
	  0	Normal return (ACK_OK)
	 >0	will result in a ACK_RETRY
	 <0	will result in a ACK_FAIL
*******************************************************************************/
int finish_recv(prod_info_t *p_prod)
{
	struct tm *p_tm;
	char timebuf[DATESTR_MAX_LEN];
	char delaybuf[DATESTR_MAX_LEN];
	time_t now;
	int flag;

	if (ServOpt.verbosity > 2) {
		LOG_DEBUG(DEBUG_FP, "%s: received %s, %d bytes\n", LOG_PREFIX,
				p_prod->filename, p_prod->size);
	}

	flag = CLIENT_OPT_ADD_WMO_FILE_TBL_DIR_LINKS;

	if (acq_wmo_create_symlinks (p_WFtab, &WFinfo, flag) < 0) {
		LOG_ERROR (ERROR_FP, "%s: FAIL symlink prod %d WMO[%s]\n",
			LOG_PREFIX, p_prod->seqno, WFinfo.wmo_string);
		return -1; 
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	if (now > p_prod->queue_time) {
		sprintf(delaybuf, " +%lds", now - p_prod->queue_time);
	} else {
		delaybuf[0] = '\0';
	}

	LOG_PRODUCT(PRODUCT_FP,
		"END %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d) f(%s) %s\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		p_prod->filename,
		delaybuf);

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	int abort_recv(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Product abort handling function.  Remove product file and log the abort.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			O	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
int abort_recv(prod_info_t *p_prod)
{
	struct tm *p_tm;
	char timebuf[DATESTR_MAX_LEN];
	time_t now;

	if (ServOpt.verbosity > 2) {
		LOG_DEBUG(DEBUG_FP, "%s: aborting %s, %d bytes\n", LOG_PREFIX,
				p_prod->filename, p_prod->size);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	LOG_PRODUCT(PRODUCT_FP,
		"ABORT %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d) /P%d\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		p_prod->priority);

	if (unlink(p_prod->filename) < 0) {
		LOG_ERROR(ERROR_FP, "%s: ERROR fail unlink %s, %s\n",
				LOG_PREFIX, p_prod->filename, strerror(errno));
		return -1;
	}

	return 0;
}
