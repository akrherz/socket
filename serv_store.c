/*******************************************************************************
FILE NAME
	serv_store.c

FILE DESCRIPTION
	Product storage and handling routines

FUNCTIONS
	get_out_path	- get output path for product
	finish_recv		- product post-processing
	abort_recv		- product abort processing

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%
	04/13/06                 - seqno%10000 changed to seqno%1000000
NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_serv_store_c[]= "@(#)serv_store.c 0.4 09/29/2003 14:49:27";

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>

#ifdef INCLUDE_WMO_FILE_TBL
#	include <co/include/acq_wmo_file_tbl.h>
#	include <co/include/cp_product.h>
#	include <co/include/sbnstruct.h>

#	define MAX_WMO_LEN	64

	WMO_FILE_TABLE *p_WFtab;
	WMO_FILE_INFO WFinfo;
#endif

#include "share.h"
#include "server.h"

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

#ifdef INCLUDE_WMO_FILE_TBL

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
			CS_LOG_ERR(ERROR_FP,
					"%s: FAIL attach to wmo file table shmem at region %d\n",
					LOG_PREFIX, ServOpt.shm_region);
			sleep(LONG_RETRY_SLEEP);
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
	WFinfo.link_id = ConnInfo.link_id;
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
		CS_LOG_ERR(ERROR_FP,
				"%s: FAIL set filename for prod %d wmo [%s]\n",
				LOG_PREFIX, p_prod->seqno, wmo_buf);
		return -1;
	}

#else

	sprintf (p_prod->filename, "%s/%.5d-%.6d",
						ServOpt.outdir, getpid(), p_prod->seqno%1000000); 
	/* CS_LOG_DBUG(DEBUG_FP,
			"%s: set filename to %s\n", LOG_PREFIX, p_prod->filename); */
#endif

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP,
				"%s: set filename to %s for wmo [%s %s %s %s %s]\n",
				LOG_PREFIX, p_prod->filename, 
				p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
				p_prod->wmo_bbb, p_prod->wmo_nnnxxx);
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
	int i;
	char *p_basename;
	char *p_subdir;
	char log_path[FILENAME_LEN];
	static unsigned long total_count;

#ifdef INCLUDE_WMO_FILE_TBL
	int flag;
#endif

	if (ServOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: received %s, %d bytes\n", LOG_PREFIX,
				p_prod->filename, p_prod->size);
	}
	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

#ifdef INCLUDE_WMO_FILE_TBL

	flag = CLIENT_OPT_ADD_WMO_FILE_TBL_DIR_LINKS;
	if (acq_wmo_create_symlinks (p_WFtab, &WFinfo, flag) < 0) {
		CS_LOG_ERR (ERROR_FP, "%s: FAIL symlink prod %d WMO[%s]\n",
			LOG_PREFIX, p_prod->seqno, WFinfo.wmo_string);
		return -1; 
	}

#endif

	strcpy(log_path, p_prod->filename+strlen(ServOpt.outdir)+1);

#ifdef INCLUDE_WMO_FILE_TBL
	/* log all the symlinks */
	for (i = 0, strcat(log_path,",(");
			i < p_WFtab->max_link_dir_per_wmo_entry
					&& p_WFtab->wmo_file_table_entry
						[WFinfo.match_entry_id].link_dir_base_offset[i];
				i++) {
		sprintf(log_path+strlen(log_path), "%s%s/%s",
				(i > 0) ? "," : "", 
				(char*)p_WFtab + p_WFtab->base_name_list_offset
						+ p_WFtab->wmo_file_table_entry
							[WFinfo.match_entry_id].link_dir_base_offset[i],
				WFinfo.file_name);
	}
	strcat(log_path,")");
#endif

	if (now > p_prod->queue_time) {
		sprintf(delaybuf, " +%lds", now - p_prod->queue_time);
	} else {
		delaybuf[0] = '\0';
	}

	/* log status every 100 entries */
	if (!(++total_count % 100)) {
		char hostbuf[HOSTNAME_MAX_LEN];
		gethostname(hostbuf, sizeof(hostbuf));
		CS_LOG_PROD(PRODUCT_FP,
			"STATUS [%s] pid(%d) host:%s %s-l%d from=%s tot(%d) dir(%s)\n",
			Program, getpid(), hostbuf,
			ConnInfo.source?ConnInfo.source:"unknown", ConnInfo.link_id,
			ConnInfo.remotehost?ConnInfo.remotehost:"unknown", total_count,
			ServOpt.outdir);
	}

	CS_LOG_PROD(PRODUCT_FP,
		"END %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d) f(%s)%s\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		log_path,
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
	char delaybuf[DATESTR_MAX_LEN];

	if (ServOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: aborting #%d %s, %d bytes\n", LOG_PREFIX,
				p_prod->seqno, p_prod->filename, p_prod->size);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);
	
	if (now > p_prod->queue_time) {
		sprintf(delaybuf, " +%lds", now - p_prod->queue_time);
	} else {
		delaybuf[0] = '\0';
	}

	CS_LOG_PROD(PRODUCT_FP,
		"ABORT %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d) f(%s)%s\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		p_prod->filename,
		delaybuf);

	if (unlink(p_prod->filename) < 0) {
		if (errno != ENOENT) {
			CS_LOG_ERR(ERROR_FP, "%s: ERROR fail unlink %s, %s\n",
					LOG_PREFIX, p_prod->filename, strerror(errno));
			return -1;
		}
	}

	return 0;
}
