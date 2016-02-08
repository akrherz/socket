/*******************************************************************************
FILE NAME
	share.h

FILE DESCRIPTION
	Header file for shared client/server utilities

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/

#ifndef SHARE_H
#define SHARE_H

static char Sccsid_share_h[]= "@(#)share.h 0.9 06/20/2005 14:15:21";

#include <time.h>
#include <stdio.h>
#include <stdarg.h>

#ifndef MIN
#define MIN(a,b)			(((b)<(a))?(b):(a))
#endif

#ifndef MAX
#define MAX(a,b)			(((b)>(a))?(b):(a))
#endif

#define ACK_MSG_LEN			6

#define MSG_HDR_LEN			10
#define PROD_HDR_LEN		22

/* default log directory path */
#define LOG_DIR_PATH    "/tmp/logs"

/* default values for client/server */
#define DFLT_LISTEN_PORT	53000
#define DFLT_BUFSIZE		(32*1024)

/* field lengths */
#define FILENAME_LEN		256
#define WMO_TTAAII_LEN		6
#define WMO_CCCC_LEN		4
#define WMO_DDHHMM_LEN		6
#define WMO_DDHH_LEN		4
#define WMO_BBB_LEN			3
#define WMO_NNNXXX_LEN		6
#define WMO_NNNXXX_MIN_LEN	4
#define DATESTR_MAX_LEN		32
#define SOURCE_MAX_LEN		32
#define HOSTNAME_MAX_LEN	64

/* Buffer must be big enough to hold a header or and ack */
#define MIN_BUFSIZE			MAX(MSG_HDR_LEN+PROD_HDR_LEN+1, ACK_MSG_LEN+1)
/* MAX_BUFSIZE is arbitrary */
#define MAX_BUFSIZE			(1024*1024)

/* must change size field in header to send bigger files than this */
#define MAX_PROD_SIZE		(99999999-PROD_HDR_LEN)

/* must change seqno field in header to use higher seqno than this */
#define MAX_PROD_SEQNO		99999

#define CONN_MSG_START	"CONNECTION MESSAGE"
#define REMOTE_ID		"REMOTE"
#define SOURCE_ID		"SOURCE"
#define LINK_ID			"LINK"

/* CCB definitions */
#define CCB_FLAG_BYTE		0
#define CCB_LENGTH_BYTE		1
#define CCB_FLAG_VAL		0x40
#define CCB_LENGTH_MASK		0x3f
#define CCB_MIN_HDR_LEN		24
#define CCB_MAX_HDR_LEN		1024		/* guess */

/* product info structure */
typedef struct prod_info_struct {
	int		seqno;
	char	filename[FILENAME_LEN];
	char	wmo_ttaaii[WMO_TTAAII_LEN+1];
	char	wmo_cccc[WMO_CCCC_LEN+1];
	char	wmo_ddhhmm[WMO_DDHHMM_LEN+1];
	char	wmo_bbb[WMO_BBB_LEN+1];
	char	wmo_nnnxxx[WMO_NNNXXX_LEN+1];
	size_t	size;
	size_t	ccb_len;
	char	state;
	int  	send_count;
	time_t	queue_time;
	time_t	send_time;
	int		priority;
	struct prod_info_struct	*p_next;
} prod_info_t;

/* values for state field of prod_info_t structure */
#define STATE_FREE		' '
#define STATE_QUEUED	'Q'
#define STATE_SENT		'S'
#define STATE_ACKED		'A'
#define STATE_NACKED	'N'
#define STATE_RETRY		'R'
#define STATE_FAILED	'F'
#define STATE_DEAD		'X'

/* values for code field of ack message */
#define ACK_OK			'K'
#define ACK_FAIL		'F'
#define ACK_RETRY		'R'

/* Bits for global Flags variable */
#define SHUTDOWN_FLAG	1
#define DISCONNECT_FLAG	2
#define NOPEER_FLAG		4

/* log info structure */
typedef struct {
	char		name[FILENAME_LEN];
	char		dir[FILENAME_LEN];
	char		path[FILENAME_LEN];
	size_t		maxsize;
	int			flags;
	int			writes_per_check;
	time_t		check_time_interval;
	int			writes_per_flush;
	time_t		flush_time_interval;
	FILE *		p_stream;
	time_t		last_check_time;
	int			writes_since_last_check;
	time_t		last_flush_time;
	int			writes_since_last_flush;
	time_t		last_write_time;
} logfile_t;

/* Bits for flags field of loginfo_t */
#define LOG_ROTATE_FLAG		1
#define LOG_ARCHIVE_FLAG	2
#define LOG_STDOUT_FLAG		4
#define LOG_STDERR_FLAG		8

/* Logging macros...
	LOG_XXXX define the functions
	XXXX_FP defines the first arg

	The following would cause all errors to go to stderr
		#define LOG_ERROR fprintf
		#define ERROR_FP stderr
*/

#ifdef INCLUDE_CP_LOG_LIB
#	include	<co/include/cp_log.h>
#	define CS_LOG_ERR	com_vlog
#	define ERROR_FP	(L_ERROR|L_LVL0)
#	define CS_LOG_DBUG	com_vlog
#	define DEBUG_FP	(L_DEBUG|L_LVL1)
#else
#	define CS_LOG_ERR	write_log
#	define ERROR_FP	(&LogFile)
#	define CS_LOG_DBUG	write_log
#	define DEBUG_FP	(&LogFile)
#endif

#define CS_LOG_PROD	write_log
#define PRODUCT_FP	(&LogFile)

/* heading of each error and debug log entry */
#define LOG_PREFIX	log_prefix(Program, __FILE__, __LINE__)

/* global variables */
char Program[32];		/* program name */
long Flags;				/* flags, set by signal handlers and other routines */
logfile_t LogFile;		/* log file info structure */

/* prototypes */
int format_msghdr(char *buf, prod_info_t *p_prod);
int parse_msghdr(char *buf, size_t buflen, prod_info_t *p_prod);

int format_ack(char *buf, int seqno, char code);
int parse_ack(char *buf, size_t buflen, int *p_seqno, char *p_code);

void daemonize(void);

int my_mkdir(const char *path);
int my_rename(const char *source, const char *target);
int my_copy(const char *source, const char *target);

int parse_wmo(char *buf, size_t buflen, prod_info_t *p_prod);
char *debug_buf(char *buf, size_t buflen);

int write_log(logfile_t *p_logfile, const char *format, ...);
int flush_log(logfile_t *p_logfile);
int rename_log(logfile_t *p_logfile, char *newname);
char *log_prefix(char *program, char *file, int line);

int get_ccb_len(char *buf, size_t buflen);

void write_pidfile(char *path);

#endif
