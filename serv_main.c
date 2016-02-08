/*******************************************************************************
NAME
	serv_main.c

DESCRIPTION
	Main, command line option processing, and signal handling routines
	for server applications.

FUNCTIONS
	main				- program entry
	process_args		- command line argument processing
	usage				- print usage message
	setup_sig_handler	- register signal handlers
	stop_sighandler		- handles shutdown signals SIGTERM, SIGINT, etc.
	pipe_sighandler		- handles SIGPIPE (remote socket close)
	alarm_sighandler	- handle SIGALRM (timeouts)

HISTORY
	Last delta date:
    02/03/07  s command line option added for log identification
    03/28/97  SCCS identifier:  7.1

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_serv_main_c[]= "@(#)serv_main.c 0.5 04/12/2005 11:13:51";

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "share.h"
#include "server.h"

static void process_args(int argc, char *argv[]);
static void usage(void);
static void setup_sig_handler(void);
void stop_sighandler(int signum);
void pipe_sighandler(int signum);
void child_sighandler(int signum);
void alarm_sighandler(int signum);

/*******************************************************************************
FUNCTION NAME
	int main(int argc, char *argv[]) 

FUNCTION DESCRIPTION
	Process command line options, set-up signal handler, and turn into
	a daemon.  Then call dispatcher() to do real work.

PARAMETERS
	Type			Name			I/O	Description
	int				argc			I	arg count
	char **			argv			I	arg vector

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			debug;			I	debug flag, do not daemonize
	char *			Program			I/O	Program name
	unsigned int	listen_port		O	port number for listen/connect

RETURNS
	0	normal exit
	1	command line arg error
	2	server initialization error
	3	dispatcher failure
	4	server shutdown error
	7	both (3) and (4)
*******************************************************************************/
int main (int argc, char *argv[])
{
	char *p;
	int status;
	char pidfile[256];

	if ((p = strrchr(argv[0], '/')) != NULL) {
		strcpy(Program, ++p);
	} else {
		strcpy(Program, argv[0]);
	}

	process_args(argc, argv);	/* exit with status 1 on error */

	setup_sig_handler();

	if (!ServOpt.debug) {
		daemonize();
	}

	sprintf(pidfile, "/var/run/%s-%d", Program, ServOpt.listen_port);
	write_pidfile(pidfile);

	if (serv_init() < 0) {
		return 2;
	}

	CS_LOG_DBUG(DEBUG_FP, "%s: starting dispatcher pid=%d\n",
					LOG_PREFIX, getpid());

	if (dispatcher() < 0) {
		status = 3;
	} else {
		status = 0;
	}

	CS_LOG_DBUG(DEBUG_FP, "%s: dispatcher %d exiting with status %d\n",
					LOG_PREFIX, getpid(), status);

	if (serv_close() < 0) {
		status += 4;
	}
	
	return status;
} /* end main */

/*******************************************************************************
FUNCTION NAME
	static void process_args(int argc, char *argv[]) 

FUNCTION DESCRIPTION
	Process command line options.  Any error results in an exit.  Error
	messages are printed to standard error.

PARAMETERS
	Type			Name			I/O	Description
	int				argc			I	arg count
	char **			argv			I	arg vector

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	unsigned int	listen_port		O	port number for listen/connect
	char			debug			O	debug flag, do not daemonize
	char			verbosity		O	debugging verbosity level
	int				max_worker		O	maximum concurrent workers
	time_t			timeout			O	timeout interval (on socket)
	size_t			bufsize			O	max size to read from socket
	char *			out_dir			O	storage directory for received files
	int				outfile_flags	O	outfile open flags (overwrite)
	int				LogFile.flags	O	logging options flags

RETURNS
	void - error results in an exit(1)
*******************************************************************************/
static void process_args(int argc, char *argv[])
{
	int		c;
	size_t	dirlen;

	/* default options */
	ServOpt.listen_port = DFLT_LISTEN_PORT;
	ServOpt.debug = 0;
	ServOpt.verbosity = 0;
	ServOpt.max_worker = DFLT_MAX_WORKER;
	ServOpt.timeout = DFLT_TIMEOUT;
	ServOpt.bufsize = DFLT_BUFSIZE;
	if (!getcwd(ServOpt.outdir, FILENAME_LEN)) {
		fprintf(stderr, "%s: FAIL getcwd, %s\n", LOG_PREFIX, strerror(errno));
		exit(1);
	}
	if (strlen(ServOpt.outdir) + strlen(OUTPUT_SUBDIR_NAME) +2 > FILENAME_LEN) {
		fprintf(stderr, "%s: output pathlen overflow, max %d bytes\n",
				LOG_PREFIX, FILENAME_LEN);
		exit(1);
	}
	sprintf(ServOpt.outdir+strlen(ServOpt.outdir), "/%s", OUTPUT_SUBDIR_NAME);
	
	ServOpt.outfile_flags = O_WRONLY|O_CREAT|O_EXCL;

	while ((c = getopt(argc, argv, "dv:ap:w:t:b:c:l:D:OPm:s:")) != -1) {
		switch (c) {
			case 'd':
				fprintf(stdout, "%s: Setting debug option\n", Program);
				ServOpt.debug = 1;
				break;
			case 'l':
				fprintf(stdout, "%s: Setting log path to %s\n",
						Program, optarg);
				if (strlen(optarg) > FILENAME_LEN) {
					fprintf(stderr,
						"%s: ERROR log path %s is too long (%d bytes max)\n",
						Program, optarg, FILENAME_LEN);
					exit(1);
				}
				strcpy(LogFile.dir, optarg);
				break;
			case 'v':
				fprintf(stdout, "%s: Setting verbosity level to %s\n",
						Program, optarg);
				ServOpt.verbosity = (char) atoi(optarg);
				LogFile.flags |= LOG_STDOUT_FLAG;
				fprintf(stdout, "%s: Error messages will be sent to stdout\n",
						Program);
				break;
			case 'a':
				LogFile.flags |= LOG_ARCHIVE_FLAG;
				fprintf(stdout, "%s: Log files will be archived\n",
						Program);
				break;
			case 'w':
				fprintf(stdout, "%s: Setting max worker count to %s\n",
						Program, optarg);
				ServOpt.max_worker = atoi(optarg);
				if (ServOpt.max_worker < 0 || ServOpt.max_worker > 100000) {
					fprintf(stderr,
						"%s: Invalid max_worker %d, (min=0, max=100000)\n",
						Program, ServOpt.max_worker);
					exit(1);
				}
				break;
			case 'p':
				fprintf(stdout, "%s: Setting port number to %s\n",
						Program, optarg);
				ServOpt.listen_port = atoi(optarg);
				break;
			case 't':
				fprintf(stdout, "%s: Setting timeout interval to %s\n",
						Program, optarg);
				ServOpt.timeout = atoi(optarg);
				break;
			case 'b':
				ServOpt.bufsize = atoi(optarg);
				if (ServOpt.bufsize < MSG_HDR_LEN+PROD_HDR_LEN
						|| ServOpt.bufsize > 1024*1024) {
					fprintf(stderr,
						"%s: Invalid buffer size %d! (must be [%d-%d])\n",
						Program, ServOpt.bufsize, MSG_HDR_LEN+PROD_HDR_LEN,
						MAX_BUFSIZE);
					exit(1);
				}
				break;
			case 'c':
				if (!(ServOpt.connect_wmo = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				break;
			case 'D':
				fprintf(stdout, "%s: Setting output directory to %s\n",
						Program, optarg);
				dirlen = strlen(optarg);
				/* check for trailing '/' for clip */
				if (dirlen > 1 && optarg[dirlen-1] == '/') {
					dirlen--;
				}
				/* filename format is <outdir>/<pid>-<seqno>%10000 */
				if (dirlen + 12 > FILENAME_LEN) {
					fprintf(stderr,
						"%s: ERROR outdir path %s is too long (%d bytes max)\n",
						Program, optarg, FILENAME_LEN - 12);
					exit(1);
				}
				strncpy(ServOpt.outdir, optarg, dirlen);
				ServOpt.outdir[dirlen] = '\0';
				break;
			case 'O':
				ServOpt.outfile_flags |= OVER_WRITE_FLAG;
				fprintf(stdout, "%s: Set out file flags to overwrite (%o)\n",
						Program, ServOpt.outfile_flags);
				break;
			case 'P':
				ServOpt.outfile_flags |= TOGGLE_PERMS_FLAG;
				fprintf(stdout, "%s: Set out file flags to toggle perms (%o)\n",
						Program, ServOpt.outfile_flags);
				break;
			case 'm':
				ServOpt.shm_region = atoi(optarg);
				fprintf(stdout, "%s: Set shared memory region to (%d)\n",
						Program, ServOpt.shm_region);
				break;
			case 's':
				sprintf(Program+strlen(Program), "-%s", optarg);
				break;
			case '?':
				usage();
				exit(0);
			default:
				fprintf(stderr, "%s: Unrecognized option -%c\n",
									Program, optopt);
				usage();
				exit(1);
		} /* end switch */
	} /* end while */

	return;

} /* end process_args */

/*******************************************************************************
FUNCTION NAME
	static void usage(void)

FUNCTION DESCRIPTION
	Print usage message.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char *			Program;		I	program name

RETURNS
	void
*******************************************************************************/
static void usage(void)
{
	fprintf(stderr, "usage: %s\n", Program);
	fprintf(stderr,
		"         [-p port]        (listen port, default=%d)\n",
		DFLT_LISTEN_PORT);
	fprintf(stderr,
		"         [-w max_worker]  (maximum concurrent workers, default=%d)\n",
		DFLT_MAX_WORKER);
	fprintf(stderr,
		"         [-t timeout]     (socket timeout, default=%d secs)\n",
		DFLT_TIMEOUT);
	fprintf(stderr,
		"         [-b bufsiz]      (send/recv buffer size, default=%d bytes)\n",
		DFLT_BUFSIZE);
	fprintf(stderr,
		"         [-s source]      (set source id string to <source>)\n");
	fprintf(stderr,
		"         [-d]             (debug mode, default NO)\n");
	fprintf(stderr,
		"         [-l log_dir]     (path for log files, default=%s)\n", LOG_DIR_PATH);
	fprintf(stderr,
		"         [-v lvl]         (verbosity level, default=0)\n");
	fprintf(stderr,
		"         [-a]             (archive log files, default NO)\n");
	fprintf(stderr,
		"         [-c ttaaii]      (expect connect msg with wmo heading ttaaii)\n");
	fprintf(stderr,
		"         [-D outdir]      (output dir, default=<working dir>/output)\n");
	fprintf(stderr,
		"         [-O]             (Overwrite output files, default NO)\n");
	fprintf(stderr,
		"         [-P]             (Toggle read perms on output files, default NO)\n");

#ifdef INCLUDE_WMO_FILE_TBL
	fprintf(stderr,
		"         [-m <region>]    (Use alt shared memory region for WMO file table)\n");
#endif

} /* end usage */

/*******************************************************************************
FUNCTION NAME
	static void setup_sig_handler(void)

FUNCTION DESCRIPTION
	Set-up handler functions for signals
		SIGINT, SIGTERM	-	shutdown signal handler
		SIGPIPE			-	socket disconnection handler
		SIGCHLD			-	death of child (worker exit) handler
		SIGALRM			-	timeout handler

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES
	Type			Name			I/O	Description
	None

RETURNS
	void 
*******************************************************************************/
static void setup_sig_handler(void)
{
	struct sigaction act;

	sigemptyset (&act.sa_mask);
	act.sa_handler = stop_sighandler;
	act.sa_flags=0;
	if (sigaction (SIGTERM, &act, 0) == -1) {
		CS_LOG_ERR(ERROR_FP, 
				"%s: Sigaction failed for sig=%d, act=stop_sighandler, %s\n",
				LOG_PREFIX, SIGTERM, strerror(errno));
	}
	if (sigaction (SIGINT, &act, 0) == -1) {
		CS_LOG_ERR(ERROR_FP, 
				"%s: Sigaction failed for sig=%d, act=stop_sighandler, %s\n",
				LOG_PREFIX, SIGINT, strerror(errno));
	}

	sigemptyset (&act.sa_mask);
	act.sa_handler = pipe_sighandler;
	act.sa_flags=0;
	if (sigaction (SIGPIPE, &act, 0) == -1) {
		CS_LOG_ERR(ERROR_FP, 
				"%s: Sigaction failed for sig=%d, act=pipe_sighandler, %s\n",
				LOG_PREFIX, SIGPIPE, strerror(errno));
	}

	sigemptyset (&act.sa_mask);
	act.sa_handler = child_sighandler;
	act.sa_flags=0;
	if (sigaction (SIGCHLD, &act, 0) == -1) {
		CS_LOG_ERR(ERROR_FP, 
				"%s: Sigaction failed for sig=%d, act=child_sighandler, %s\n",
				LOG_PREFIX, SIGCHLD, strerror(errno));
	}

	sigemptyset (&act.sa_mask);
	act.sa_handler = alarm_sighandler;
	act.sa_flags=0;
	if (sigaction (SIGALRM, &act, 0) == -1) {
		CS_LOG_ERR(ERROR_FP, 
				"%s: Sigaction failed for sig=%d, act=alarm_sighandler, %s\n",
				LOG_PREFIX, SIGALRM, strerror(errno));
	}

	return;
} /* end setup_sig_handler */

/*******************************************************************************
FUNCTION NAME
	void stop_sighandler(int signum)

FUNCTION DESCRIPTION
	If shutdown flag is not set, set shutdown flag and allow for graceful
	exit, otherwise kill workers and exit directly.

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES
	Type			Name			I/O	Description
	int				Flags			I+O	Control Flags

RETURNS
	void 
*******************************************************************************/
void stop_sighandler(int signum)
{
	if (!(Flags & SHUTDOWN_FLAG)) {
		CS_LOG_ERR(ERROR_FP, "%s: Setting shutdown flag on signal %d\n",
				LOG_PREFIX, signum);
		Flags |= SHUTDOWN_FLAG;
	} else {
		/* exit directly in case we are hanging somewhere */
		CS_LOG_ERR(ERROR_FP, "%s: Exiting on signal %d\n",
				LOG_PREFIX, signum);
		kill_workers();
		exit(0);
	}
} /* end stop_sighandler */

/*******************************************************************************
FUNCTION NAME
	void pipe_sighandler(int signum)

FUNCTION DESCRIPTION
	Set flag to indicate that peer has disconnected.

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES
	Type			Name			I/O	Description
	int				Flags			O	Control Flags

RETURNS
	void 
*******************************************************************************/
void pipe_sighandler(int signum)
{
	if (ServOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Set disconnect flag on signal %d\n",
						LOG_PREFIX, signum);
	}
	Flags |= DISCONNECT_FLAG;

	return;
} /* end pipe_sighandler */

/*******************************************************************************
FUNCTION NAME
	void child_sighandler(int signum)

FUNCTION DESCRIPTION
	Get status of exiting worker

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES
	Type			Name			I/O	Description
	none

RETURNS
	void 
*******************************************************************************/
void child_sighandler(int signum)
{
	if (ServOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Received signal %d, (death-of-child)\n",
						LOG_PREFIX, signum);
	}

	wait_for_worker();

	return;
} /* end child_sighandler */

/*******************************************************************************
FUNCTION NAME
	void alarm_sighandler(int signum)

FUNCTION DESCRIPTION
	Set disconnect flag.

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES
	Type			Name			I/O	Description
	int				Flags			O	Control Flags

RETURNS
	void 
*******************************************************************************/
void alarm_sighandler(int signum)
{
	CS_LOG_ERR(ERROR_FP,
			"%s: Received alarm signal %d, set disconnect flag\n",
			LOG_PREFIX, signum);

	Flags |= DISCONNECT_FLAG;

	return;
} /* end alarm_sighandler */
