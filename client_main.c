/*******************************************************************************
FILE NAME
	client_main.c

FILE DESCRIPTION
	Main, command line option processing, and signal handling routines
	for client applications.

FUNCTIONS
	main				- program entry
	process_args		- command line argument processing
	usage				- print usage message
	setup_sig_handler	- register signal handlers
	stop_sighandler		- handles shutdown signals SIGTERM, SIGINT, etc.
	pipe_sighandler		- handles SIGPIPE (remote socket close)
	alarm_sighandler	- handle SIGALRM (timeouts)

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

	Mon Jun 20 14:01:43 EDT 2005 - Added TTL option and ignore .dot files

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_client_main_c[]= "@(#)client_main.c 0.10 06/20/2005 14:20:50";

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "client.h"

#define CLIP_TRAILING_SLASH(s) if (s[strlen(s)-1]=='/') s[strlen(s)-1]='\0'

static void process_args(int argc, char *argv[]);
static void usage(void);
static void setup_sig_handler(void);
void stop_sighandler(int signum);
void pipe_sighandler(int signum);
void alarm_sighandler(int signum);

/*******************************************************************************
FUNCTION NAME
	int main(int argc, char *argv[]) 

FUNCTION DESCRIPTION
	Process command line options, set-up signal handler, and turn into
	a daemon.  Then call poll_and_send() to do real work.

PARAMETERS
	Type			Name			I/O	Description
	int				argc			I	arg count
	char **			argv			I	arg vector

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char			debug;			I	debug flag, do not daemonize
	char *			Program			I/O	Program name

RETURNS
	0	normal exit
	1	command line arg error
	2	client initialization error
	3	poll_and_send failure
	4	client shutdown error
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

	process_args(argc, argv);
	
	if (ClientOpt.source) {
		sprintf(Program+strlen(Program), "-%s",
				ClientOpt.source);
	}

	setup_sig_handler();

	if (!ClientOpt.debug) {
		daemonize();
	}

	sprintf(pidfile, "/var/run/%s-%s-%d",
			Program, ClientOpt.host, ClientOpt.port);
	write_pidfile(pidfile);

	if (client_init() < 0) {
		return 2;
	}

	CS_LOG_PROD(PRODUCT_FP,
		"STATUS START [%s] pid(%d) %s to=%s/%d dir(%s%s)\n",
			Program, getpid(),
			ClientOpt.source ? ClientOpt.source : "unknown",
			ClientOpt.host, ClientOpt.port, ClientOpt.indir_list[0],
			ClientOpt.indir_list[1]?",...":"");

	if (poll_and_send() < 0) {
		status = 3;
	} else {
		status = 0;
	}

	CS_LOG_PROD(PRODUCT_FP,
		"STATUS EXIT %d [%s] pid(%d) %s to=%s/%d dir(%s%s)\n",
			status, Program, getpid(),
			ClientOpt.source ? ClientOpt.source : "unknown",
			ClientOpt.host, ClientOpt.port, ClientOpt.indir_list[0],
			ClientOpt.indir_list[1]?",...":"");


	if (client_close() < 0) {
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

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	unsigned int	port			O	port number for listen/connect
	char *			host			O	remote host of server
	char			debug			O	debug flag, do not daemonize
	char			verbosity		O	debugging verbosity level
	time_t			timeout			O	timeout interval (on socket)
	time_t			poll_interval	O	input polling interval (when idle)
	int				window_size		O	maximum outstanding acks
	int				max_retry		O	max number of send retries per prod
	size_t			bufsize			O	max size to write to socket
	char **			indir_list		O	null-terminated list of input dirs
	char *			sent_dir		O	holding directory for queued files
	char *			fail_dir		O	holding directory for failed files
	char			wait_last_file	O	don't send the last file
	time_t			refresh_interval O	queue refresh/resort interval
	int 			max_queue_len	O	max number of items to poll and sort
	int				LogFile.flags	O	logging options flags

RETURNS
	void - error results in an exit(1)
*******************************************************************************/
static void process_args(int argc, char *argv[])
{
	int		c;
	static char hostbuf[128];
	int		indir_count = 0;
	int		host_count = 0;
	char *	p_subdir;
	size_t	pathlen;
	char	currdir[FILENAME_LEN];
	char *	p_units;

	/* default options */
	ClientOpt.port = DFLT_LISTEN_PORT;
	ClientOpt.debug = 0;
	ClientOpt.verbosity = 0;
	ClientOpt.host_list = NULL;
	ClientOpt.timeout = DFLT_TIMEOUT;
	ClientOpt.poll_interval = DFLT_INTERVAL;
	ClientOpt.window_size = DFLT_WINSIZE;
	ClientOpt.max_retry = DFLT_RETRY;
	ClientOpt.bufsize = DFLT_BUFSIZE;
	ClientOpt.wait_last_file = 0;
	ClientOpt.refresh_interval = DFLT_REFRESH;
	ClientOpt.indir_list = NULL;
	ClientOpt.sent_dir = NULL;
	ClientOpt.fail_dir = NULL;
	ClientOpt.max_queue_len = DFLT_MAX_QUEUE;
	ClientOpt.sent_count = DFLT_SENT_COUNT;

	while ((c = getopt(argc, argv, "dv:ap:n:t:i:l:w:r:b:c:s:m:h:k:xD:P:S:F:LI:Q:N:")) != -1) {
		switch (c) {
			case 'd':
				fprintf(stdout, "%s: Setting debug option\n", Program);
				ClientOpt.debug = 1;
				break;
			case 'v':
				ClientOpt.verbosity = atoi(optarg);
				fprintf(stdout, "%s: Setting verbosity level to %d\n",
						Program, ClientOpt.verbosity);
				/* LogFile.flags |= LOG_STDOUT_FLAG; */
				/* fprintf(stdout, "%s: Error messages will be sent to stdout\n",
						Program); */
				break;
			case 'a':
				LogFile.flags |= LOG_ARCHIVE_FLAG;
				fprintf(stdout, "%s: Log files will be archived\n",
						Program);
				break;
			case 'P':
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
			case 'n':
				host_count++;
				if (!(ClientOpt.host_list
						= realloc(ClientOpt.host_list,
									(host_count+1)*sizeof(char*)))) {
					fprintf(stderr,
						"%s: FAIL realloc %d host's, %s\n",
						Program, host_count, strerror(errno));
					exit(1);
				}
				if (!(ClientOpt.host_list[host_count-1] = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				if (!strcmp(ClientOpt.host_list[host_count-1], "null")
						|| !strcmp(ClientOpt.host_list[host_count-1], "NULL")) {
					gethostname(hostbuf, sizeof(hostbuf));
					ClientOpt.host = hostbuf;
					ClientOpt.port = DISCARD_PORT;
					fprintf(stdout, "%s: Setting port number to %d\n",
							Program, ClientOpt.port);
				}
				ClientOpt.host_list[host_count] = NULL;
				fprintf(stdout, "%s: Adding %s to remote host list\n",
						Program, ClientOpt.host_list[host_count-1]);

				break;
			case 'p':
				ClientOpt.port = atoi(optarg);
				if (ClientOpt.port < 1024 && ClientOpt.port != DISCARD_PORT) {
					fprintf(stderr,
						"%s: Invalid port number %d!  Use port above 1024\n",
						Program, ClientOpt.port);
					exit(1);
				}
				fprintf(stdout, "%s: Setting port number to %d\n",
						Program, ClientOpt.port);
				break;
			case 't':
				ClientOpt.timeout = atoi(optarg);
				if (ClientOpt.timeout < 1) {
					fprintf(stderr,
						"%s: Invalid timeout interval %u! (must be > 0)\n",
						Program, ClientOpt.timeout);
					exit(1);
				}
				fprintf(stdout, "%s: Setting timeout interval to %u\n",
						Program, ClientOpt.timeout);
				break;
			case 'i':
				ClientOpt.poll_interval = atoi(optarg);
				if (ClientOpt.poll_interval < 1) {
					fprintf(stderr,
						"%s: Invalid poll interval %ld! (must be > 0)\n",
						Program, ClientOpt.poll_interval);
					exit(1);
				}
				fprintf(stdout, "%s: Setting poll interval to %ld\n",
						Program, ClientOpt.poll_interval);
				break;
			case 'l':
				ClientOpt.queue_ttl = strtol(optarg, &p_units, 10);
				if (ClientOpt.queue_ttl < 1) {
					fprintf(stderr,
						"%s: Invalid queue ttl %ld! (must be >= 0)\n",
						Program, ClientOpt.queue_ttl);
					exit(1);
				}
				if (p_units) {
					switch (*p_units) {
						case 's':
							/* default */
							break;
						case 'm':
							/* minutes */
							ClientOpt.queue_ttl *= 60;
							break;
						case 'h':
							/* hours */
							ClientOpt.queue_ttl *= 60*60;
							break;
						case 'd':
							ClientOpt.queue_ttl *= 24*60*60;
							/* days */
							break;
						case ' ':
						case '\0':
						case '\t':
						case '\n':
							/* ignore */
							break;
					}
				}
				fprintf(stdout, "%s: Setting poll interval to %ld secs\n",
						Program, ClientOpt.queue_ttl);
				break;
			case 'w':
				ClientOpt.window_size = atoi(optarg);
				if (ClientOpt.window_size < 1) {
					fprintf(stderr,
						"%s: Invalid window size %d! (must be > 0)\n",
						Program, ClientOpt.window_size);
					exit(1);
				}
				fprintf(stdout, "%s: Setting ack window size to %d\n",
						Program, ClientOpt.window_size);
				break;
			case 'r':
				ClientOpt.max_retry = atoi(optarg);
				if (ClientOpt.max_retry < -1 || ClientOpt.max_retry > 99 || 
					(ClientOpt.max_retry == 0 && *optarg != '0')) {
					fprintf(stderr,
						"%s: Invalid max retry value %d! (must be [-1 - 99])\n",
						Program, ClientOpt.max_retry);
					exit(1);
				}
				fprintf(stdout, "%s: Setting max retry to %d\n",
						Program, ClientOpt.max_retry);
				break;
			case 'b':
				ClientOpt.bufsize = atol(optarg);
				if (ClientOpt.bufsize < MIN_BUFSIZE
						|| ClientOpt.bufsize > MAX_BUFSIZE) {
					fprintf(stderr,
						"%s: Invalid buffer size %d! (must be [%d-%d])\n",
						Program, ClientOpt.bufsize, MIN_BUFSIZE, MAX_BUFSIZE);
					exit(1);
				}
				break;
			case 'c':
				if (!(ClientOpt.connect_wmo = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				break;
			case 's':
				if (!(ClientOpt.source = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				break;
			case 'm':
				ClientOpt.shm_region = atoi(optarg);
				if (ClientOpt.shm_region < 0 || ClientOpt.shm_region > 99 ||
					(ClientOpt.shm_region == 0 && *optarg != '0')) {
					fprintf(stderr,
						"%s: Invalid region (%s), region must be [0-9]\n",
						Program, optarg);
					exit(1);
				}
				break;
			case 'h':
				ClientOpt.host_id = atoi(optarg);
				if (ClientOpt.host_id < 0 || ClientOpt.host_id > 99 ||
					(ClientOpt.host_id == 0 && *optarg != '0')) {
					fprintf(stderr,
						"%s: Invalid host_id (%s), host_id must be [0-99]\n",
						Program, optarg);
					exit(1);
				}
				break;
			case 'k':
				ClientOpt.link_id = atoi(optarg);
				if (ClientOpt.link_id < 0 || ClientOpt.link_id > 99 ||
					(ClientOpt.link_id == 0 && *optarg != '0')) {
					fprintf(stderr,
						"%s: Invalid link_id (%s), link_id must be [0-99]\n",
						Program, optarg);
					exit(1);
				}
				break;
			case 'x':
				ClientOpt.strip_ccb = 1;
				fprintf(stdout, "%s: Setting strip ccb header option ON\n",
						Program);
				break;
			case 'D':
				indir_count++;
				if (!(ClientOpt.indir_list
						= realloc(ClientOpt.indir_list,
									(indir_count+1)*sizeof(char*)))) {
					fprintf(stderr,
						"%s: FAIL realloc %d indir's, %s\n",
						Program, indir_count, strerror(errno));
					exit(1);
				}
				if (!(ClientOpt.indir_list[indir_count-1] = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				CLIP_TRAILING_SLASH(ClientOpt.indir_list[indir_count-1]);
				ClientOpt.indir_list[indir_count] = NULL;
				fprintf(stdout, "%s: Adding %s to queue directory list\n",
						Program, ClientOpt.indir_list[indir_count-1]);
				break;
			case 'S':
				if (!(ClientOpt.sent_dir = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				CLIP_TRAILING_SLASH(ClientOpt.sent_dir);
				fprintf(stdout, "%s: Setting sent directory to %s\n",
						Program, ClientOpt.sent_dir);
				break;
			case 'F':
				if (!(ClientOpt.fail_dir = strdup(optarg))) {
					fprintf(stderr,
						"%s: FAIL strdup(%s), %s\n",
						Program, optarg, strerror(errno));
					exit(1);
				}
				CLIP_TRAILING_SLASH(ClientOpt.fail_dir);
				fprintf(stdout, "%s: Setting failure directory to %s\n",
						Program, ClientOpt.fail_dir);
				break;
			case 'L':
				ClientOpt.wait_last_file = 1;
				fprintf(stdout, "%s: Setting last file wait option ON\n",
						Program);
				break;
			case 'I':
				ClientOpt.refresh_interval = atoi(optarg);
				if (ClientOpt.refresh_interval <= 0
						&& ClientOpt.refresh_interval != -1) {
					fprintf(stderr,
						"%s: Invalid refresh interval %ld! (must -1 or > 0)\n",
						Program, ClientOpt.refresh_interval);
					exit(1);
				}
				fprintf(stdout, "%s: Setting refresh interval to %ld\n",
						Program, ClientOpt.refresh_interval);
				break;
			case 'Q':
				ClientOpt.max_queue_len = atoi(optarg);
				if (ClientOpt.max_queue_len <= 0
						&& ClientOpt.max_queue_len != -1) {
					fprintf(stderr,
						"%s: Invalid max queue len %d! (must -1 or > 0)\n",
						Program, ClientOpt.max_queue_len);
					exit(1);
				}
				fprintf(stdout, "%s: Setting max queue len to %d\n",
						Program, ClientOpt.max_queue_len);
				break;
			case 'N':
				ClientOpt.sent_count = atoi(optarg);
				if (ClientOpt.sent_count <= 0
						&& ClientOpt.sent_count != -1) {
					fprintf(stderr,
						"%s: Invalid sent count %d! (must -1 or > 0)\n",
						Program, ClientOpt.sent_count);
					exit(1);
				}
				fprintf(stdout, "%s: Setting sent count to %d\n",
						Program, ClientOpt.sent_count);
				break;
			case '?':
				/* invalid option */
				usage();
				exit(1);
			case ':':
				/* missing parameter */
				usage();
				exit(1);
			default:
				fprintf(stderr, "%s: Option handler error for -%c\n",
									Program, (char)c);
				usage();
				exit(1);
		} /* end switch */
	} /* end while */

	if (ClientOpt.refresh_interval > 0
			&& ClientOpt.refresh_interval < ClientOpt.poll_interval) {
		fprintf(stderr,
			"%s: ERROR refresh interval %ld must be > poll interval %ld\n",
			LOG_PREFIX, ClientOpt.refresh_interval, ClientOpt.poll_interval);
		exit(1);
	}
	if (ClientOpt.host_list == NULL) {
		if (!(ClientOpt.host_list = malloc(2*sizeof(char *)))) {
			fprintf(stderr, "%s: FAIL malloc 2 host strings, %s\n",
						LOG_PREFIX, strerror(errno));
			exit(1);
		}
		gethostname(hostbuf, sizeof(hostbuf));
		ClientOpt.host_list[0] = hostbuf;
		ClientOpt.host_list[1] = NULL;
	}
	ClientOpt.host = ClientOpt.host_list[0];

	if (ClientOpt.indir_list == NULL) {
		if (!(ClientOpt.indir_list = malloc(2*sizeof(char *)))) {
			fprintf(stderr, "%s: FAIL malloc 2 indir strings, %s\n",
						LOG_PREFIX, strerror(errno));
			exit(1);
		}
		if (!(ClientOpt.indir_list[0] = malloc(FILENAME_LEN))) {
			fprintf(stderr, "%s: FAIL malloc %d bytes for indir, %s\n",
						LOG_PREFIX, FILENAME_LEN, strerror(errno));
			exit(1);
		}
		if (!getcwd(ClientOpt.indir_list[0], FILENAME_LEN)) {
			fprintf(stderr, "%s: FAIL getcwd, %s\n",
						LOG_PREFIX, strerror(errno));
			exit(1);
		}
		if (strlen(ClientOpt.indir_list[0]) + strlen(INPUT_SUBDIR_NAME) +2
				> FILENAME_LEN) {
			fprintf(stderr, "%s: input pathlen overflow, max %d bytes\n",
					LOG_PREFIX, FILENAME_LEN);
			exit(1);
		}
		sprintf(ClientOpt.indir_list[0]+strlen(ClientOpt.indir_list[0]),
				"/%s", INPUT_SUBDIR_NAME);

		ClientOpt.indir_list[1] = NULL;
	}

	if (ClientOpt.sent_dir == NULL) {
		if ((p_subdir = strrchr(ClientOpt.indir_list[0], '/'))) {
			pathlen = p_subdir-ClientOpt.indir_list[0] + 1
						+ strlen(SENT_SUBDIR_NAME) + 1;
			if (!(ClientOpt.sent_dir = malloc(pathlen))) {
				fprintf(stderr,
					"%s: FAIL malloc %d bytes for sent dir path, %s\n",
					LOG_PREFIX, pathlen, strerror(errno));
				exit(1);
			}
			sprintf(ClientOpt.sent_dir, "%.*s/%s",
						p_subdir - ClientOpt.indir_list[0],
						ClientOpt.indir_list[0], SENT_SUBDIR_NAME);
		} else {
			if (!getcwd(currdir, FILENAME_LEN)) {
				fprintf(stderr, "%s: FAIL getcwd, %s\n",
							LOG_PREFIX, strerror(errno));
				exit(1);
			}
			sprintf(currdir+strlen(currdir), "/%s", SENT_SUBDIR_NAME);
			if (!(ClientOpt.sent_dir = strdup(currdir))) {
				fprintf(stderr,
					"%s: FAIL strdup(%s), %s\n",
					LOG_PREFIX, currdir, strerror(errno));
				exit(1);
			}
		}
	}

	if (ClientOpt.fail_dir == NULL) {
		if ((p_subdir = strrchr(ClientOpt.indir_list[0], '/'))) {
			pathlen = p_subdir - ClientOpt.indir_list[0] + 1
						+ strlen(FAIL_SUBDIR_NAME) + 1;
			if (!(ClientOpt.fail_dir = malloc(pathlen))) {
				fprintf(stderr,
					"%s: FAIL malloc %d bytes for sent dir path, %s\n",
					LOG_PREFIX, pathlen, strerror(errno));
				exit(1);
			}
			sprintf(ClientOpt.fail_dir, "%.*s/%s",
						p_subdir - ClientOpt.indir_list[0],
						ClientOpt.indir_list[0], FAIL_SUBDIR_NAME);
		} else {
			if (!getcwd(currdir, FILENAME_LEN)) {
				fprintf(stderr, "%s: FAIL getcwd, %s\n",
							LOG_PREFIX, strerror(errno));
				exit(1);
			}
			sprintf(currdir+strlen(currdir), "/%s", FAIL_SUBDIR_NAME);
			if (!(ClientOpt.fail_dir = strdup(currdir))) {
				fprintf(stderr,
					"%s: FAIL strdup(%s), %s\n",
					LOG_PREFIX, currdir, strerror(errno));
				exit(1);
			}
		}
	}

	if (ClientOpt.max_queue_len == 1 && ClientOpt.wait_last_file > 0) {
		fprintf(stderr,
			"%s: ERROR max queue len must be > 1 for last file wait option!\n",
			LOG_PREFIX);
		exit(1);
	}

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

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char *			Program;		I	program name

RETURNS
	void
*******************************************************************************/
static void usage(void)
{
	static char hostbuf[128];

	fprintf(stderr, "usage: %s\n", Program);
	fprintf(stderr,
		"         [-p port]        (listen port, default=%d)\n",
		DFLT_LISTEN_PORT);
	gethostname(hostbuf, sizeof(hostbuf));
	fprintf(stderr,
		"         [-n host [-n alt ...]] (remote [+alternate] hosts, default=%s)\n",
		hostbuf);
	fprintf(stderr,
		"         [-t timeout]     (socket timeout, default=%d secs)\n",
		DFLT_TIMEOUT);
	fprintf(stderr,
		"         [-i poll_int]    (input poll interval, default=%d secs)\n", 
		DFLT_INTERVAL);
	fprintf(stderr,
		"         [-l ttl[smhd]]   (discard file after ttl secs, default=0 (never)\n");
	fprintf(stderr,
		"         [-w window_size] (ack window size, default=%d prods)\n",
		DFLT_WINSIZE);
	fprintf(stderr,
		"         [-r retries]     (max send retries, -1=infinite, default=%d)\n",
		DFLT_RETRY);
	fprintf(stderr,
		"         [-b bufsiz]      (send/recv buffer size, default=%d bytes)\n",
		DFLT_BUFSIZE);
	fprintf(stderr,
		"         [-c ttaaii_cccc] (send connect msg with wmo heading ttaaii_cccc)\n");
	fprintf(stderr,
		"         [-s source]      (set source id connection string to <source>\n");
	fprintf(stderr,
		"         [-d]             (debug mode, default NO)\n");
	fprintf(stderr,
		"         [-v lvl]         (verbosity level, default=0)\n");
	fprintf(stderr,
		"         [-a]             (archive log files, default NO)\n");
	fprintf(stderr,
		"         [-x]             (strip CCB headers, default NO)\n");
	fprintf(stderr,
		"         [-D dir1 [-D dir2 ...]](input dirs, default=<working dir>/input)\n");
	fprintf(stderr,
		"         [-L]             (wait for last file, default NO)\n");
	fprintf(stderr,
		"         [-I refresh_int] (queue refresh interval, default=%d secs)\n",
		DFLT_REFRESH);
	fprintf(stderr,
		"         [-Q queue_len]   (max queue length, default=%d prods)\n",
		DFLT_MAX_QUEUE);
	fprintf(stderr,
		"         [-S sent_dir]    (sent dir, default=<input dir>/../sent)\n");
	fprintf(stderr,
		"         [-N sent_cnt]    (keep up to <sent_cnt> files in sent dir, default=%d\n",
		ClientOpt.sent_count);
	fprintf(stderr,
		"         [-F fail_dir]    (fail dir, default=<input dir>/../fail)\n");
	fprintf(stderr,
		"         [-P log_dir]     (path for log files, default=%s)\n", LOG_DIR_PATH);
#ifdef INCLUDE_ACQ_STATS
	fprintf(stderr,
		"         [-m region]      (set shared mem to region for acq_stats\n");
	fprintf(stderr,
		"         [-h host_id]     (set shared mem host index to host_id\n");
	fprintf(stderr,
		"         [-k link_id]     (set shared mem link index to link_id\n");
#endif
} /* end usage */

/*******************************************************************************
FUNCTION NAME
	static void setup_sig_handler(void)

FUNCTION DESCRIPTION
	Set-up handler functions for signals
		SIGINT, SIGTERM	-	shutdown signal handler
		SIGPIPE			-	socket disconnection handler
		SIGALRM			-	timeout handler

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ClientOpt structure)
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
				LOG_PREFIX, SIGTERM, strerror(errno));
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
	exit, otherwise exit directly.

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES (from ClientOpt structure)
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

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	int				Flags			O	Control Flags

RETURNS
	void 
*******************************************************************************/
void pipe_sighandler(int signum)
{
	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Set disconnect flag on signal %d\n",
						LOG_PREFIX, signum);
	}
	Flags |= (DISCONNECT_FLAG|NOPEER_FLAG);

	return;
} /* end pipe_sighandler */

/*******************************************************************************
FUNCTION NAME
	void alarm_sighandler(int signum)

FUNCTION DESCRIPTION
	Set disconnect flag.

PARAMETERS
	Type			Name			I/O	Description
	int				signum			I	signal number received

GLOBAL VARIABLES (from ClientOpt structure)
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
