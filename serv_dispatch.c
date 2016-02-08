/*******************************************************************************
FILE NAME
	serv_dispatch.c

FILE DESCRIPTION
	Routines for server to dispatch and manage workers.

FUNCTIONS
	dispatcher			- listen for connections and dispatch workers
	new_listen_socket	- create a listen socket
	fork_service		- fork a worker
	verify_workers		- check worker table pids
	kill_workers		- kill all workers
	wait_for_worker		- get worker exit status

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_serv_dispatch_c[]= "@(#)serv_dispatch.c 0.4 04/12/2005 14:05:28";

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#ifdef _XOPEN_SOURCE_EXTENDED
#include <arpa/inet.h>
#endif

#include "share.h"
#include "server.h"

#define RECOVER_SLEEP		3
#define MAX_WORKER_SLEEP	30

static int new_listen_socket(unsigned int port);
static int fork_service(int listen_sd, int accept_sd);
static void verify_workers(void);

/* static variables to manage workers */
static pid_t	*WorkerPids;
static int		WorkerCount;

/*******************************************************************************
FUNCTION NAME
	int dispatcher(void) 

FUNCTION DESCRIPTION
	Listen to well known socket and for a worker to provide service for each
	accepted connection.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	unsigned int	listen_port		I	port number for listen/connect
	int				max_worker		I	maximum number of concurrent workers
	char			verbosity		I	debugging verbosity level
	int				Flags			I	Control Flags

RETURNS
	 0	Normal exit
	-1	Error
*******************************************************************************/
int dispatcher(void)
{
	int listen_sd = -1;
	int accept_sd = -1;
	struct sockaddr_in accept_addr;		/* accepted remote socket address */
	unsigned int addrlen = sizeof(struct sockaddr_in);
	struct hostent	*p_hostent;

	WorkerCount = 0;
	if (ServOpt.max_worker > 0) {
		WorkerPids = calloc(ServOpt.max_worker, sizeof(pid_t));
		if (!WorkerPids) {
			CS_LOG_ERR(ERROR_FP,
				"%s: FAIL calloc %d pid_t's, %s -- ABORT\n",
				LOG_PREFIX, ServOpt.max_worker, strerror(errno));
			exit(1);
		}
	}

	while (!(Flags & SHUTDOWN_FLAG)) {
		if (listen_sd < 0) {
			if ((listen_sd = new_listen_socket(ServOpt.listen_port)) < 0) {
				return -1;
			}
			if (ServOpt.verbosity > 0) {
				CS_LOG_DBUG(DEBUG_FP, "%s: Created listen socket %d\n",
						LOG_PREFIX, listen_sd);
			}
		}

		if (ServOpt.max_worker > 0) {
			if (WorkerCount >= ServOpt.max_worker) {
				verify_workers();
			}

			if (WorkerCount >= ServOpt.max_worker) {
				CS_LOG_ERR(ERROR_FP,
					"%s: WARNING: %d workers running, no more connections\n",
					LOG_PREFIX, WorkerCount);
				sleep(MAX_WORKER_SLEEP);
				continue;
			}
		} 

		if (ServOpt.verbosity > 0) {
			CS_LOG_DBUG(DEBUG_FP,
					"%s: accepting connections on port %d fd %d\n",
					LOG_PREFIX, ServOpt.listen_port, listen_sd);
		}

		/* accept any connection to the listening socket */
		if ((accept_sd = accept(
				listen_sd, (struct sockaddr *)&accept_addr, &addrlen)) < 0) {
			if (errno != EINTR) {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL accept, %s\n",
						LOG_PREFIX, strerror(errno));
				close(listen_sd);
				listen_sd = -1;
				sleep(RECOVER_SLEEP);
			}
			continue;
		}

		if ((p_hostent = gethostbyaddr(&accept_addr.sin_addr,
									sizeof(accept_addr.sin_addr), AF_INET))
				&& p_hostent->h_name) {
			RemoteHost = strdup(p_hostent->h_name);
		} else {
			RemoteHost = strdup("unknown");
		}

		if (ServOpt.verbosity > 0) {
			CS_LOG_DBUG(DEBUG_FP,
				"%s: Accepted connection on sd %d from host %s, port %d\n",
				LOG_PREFIX, accept_sd, RemoteHost, ntohs(accept_addr.sin_port));
		}

		if (ServOpt.max_worker > 0) {
			/* fork service */
			if (fork_service(listen_sd, accept_sd) < 0) {
				/* error, assume socket has not been shut down */
				if (shutdown(accept_sd, SHUT_RDWR) < 0) {
					CS_LOG_ERR(ERROR_FP, "%s: FAIL shutdown socket %d, %s\n",
							LOG_PREFIX, accept_sd, strerror(errno));
				}
			}
		} else {
			service(accept_sd, RemoteHost);
		}

		/* close the socket for the service */

		if (close(accept_sd) < 0) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL close socket %d, %s\n",
					LOG_PREFIX, accept_sd, strerror(errno));
		}

		free(RemoteHost);

		Flags &= ~DISCONNECT_FLAG;
	}

	if (listen_sd >= 0) {
		if (shutdown(listen_sd, SHUT_RDWR) < 0) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL shutdown listen socket %d, %s\n",
					LOG_PREFIX, listen_sd, strerror(errno));
		}
		if (close(listen_sd) < 0) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL close listen socket %d, %s\n",
					LOG_PREFIX, listen_sd, strerror(errno));
		}
	}

	kill_workers();

	free(WorkerPids);

	return 0;
} /* end dispatcher */

/*******************************************************************************
FUNCTION NAME
	int new_listen_socket(unsigned int port) 

FUNCTION DESCRIPTION
	Create a listen socket.

PARAMETERS
	Type			Name			I/O	Description
	unsigned int	port			I	port to listen on

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	none

RETURNS
	listen socket file descriptor
	-1	Error
*******************************************************************************/
static int new_listen_socket(unsigned int port)
{
	const char fname[]="new_listen_socket";
	int	 option = 1;		/* setsockopt() option */
	struct sockaddr_in local;	/* local socket address */
	int lsd;

	/* setup socket on a "well known" port */
	local.sin_family	  = (short) AF_INET;
	local.sin_port		  = (unsigned short)htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	
	/* create and bind to a reusable listen socket */
	if ((lsd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		CS_LOG_ERR(ERROR_FP, "%s: socket failed, %s\n", fname, strerror(errno));
		return -1;
	}

	if (setsockopt(lsd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int))) {
		CS_LOG_ERR(ERROR_FP, "%s: setsockopt failed, %s\n", fname,
							strerror(errno));
		return -1;
	}

	if (bind(lsd, (struct sockaddr *)&local, sizeof(local)) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: bind failed, %s\n", fname, strerror(errno));
		return -1;
	}

	/* listen for up to 10 simutaneous connections */
	if (listen(lsd, 10) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: listen failed, %s\n", fname, strerror(errno));
		return -1;
	}

	return lsd;
} /* end new_listen_socket */

/*******************************************************************************
FUNCTION NAME
	static int fork_service(int listen_sd, int accept_sd) 

FUNCTION DESCRIPTION

PARAMETERS
	Type			Name			I/O	Description
	int				listen_sd		I	listen socket file descriptor
	int				accept_sd		I	accepted socket file descriptor

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	int				max_worker		I	maximum number of concurrent workers
	char			verbosity		I	debugging verbosity level

RETURNS
	 0	Normal exit
	-1	Error
*******************************************************************************/
static int fork_service(int listen_sd, int accept_sd)
{
	int i_wrkr;
	int status;
	char pidfile[256];

	/* find an available worker slot */
	for (i_wrkr = 0; i_wrkr < ServOpt.max_worker; i_wrkr++) {
		if (WorkerPids[i_wrkr] <= 0) {
			break;
		}
	}	

	if (i_wrkr >= ServOpt.max_worker) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR no worker slots available!\n",
				LOG_PREFIX);
		verify_workers();
		return -1;
	}

	switch (WorkerPids[i_wrkr] = fork()) {
		case 0:		/* worker (child), successful fork */
			WorkerIndex = i_wrkr;
			/* Set worker's Program name to worker_# */
			sprintf (Program+strlen(Program), "_%d", WorkerIndex);

			sprintf(pidfile, "/var/run/%s-%d", Program, ServOpt.listen_port);
			write_pidfile(pidfile);

			/* worker does not need the listen socket */
			if (close(listen_sd) < 0) {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL close socket %d, %s\n",
						LOG_PREFIX, accept_sd, strerror(errno));
			}

			CS_LOG_DBUG(DEBUG_FP, "%s: Worker %d starting\n",
						LOG_PREFIX, getpid());

			/* start the service */
			if (service(accept_sd, RemoteHost) < 0) {
				status = 1;
			} else {
				status = 0;
			}

			/* assume service has shutdown the socket */
			if (close(accept_sd) < 0) {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL close socket %d, %s\n",
						LOG_PREFIX, accept_sd, strerror(errno));
			}

			CS_LOG_DBUG(DEBUG_FP, "%s: Worker %d exiting with status %d\n",
						LOG_PREFIX, getpid(), status);

			/* child does not return to dispatcher loop! */
			exit(status);
		case -1:
			/* error */
			CS_LOG_ERR(ERROR_FP,"%s: Fork failed, %s",
							LOG_PREFIX, strerror(errno));
			return -1;
		default:
			/* dispatcher (parent), successful fork */
			return 0;
	}
} /* end fork_service */

/*******************************************************************************
FUNCTION NAME
	static int verify_workers(void) 

FUNCTION DESCRIPTION
	Verify that all workers are running.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	int				max_worker		I	maximum number of concurrent workers

RETURNS
	void
*******************************************************************************/
static void verify_workers(void)
{
	int i_wrkr;

	WorkerCount = 0;
	for (i_wrkr = 0; i_wrkr < ServOpt.max_worker; i_wrkr++) {
		if (WorkerPids[i_wrkr] > 0) {
			/* verify that process exists */
			if (kill(WorkerPids[i_wrkr], 0) < 0) {
				CS_LOG_ERR(ERROR_FP, "%s: kill(%d,0) failed, %s\n",
							LOG_PREFIX, WorkerPids[i_wrkr], strerror(errno));
				/* assume this is not really a worker */
				WorkerPids[i_wrkr] = 0;
			} else {
				WorkerCount++;
			}
		}
	}
} /* end verify_workers */

/*******************************************************************************
FUNCTION NAME
	void kill_workers(void) 

FUNCTION DESCRIPTION
	Send SIGTERM to all workers.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	int				max_worker		I	maximum number of concurrent workers
	char			verbosity		I	debugging verbosity level

RETURNS
	void
*******************************************************************************/
void kill_workers(void)
{
	int i_wrkr;

	/* Could use kill(0, SIGTERM) instead to kill every process in the
	   process group, but then we might send ourselves the SIGTERM signal
	   and cause a feedback loop. */

	for (i_wrkr = 0; i_wrkr < ServOpt.max_worker; i_wrkr++) {
		if (WorkerPids[i_wrkr] > 0) {
			if (ServOpt.verbosity > 0) {
				CS_LOG_DBUG(DEBUG_FP, "%s: Sending SIGTERM to worker pid %d\n",
						LOG_PREFIX, WorkerPids[i_wrkr]);
			}
			if (kill(WorkerPids[i_wrkr], SIGTERM) < 0) {
				CS_LOG_ERR(ERROR_FP, "%s: kill(%d,SIGTERM) failed, %s\n",
							LOG_PREFIX, WorkerPids[i_wrkr], strerror(errno));
			}
		}
	}
} /* end kill_workers */

/*******************************************************************************
FUNCTION NAME
	void wait_for_worker(void) 

FUNCTION DESCRIPTION
	Get exit status from exiting worker.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	int				max_worker		I	maximum number of concurrent workers
	char			verbosity		I	debugging verbosity level

RETURNS
	void
*******************************************************************************/
void wait_for_worker(void)
{
	int wait_stat;
	int i_wrkr;
	pid_t child_pid;

	if ((child_pid = waitpid(0, &wait_stat, WNOHANG)) == -1) {
		CS_LOG_ERR(ERROR_FP, "%s: waitpid failed, %s\n",
						LOG_PREFIX, strerror(errno));
		return;
	}

	for (i_wrkr = 0; i_wrkr < ServOpt.max_worker; i_wrkr++) {
		if (WorkerPids[i_wrkr] == child_pid) {
			WorkerPids[i_wrkr] = 0;
			WorkerCount--;
			break;
		}
	}

	if (i_wrkr >= ServOpt.max_worker) {
		CS_LOG_ERR(ERROR_FP,
					"%s: ERROR child pid %d not found in worker table!\n",
					LOG_PREFIX, child_pid);
		return;
	}

	if (WIFEXITED(wait_stat)) {
		if (ServOpt.verbosity > 0) {
			CS_LOG_DBUG(DEBUG_FP, "%s: Worker pid %d exited with status %d\n",
							LOG_PREFIX, child_pid, WEXITSTATUS(wait_stat));
		}
	} else if (WIFSIGNALED(wait_stat)) {
		CS_LOG_ERR(ERROR_FP, "%s: Worker pid %d killed by signal %d\n",
						LOG_PREFIX, child_pid, WTERMSIG(wait_stat));
	}

	return;
} /* end wait_for_worker */
