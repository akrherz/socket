/*******************************************************************************
FILE NAME
	client_send.c

FILE DESCRIPTION
	Poll and send loop for client and supporting routines

FUNCTIONS
	poll_and_send			- poll for next file and send it
	get_sockaddr			- create socket address from host/port
	connect_to_server		- connect to server via socket
	disconnect_from_server	- disconnect from server
	send_prod				- send a product to the server
	check_for_ack			- check if any acknowledgements are waiting
	recv_ack				- read and process acknowledgement
	push_prod				- push a product onto a list
	pop_prod				- pop a product from a list
	rebuild_lists			- rebuild product table lists
	attach_acqshm			- attach to acq_table shared memory (for monitoring)
	create_conn_msg			- create file to send as connection message

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

	Mon Jun 20 14:01:43 EDT 2005 - Added TTL option and ignore .dot files

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_client_send_c[]= "@(#)client_send.c 0.15 06/20/2005 14:24:38";

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>

#ifdef _XOPEN_SOURCE_EXTENDED
#include <arpa/inet.h>
#endif

#include "client.h"
#include "share.h"

#ifdef INCLUDE_ACQ_STATS
#	include <cp_product.h>
#	include <acq_shm_lib.h>
#	define ACQ_STATS(x) x
#else
#	define ACQ_STATS(x) 
#endif

static int ProdSeqno = 0;

#define TIMEOUT_TIME(p)		(p->send_time + ClientOpt.timeout - time(NULL))
#define NEXT_SEQNO(x)		((x+1) % (MAX_PROD_SEQNO+1))
#define RECOVERY_SLEEP		20

static int get_sockaddr(char *host, unsigned int port, struct sockaddr_in *p_addr);
static int connect_to_server(char *host);
static void disconnect_from_server(int sock_fd);
#ifdef INCLUDE_ACQ_STATS
static int send_prod(int sock_fd, prod_info_t *p_prod, DIST_INFO *p_stats);
#else
static int send_prod(int sock_fd, prod_info_t *p_prod);
#endif
static int check_for_ack(int sock_fd, time_t timeout);
static int recv_ack(int sock_fd, prod_info_t *p_ack, char * p_code);
static void push_prod(prod_list_t *p_list, prod_info_t *p_prod);
static prod_info_t *pop_prod(prod_list_t *p_list);
static void rebuild_lists(prod_tbl_t *p_tbl);
static prod_info_t *create_conn_msg(prod_tbl_t *p_tbl);
#ifdef INCLUDE_ACQ_STATS
static DIST_INFO *attach_acqshm(void);
#endif

/*******************************************************************************
FUNCTION NAME
	int poll_and_send(void) 

FUNCTION DESCRIPTION
	Send products to receive server and process acknowledgements

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	unsigned int	port			I	port number for listen/connect
	char *			host			I	remote host of server
	char			verbosity		I	debugging verbosity level
	time_t			timeout			I	timeout interval (on socket)
	time_t			poll_interval	I	input polling interval (when idle)
	time_t			queue_ttl		I	queue time-to-live
	int				window_size		I	maximum outstanding acks
	int				Flags			I	Control Flags

RETURNS
	 0	Normal exit
	-1	Error
*******************************************************************************/
int poll_and_send(void)
{
	int	sock_fd;
	prod_tbl_t prod_tbl;
	prod_info_t	*p_prod;
	prod_info_t	*p_connect;
	int i;
	int ack_ready;
	int wait_time;
	int connect_failures;
	int input_failures;
	int	queue_len;
	int	host_idx;
	char ack_code;
	ACQ_STATS(DIST_INFO *p_stats;)

	sock_fd = -1;
	queue_len = 0;
	host_idx = 0;

	/* initialize product table */
	memset(&prod_tbl, '\0', sizeof(prod_tbl));
	if (!(prod_tbl.prod = (prod_info_t *)
					calloc(ClientOpt.window_size, sizeof(prod_info_t)))) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL calloc %d prod_info structs, %s\n",
					LOG_PREFIX, ClientOpt.window_size, strerror(errno));
		return -1;
	}

	for (i = 0; i < ClientOpt.window_size; i++) {
		prod_tbl.prod[i].state = STATE_FREE;
		push_prod(&prod_tbl.free_list, &prod_tbl.prod[i]);
	}

	connect_failures = 0;
	input_failures = 0;
	p_prod = NULL;
	p_connect = NULL;

	ACQ_STATS(p_stats = attach_acqshm();)

	/* create a connection message to send first */
	if (ClientOpt.connect_wmo) {
		p_prod = p_connect = create_conn_msg(&prod_tbl);
	}

	/* read and process data */
	while (!(Flags & SHUTDOWN_FLAG)) {

		/* check disconnect flag */
		if (sock_fd >= 0 && (Flags & DISCONNECT_FLAG)) {
			disconnect_from_server(sock_fd);
			sock_fd = -1;
			ACQ_STATS(p_stats->host_socket_id = -1;)
			if (ClientOpt.connect_wmo) {
				/* push curr prod on retr list, unless it is a conn msg */
				if (p_prod && p_prod != p_connect) {
					push_prod(&prod_tbl.retr_list, p_prod);
				}
				/* create a connection message to send on reconnect */
				p_prod = p_connect = create_conn_msg(&prod_tbl);
			}
		}

		/* connect now if not already connected */
		if (sock_fd < 0) {
			ACQ_STATS(strcpy(p_stats->host_name, ClientOpt.host);)
			if ((sock_fd =
					connect_to_server(ClientOpt.host)) < 0) {
				connect_failures++;
				host_idx++;
				if (ClientOpt.host_list[host_idx] == NULL) {
					host_idx = 0;
				}
				ClientOpt.host = ClientOpt.host_list[host_idx];
				ACQ_STATS(p_stats->host_socket_id = -1;)
				ACQ_STATS(p_stats->host_conn_fails++;)
			} else {
				ACQ_STATS(p_stats->host_socket_id = sock_fd;)
				ACQ_STATS(p_stats->host_last_conn_time = time(NULL);)
				ACQ_STATS(p_stats->host_conn_fails = 0;)

				/* if acks are pending for sent items, re-queue them */
				for (i = 0; prod_tbl.ack_list.count > 0; i++) {
					prod_info_t *p_retr;
					if (!(p_retr = pop_prod(&prod_tbl.ack_list))) {
						/* This should never happen */
						CS_LOG_ERR(ERROR_FP,
							"%s: ERROR, ack list underflow, count = %d\n",
							LOG_PREFIX, prod_tbl.ack_list.count);
						rebuild_lists(&prod_tbl);
						continue;
					}
					if (p_retr == p_connect) {
						/* don't retransmit connection message */
						continue;
					}
					/* send only counts against the next prod in ack list */
					if (i > 0 && p_prod->send_count > 0) {
						p_prod->send_count--;
					}
					if (ClientOpt.verbosity > 0) {
						CS_LOG_DBUG(DEBUG_FP,
							"%s: resend seq=%d f(%s) bytes(%d)\n",
							LOG_PREFIX, p_retr->seqno,
							p_retr->filename, p_retr->size); 
					}
					push_prod(&prod_tbl.retr_list, p_retr);
				}
				connect_failures = 0;
			}
		}

		/* get next product if we don't have one and ack window is not full */
		if (!p_prod) {
			if (prod_tbl.ack_list.count < ClientOpt.window_size) {
				if (prod_tbl.retr_list.count > 0) {
					/* get a retransmission */
					if (!(p_prod = pop_prod(&prod_tbl.retr_list))) {
						/* This should never happen */
						CS_LOG_ERR(ERROR_FP,
							"%s: ERROR, retr list underflow, count = %d\n",
							LOG_PREFIX, prod_tbl.retr_list.count);
						rebuild_lists(&prod_tbl);
					}
				} else {
					/* get a new product out of the queue */
					if (!(p_prod = pop_prod(&prod_tbl.free_list))) {
						/* This should never happen */
						CS_LOG_ERR(ERROR_FP,
							"%s: ERROR, free list underflow, ack_count = %d\n",
							LOG_PREFIX, prod_tbl.ack_list.count);
						rebuild_lists(&prod_tbl);
						continue;
					}
					if ((queue_len = get_next_file(&prod_tbl, p_prod)) < 0) {
						input_failures++;
					} else {
						input_failures = 0;
						ACQ_STATS(p_stats->list_dist_hdr.count = queue_len;) 
						if (queue_len > 0) {
							p_prod->state = STATE_QUEUED;
							ACQ_STATS(p_stats->client_wait_state = WAIT_NONE;) 
						} else {
							ACQ_STATS(p_stats->client_wait_state = WAIT_PROD;) 
							/* no product to send, release prod entry */
							push_prod(&prod_tbl.free_list, p_prod);
							p_prod = NULL;
						}
					}
				}
			} else if (prod_tbl.ack_list.count == ClientOpt.window_size) {
				if (ClientOpt.verbosity > 0) {
					CS_LOG_DBUG(DEBUG_FP,
							"%s: Full window skip get_next_file\n",
							LOG_PREFIX);
				}
			} else {
				/* This should never happen */
				CS_LOG_ERR(ERROR_FP, "%s: ERROR ack queue overflow\n",
						LOG_PREFIX);
				rebuild_lists(&prod_tbl);
			}
		}

		/* check TTL */
		if (p_prod) {
			if (ClientOpt.queue_ttl > 0) {
				if (time(NULL) > p_prod->queue_time + ClientOpt.queue_ttl) {
					CS_LOG_ERR(ERROR_FP,
							"%s: Discarding %s, age=%d ttl=%d secs\n",
							LOG_PREFIX, p_prod->filename, 
							time(NULL)-p_prod->queue_time,
							ClientOpt.queue_ttl);
					p_prod->state = STATE_DEAD;
					abort_send(p_prod);
					push_prod(&prod_tbl.free_list, p_prod);
					p_prod = NULL;
					ACQ_STATS(p_stats->host_write_fails++;)
				}
			}
		}

		/* if we are connected and have a product to send */
		if (p_prod && sock_fd >= 0) {
			/* send the product, disconnect if error writing to socket */
			ACQ_STATS(p_stats->host_xfr_status = CLIENT_XFR_INPROG;)
			ACQ_STATS(p_stats->client_wait_state = WAIT_BUFF;) 
#			ifdef INCLUDE_ACQ_STATS
				if (send_prod(sock_fd, p_prod, p_stats) == 0)
#			else
				if (send_prod(sock_fd, p_prod) == 0)
#			endif
			{
				/* successfully sent! */
				ACQ_STATS(p_stats->client_prod_seqno = p_prod->seqno;)
				ACQ_STATS(p_stats->client_tot_prods++;)
				ACQ_STATS(p_stats->client_tot_bytes_sent += p_prod->size;)
				ACQ_STATS(p_stats->host_last_send_time = time(NULL);)
				ACQ_STATS(p_stats->host_write_fails = 0;)
				ACQ_STATS(strcpy(p_stats->host_nfs_file_name,p_prod->filename);)
				push_prod(&prod_tbl.ack_list, p_prod);
				p_prod = NULL;
			} else if (p_prod->state == STATE_FAILED) {
				/* error */
				abort_send(p_prod);
				push_prod(&prod_tbl.free_list, p_prod);
				p_prod = NULL;
				ACQ_STATS(p_stats->host_write_fails++;)
			} /* else retry p_prod next time through the loop */

			ACQ_STATS(p_stats->client_wait_state = WAIT_NONE;) 
			ACQ_STATS(p_stats->host_xfr_status = CLIENT_XFR_IDLE;)
		}

		/* set ack_ready if acks are pending to get into the loop */
		ack_ready = prod_tbl.ack_list.count;

		/* while we are connected and have acks ready to read, process them */
		while (sock_fd >= 0 && prod_tbl.ack_list.p_head && ack_ready > 0) {
			/* Block for an ack if the window is full */
			if (prod_tbl.ack_list.count == ClientOpt.window_size) {
				wait_time = TIMEOUT_TIME(prod_tbl.ack_list.p_head);
				if (wait_time < 0) {
					wait_time = 0;
				}
				if (ClientOpt.verbosity > 0) {
					CS_LOG_DBUG(DEBUG_FP,
							"%s: FULL WINDOW, blocking up to %d sec for ack\n",
							LOG_PREFIX, wait_time);
				}
			} else {
				wait_time = 0; /* don't block for acks */
			}

			/* now set ack_ready for real */
			if ((ack_ready = check_for_ack(sock_fd, wait_time)) > 0) {
				prod_info_t *p_ack;
				if (!(p_ack = pop_prod(&prod_tbl.ack_list))) {
					/* This should never happen */
					CS_LOG_ERR(ERROR_FP,
						"%s: ERROR, ack list underflow, count = %d\n",
						LOG_PREFIX, prod_tbl.ack_list.count);
					rebuild_lists(&prod_tbl);
					continue; /* ack reading loop */
				}

				if (recv_ack(sock_fd, p_ack, &ack_code) < 0) {
					ack_ready = -1;
					Flags |= DISCONNECT_FLAG;
					push_prod(&prod_tbl.ack_list, p_ack);
					break; /* out of ack reading loop */
				}

				switch(ack_code) {
					case ACK_OK:
						p_ack->state = STATE_ACKED;
						finish_send(p_ack);
						/* Update filename to sent dir name when last 
						   pending ack is received */
						if (!prod_tbl.ack_list.p_head) {
							ACQ_STATS(strcpy(p_stats->host_nfs_file_name,
											p_ack->filename);)
						}
						prod_tbl.prod[i].state = STATE_FREE;
						push_prod(&prod_tbl.free_list, p_ack);
						break;
					case ACK_FAIL:
						p_ack->state = STATE_NACKED;
						abort_send(p_ack);
						prod_tbl.prod[i].state = STATE_FREE;
						push_prod(&prod_tbl.free_list, p_ack);
						break;
					case ACK_RETRY:
						if (p_ack == p_connect) {
							/* don't retry connect msg */
							CS_LOG_ERR(ERROR_FP,
								"%s: ERROR, retry for conn msg aborted\n",
								LOG_PREFIX);
							prod_tbl.prod[i].state = STATE_FREE;
							push_prod(&prod_tbl.free_list, p_ack);
						} else {
							p_ack->state = STATE_RETRY;
							retry_send(p_ack);
							push_prod(&prod_tbl.retr_list, p_ack);
						}
						break;
					default:
						CS_LOG_ERR(ERROR_FP,
								"%s: ERROR Invalid ack code %d\n",
								LOG_PREFIX, ack_code);
						ack_ready = -1;
						push_prod(&prod_tbl.ack_list, p_ack);
						Flags |= DISCONNECT_FLAG;
						break;
				}
				if (p_ack == p_connect) {
					p_connect = NULL;
				}
			} else if (ack_ready == 0) {
				/* no acks waiting, check for ack timeout */
				if (TIMEOUT_TIME(prod_tbl.ack_list.p_head) <= 0) {
					CS_LOG_ERR(ERROR_FP, "%s: ERROR ack seqno %d timed out!\n",
								LOG_PREFIX, prod_tbl.ack_list.p_head->seqno);
					Flags |= DISCONNECT_FLAG;
				}
			} else {
				/* else error checking for ack */
				Flags |= DISCONNECT_FLAG;
			}
		}

		if (!(Flags & DISCONNECT_FLAG) && (queue_len <= 0 || sock_fd < 0)) {
			/* Can't send anything now, so sleep */
			if (connect_failures > 3 || input_failures > 3) {
				wait_time = RECOVERY_SLEEP;
			} else if (prod_tbl.ack_list.count) {
				wait_time = MIN(ClientOpt.poll_interval,
								TIMEOUT_TIME(prod_tbl.ack_list.p_head));
			} else {
				wait_time = ClientOpt.poll_interval;
			}
			sleep(wait_time);
		}
	}

	/* clean up */
	if (sock_fd >= 0) {
		disconnect_from_server(sock_fd);
		sock_fd = -1;
	}
	free(prod_tbl.prod);

	ACQ_STATS(p_stats->client_id = 0;)
	ACQ_STATS(p_stats->host_last_conn_time = 0;)

	return 0;
} /* end poll_and_send */

/*******************************************************************************
FUNCTION NAME
	int connect_to_server(char *host) 

FUNCTION DESCRIPTION
	Connect to server listening at socket address *p_sockaddr.

PARAMETERS
	Type			Name			I/O	Description
	char *			host			I	remote hostname

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	unsigned int	port			I	port number for listen/connect
	char			verbosity		I	debugging verbosity level
	time_t			timeout			I	timeout interval (on socket)
	int				ProdSeqno		O	reset prod seqno upon connection

RETURNS
	 socket descriptor or
	-1	Error
*******************************************************************************/
static int connect_to_server(char *host)
{
	int sock_fd;	/* socket desc */
	unsigned int addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in sockaddr;

	/* initialize socket info */
	if (get_sockaddr(host, ClientOpt.port, &sockaddr) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL get sockaddr for host/port %s/%d, %s\n",
				LOG_PREFIX, host, ClientOpt.port, strerror(errno));
		return -1;
	}

	if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL create socket, %s\n",
				LOG_PREFIX, strerror(errno));
		return -1;
	}

	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Connecting on socket %d\n",
				LOG_PREFIX, sock_fd);
	}

	/* set alarm for timeout on connect */
	if (ClientOpt.timeout > 0) {
		alarm(ClientOpt.timeout);
	}

	if (connect(sock_fd, (const struct sockaddr *)&sockaddr, addrlen) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL connect to port %d on host %s, %s\n",
						LOG_PREFIX, ClientOpt.port, host, strerror(errno));
		if (errno == ECONNREFUSED || errno == ETIMEDOUT) {
			if (ClientOpt.verbosity > 0) {
				CS_LOG_DBUG(DEBUG_FP,
						"%s: No server listening to port %d on host %s\n",
						LOG_PREFIX, ClientOpt.port, host);
			}
		}
		/* use a new socket descriptor each time */
		close(sock_fd);
		sock_fd = -1;
	} else {
		/* success */

		CS_LOG_PROD(PRODUCT_FP,
			"STATUS CONNECT [%s] pid(%d) %s to=%s/%d dir(%s%s)\n",
				Program, getpid(),
				ClientOpt.source ? ClientOpt.source : "unknown",
				ClientOpt.host, ClientOpt.port, ClientOpt.indir_list[0],
				ClientOpt.indir_list[1]?",...":"");

		ProdSeqno = 0;
	}

	/* cancel alarm if set */
	if (ClientOpt.timeout > 0) {
		alarm(0);
	}

	return sock_fd;

} /* end connect_to_server */

/*******************************************************************************
FUNCTION NAME
	static int get_sockaddr(char *host, unsigned int port, struct sockaddr_in *p_addr)

FUNCTION DESCRIPTION
	Construct socket address based on host/port

PARAMETERS
	Type			Name			I/O	Description
	char *			host			I	remote host of server
	unsigned int	port			I	port number for server
	sockaddr_in *	p_sockaddr		O	pointer to socket address

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	none

RETURNS
	 0	Success
	-1	Error
*******************************************************************************/
static int get_sockaddr(char *host, unsigned int port, struct sockaddr_in *p_addr)
{
	struct hostent *p_hostent;

	if (!(p_hostent = gethostbyname((const char *) host))) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL gethostbyname(%s), %s\n",
				LOG_PREFIX, host, strerror(errno));
		return -1;
	}

	p_addr->sin_family = AF_INET;
	p_addr->sin_port = htons(port);
	memcpy(&p_addr->sin_addr, p_hostent->h_addr_list[0], p_hostent->h_length);

	return 0;
} /* end get_sockaddr */

/*******************************************************************************
FUNCTION NAME
	static void disconnect_from_server(int sock_fd)

FUNCTION DESCRIPTION
	Shutdown connection and close socket.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket to disconnect/close

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	int				Flags			I+O	Control Flags

RETURNS
	void
*******************************************************************************/
static void disconnect_from_server(int sock_fd)
{
	CS_LOG_DBUG(DEBUG_FP, "%s: disconnecting from remote host on fd %d\n",
			LOG_PREFIX, sock_fd);

	/* don't bother with the shutdown if our peer is already gone */
	if (!(Flags & NOPEER_FLAG)) {
		if (shutdown(sock_fd, SHUT_RDWR) < 0) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL shutdown socket %d, %s\n",
					LOG_PREFIX, sock_fd, strerror(errno));
			/* fall through and try the close anyway */
		}
	} else {
		Flags &= ~NOPEER_FLAG;
	}

	if (close(sock_fd) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL close socket %d, %s\n",
				LOG_PREFIX, sock_fd, strerror(errno));
	}

	Flags &= ~DISCONNECT_FLAG;

	return;
} /* end disconnect_from_server */

/*******************************************************************************
FUNCTION NAME
	static int send_prod(int sock_fd, prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Send product to server

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket to send on
	prod_info_t *	p_prod			I	address of prod to send 

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	size_t			bufsize			I	max size to read/write from socket
	int				max_retry		I	max number of send retries per prod
	time_t			timeout			I	timeout interval (on socket)
	char			verbosity		I	debugging verbosity level
	int				Flags			I+O	Control Flags
	int				ProdSeqno		O	increment prod seqno after each send

RETURNS
	 0	Success
	-1	Error
*******************************************************************************/
#ifdef INCLUDE_ACQ_STATS
	static int send_prod(int sock_fd, prod_info_t *p_prod, DIST_INFO *p_stats) 
#else
	static int send_prod(int sock_fd, prod_info_t *p_prod) 
#endif
{
	int prod_fd;
	int bytes_read;
	int bytes_sent;
	int	bytes_left;
	size_t read_size;
	size_t send_size;
	static char *sendbuf;
	char *readbuf;
	size_t data_offset;

	if (!sendbuf) {
		if (!(sendbuf = malloc(ClientOpt.bufsize))) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL malloc %d bytes for sendbuf, %s\n",
					LOG_PREFIX, ClientOpt.bufsize, strerror(errno));
			/* this is a fatal error */
			exit(1);
		}
	}

	if (ClientOpt.max_retry > 0 && p_prod->send_count > ClientOpt.max_retry) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL prod #%d (%s) after %d retries\n",
				LOG_PREFIX, p_prod->seqno, p_prod->filename,
				ClientOpt.max_retry);
		p_prod->state = STATE_FAILED;
		return -1;
	}
	p_prod->send_count++;

	if ((prod_fd = open(p_prod->filename, O_RDONLY)) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL open prod file %s, %s\n",
				LOG_PREFIX, p_prod->filename, strerror(errno));
		p_prod->state = STATE_FAILED;
		return -1;
	}

	p_prod->seqno = ProdSeqno;
	if (ClientOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Sending prod seq %d %s [%d bytes] try=%d\n",
						LOG_PREFIX, p_prod->seqno, p_prod->filename,
						p_prod->size, p_prod->send_count); 
	}

	/* offset readbuf in sendbuf by FIXED size of header for first block */
	read_size = ClientOpt.bufsize - MSG_HDR_LEN - PROD_HDR_LEN;
	readbuf = sendbuf + MSG_HDR_LEN + PROD_HDR_LEN;

	bytes_left = p_prod->size;
	bytes_sent = 0;
	p_prod->ccb_len = 0;
	ACQ_STATS(p_stats->client_buff_last = 0;)
	ACQ_STATS(p_stats->client_prod_bytes_sent = 0;)
	while (bytes_left > 0) {
		if ((bytes_read = read(prod_fd, readbuf, read_size)) < 0) {
			if (errno == EINTR) {
				/* interrupted by signal, try read again */
				continue;
			} else {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL read prod file %s, %s\n",
						LOG_PREFIX, p_prod->filename, strerror(errno));
				p_prod->state = STATE_FAILED;
				break;
			}
		}

		data_offset = 0;

		/* check product size */
		if (bytes_read == 0 || bytes_read > bytes_left) {
			CS_LOG_ERR(ERROR_FP,
					"%s: ERROR file %s size changed from %d to %d bytes\n",
					LOG_PREFIX,
					p_prod->filename, p_prod->size + p_prod->ccb_len,
					p_prod->size + p_prod->ccb_len - bytes_left + bytes_read);
			p_prod->state = STATE_FAILED;
			break;
		}

		/* check if we are sending the first block */
		if (bytes_left == p_prod->size) {

			/* check for CCB heading */
			if (ClientOpt.strip_ccb &&
					(p_prod->ccb_len = get_ccb_len(readbuf, bytes_read)) > 0) {
				/* found ccb */
				CS_LOG_DBUG(DEBUG_FP,
					"%s: Found CCB len %d in file %s seqno %d\n",
					LOG_PREFIX, p_prod->ccb_len, p_prod->filename,
					p_prod->seqno);
				p_prod->size -= p_prod->ccb_len;

				/* shift data past the CCB header */
				memmove(readbuf,
						readbuf + p_prod->ccb_len,
						bytes_read - p_prod->ccb_len);

			} else {
				p_prod->ccb_len = 0;
			}

			data_offset = p_prod->ccb_len;

			/* parse the wmo if we don't already have it */
			if (p_prod->wmo_ttaaii[0] == '\0') {
				if (parse_wmo(readbuf, bytes_read, p_prod) < 0) {
					CS_LOG_ERR(ERROR_FP,
							"%s: FAIL parse wmo prod %d buf [%s], ttaaii=%s\n",
							LOG_PREFIX, p_prod->seqno,
							debug_buf(readbuf, bytes_read>50?50:bytes_read),
							p_prod->wmo_ttaaii);
					/* process anyway */
				}
			}

			if (format_msghdr(sendbuf, p_prod) < 0) {
				/* invalid product, skip to next */
				p_prod->state = STATE_FAILED;
				close(prod_fd);
				return -1;
			}
		}

		/* set alarm for timeout on send */
		if 	(ClientOpt.timeout > 0) {
			alarm(ClientOpt.timeout);
		}

		send_size = readbuf - sendbuf + bytes_read - data_offset;

		if (ClientOpt.verbosity > 1) {
			CS_LOG_DBUG(DEBUG_FP, "%s: Sending seqno %d, %d bytes\n",
							LOG_PREFIX, p_prod->seqno, send_size); 
		}

		while ((bytes_sent = send(sock_fd, sendbuf, send_size, 0)) < 0) {
			if (errno == EINTR) {
				/* interrupted by signal */
				if (Flags & DISCONNECT_FLAG) {
					/* disconnect (SIGPIPE) detected, break out of send loop */
					break; /* out of while-send */
				}
			} else {
				/* any other error is a failure, break out of send loop */
				CS_LOG_ERR(ERROR_FP, "%s: FAIL[%d] send %s to socket, %s\n",
						LOG_PREFIX, p_prod->send_count, p_prod->filename,
						strerror(errno));
				Flags |= (DISCONNECT_FLAG|NOPEER_FLAG);
				break; /* out of while-send */
			}
		}

		/* cancel alarm if pending */
		if (ClientOpt.timeout > 0) {
			alarm(0);
		}

		/* check if we broke out of the while-send loop */
		if (bytes_sent != send_size) {
			p_prod->state = STATE_RETRY;
			break; /* out of while-read */
		}

		bytes_left -= bytes_read;
		read_size = ClientOpt.bufsize;
		readbuf = sendbuf;

		ACQ_STATS(p_stats->client_tot_buffs++;)
		ACQ_STATS(p_stats->client_buff_last++;)
		ACQ_STATS(p_stats->client_prod_bytes_sent += bytes_sent;)
	}

	close(prod_fd);

	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Sent prod %d f(%s) bytes(%d+%d)\n",
					LOG_PREFIX, p_prod->seqno, p_prod->filename,
					p_prod->size, p_prod->ccb_len); 
	}

	if (bytes_left > 0) {
		/* we did not finish this product, handle the error */
		if (bytes_sent > 0) {
			ProdSeqno = NEXT_SEQNO(ProdSeqno);
			/* need to disconnect to syncronize with server */
			Flags |= DISCONNECT_FLAG;
		}
		return -1;
	} else {
		ProdSeqno = NEXT_SEQNO(ProdSeqno);
		p_prod->state = STATE_SENT;
		time(&p_prod->send_time);
		return 0;
	}
} /* end send_prod */

/*******************************************************************************
FUNCTION NAME
	static int check_for_ack(int sock_fd, time_t timeout)

FUNCTION DESCRIPTION
	Check socket for acknowledgements waiting.  

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket to send on
	time_t			timeout			I	seconds to wait before timing out

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	unsigned int	port			I	port number for listen/connect

RETURNS
	 1	1 or more acks are ready
	 0	No acks are ready
	-1	Error
*******************************************************************************/
static int check_for_ack(int sock_fd, time_t timeout)
{
	fd_set readfds;
	fd_set errorfds;
	int n_select;
	struct timeval tvs;

	if (ClientOpt.port == DISCARD_PORT) {
		return 1; /* pretend ack is ready so we can pretend to read it */
	}

	if (ClientOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Checking for acks with timeout=%ld secs\n",
						LOG_PREFIX, timeout); 
	}

	tvs.tv_sec = timeout;
	tvs.tv_usec = 0;

	FD_ZERO(&readfds);
	FD_SET(sock_fd, &readfds);
	FD_ZERO(&errorfds);
	FD_SET(sock_fd, &errorfds);
 
	while ((n_select = select(sock_fd+1, &readfds, 0, &errorfds, &tvs)) < 0) {
		if (errno == EINTR) {
			if (ClientOpt.verbosity > 1) {
				CS_LOG_DBUG(DEBUG_FP, "%s: select interrupted by signal\n",
							LOG_PREFIX); 
			}	
			return 0;
		} else {
			/* error */
			CS_LOG_ERR(ERROR_FP, "%s: FAIL select, %s\n",
							LOG_PREFIX, strerror(errno));
			return -1;
		}
	}
	
	if (n_select == 0) {
		/* timeout */
		if (ClientOpt.verbosity > 1) {
			CS_LOG_DBUG(DEBUG_FP, "%s: Timeout waiting for ack\n", LOG_PREFIX); 
		}
		return 0;
	} else {
		if (FD_ISSET(sock_fd, &readfds)) {
			if (ClientOpt.verbosity > 2) {
				CS_LOG_DBUG(DEBUG_FP, "%s: ack socket is ready to read\n",
								LOG_PREFIX); 
			}
			return 1;
		} else if (FD_ISSET(sock_fd, &errorfds)) {
			CS_LOG_ERR(ERROR_FP, "%s: Error reported on socket\n", LOG_PREFIX); 
			return -1;
		}
	}

	CS_LOG_ERR(ERROR_FP, "%s: ERROR in select logic!\n", LOG_PREFIX); 
	return -1;
}

/*******************************************************************************
FUNCTION NAME
	static int recv_ack(int sock_fd, prod_info_t *p_ack, char *p_code) 

FUNCTION DESCRIPTION
	Read an acknowledgement from socket and parse and check 
	the message then return code to sender via p_code argument.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket to send on
	prod_info_t *	p_ack			I	product for which ack is expected
	char *			p_code			O	ack-type code

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	int				Flags			I+O	Control Flags
	unsigned int	port			I	port number for listen/connect

RETURNS
	 0	Success
	-1	Error
*******************************************************************************/
static int recv_ack(int sock_fd, prod_info_t *p_ack, char *p_code)
{
	char recvbuf[ACK_MSG_LEN+1];
	char *p_buf;
	int recv_bytes;
	size_t bytes_left;
	char code;
	int seqno;

	if (ClientOpt.port == DISCARD_PORT) {
		*p_code = ACK_OK;	/* pretend we read OK */
		return 0;
	}

	bytes_left = ACK_MSG_LEN;
	p_buf = recvbuf;
	while (bytes_left > 0) {
		recv_bytes = recv(sock_fd, p_buf, bytes_left, 0);
		if (recv_bytes < 0) {
			if (errno == EINTR) {
				if (Flags & DISCONNECT_FLAG) {
					return -1;
				} else {
					continue;
				}
			} else {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL recv from socket, %s\n",
						LOG_PREFIX, strerror(errno));
				return -1;
			}
		} else if (recv_bytes == 0) {
			/* this is usually just a disconnect */
			CS_LOG_ERR(ERROR_FP,
					"%s: Recv 0 bytes from socket, flag reconnect\n",
					LOG_PREFIX);
			Flags |= (DISCONNECT_FLAG|NOPEER_FLAG);
			return -1;
		}
		bytes_left -= recv_bytes;
		p_buf += recv_bytes;
	}

	if (parse_ack(recvbuf, recv_bytes, &seqno, &code) < 0) {
		return -1;
	}

	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Ack received for prod %d, code = %c\n",
				LOG_PREFIX, seqno, code);
	}

	if (seqno != p_ack->seqno) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR Invalid ack expected #%d, but got %d\n",
				LOG_PREFIX, p_ack->seqno, seqno);
		return -1;
	}
	 
	if (code != ACK_OK && code != ACK_RETRY && code != ACK_FAIL) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR Invalid ack code %d\n",
				LOG_PREFIX, code);
		return -1;
	}

	*p_code = code;

	return 0;
}	/* end recv_ack */

/*******************************************************************************
FUNCTION NAME
	static void push_prod(prod_list_t *p_list, prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Push a product onto a product list (stack).

PARAMETERS
	Type			Name			I/O	Description
	prod_list_t *	p_list			I	product list head
	prod_info_t *	p_prod			I	product to push

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description

RETURNS
	void
*******************************************************************************/
static void push_prod(prod_list_t *p_list, prod_info_t *p_prod)
{
	if (p_list->p_tail) {
		p_list->p_tail->p_next = p_prod;
	} else {
		p_list->p_head = p_prod;
	}
	p_list->p_tail = p_prod;
	p_prod->p_next = NULL;
	p_list->count++;
}

/*******************************************************************************
FUNCTION NAME
	static prod_info_t pop_prod(prod_list_t *p_list) 

FUNCTION DESCRIPTION
	Pop a product from a product list (stack).

PARAMETERS
	Type			Name			I/O	Description
	prod_list_t *	p_list			I	product list head

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description

RETURNS
	popped product or NULL
*******************************************************************************/
static prod_info_t *pop_prod(prod_list_t *p_list)
{
	prod_info_t *p_prod;

	p_prod = p_list->p_head;
	if (p_list->p_head) {
		p_list->p_head = p_list->p_head->p_next;
		if (!p_list->p_head) {
			p_list->p_tail = NULL;
		}
		p_list->count--;
		p_prod->p_next = NULL;
	}
	return p_prod;
}

/*******************************************************************************
FUNCTION NAME
	static void rebuild_lists(prod_tbl_t *p_tbl) 

FUNCTION DESCRIPTION
	Rebuild free_list, ack_list, and retr_list using prod state info.

PARAMETERS
	Type			Name			I/O	Description
	prod_tbl_t *	p_tbl			I	product list head

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	int				window_size		I	maximum outstanding acks

RETURNS
	void
*******************************************************************************/
static void rebuild_lists(prod_tbl_t *p_tbl)
{
	int i;

	CS_LOG_ERR(ERROR_FP, "%s: Before rebuild free = %d, ack = %d, retr = %d\n",
				LOG_PREFIX, p_tbl->free_list.count, p_tbl->ack_list.count,
				p_tbl->retr_list.count);

	p_tbl->free_list.p_head = NULL;
	p_tbl->ack_list.p_head = NULL;
	p_tbl->retr_list.p_head = NULL;
	p_tbl->free_list.p_tail = NULL;
	p_tbl->ack_list.p_tail = NULL;
	p_tbl->retr_list.p_tail = NULL;
	p_tbl->free_list.count = 0;
	p_tbl->ack_list.count = 0;
	p_tbl->retr_list.count = 0;
	for (i = 0; i < ClientOpt.window_size; i++) {
		p_tbl->prod[i].p_next = NULL;
		switch (p_tbl->prod[i].state) {
			case STATE_QUEUED:
			case STATE_RETRY:
				push_prod(&p_tbl->retr_list, &p_tbl->prod[i]);
				break;
			case STATE_SENT:
				push_prod(&p_tbl->ack_list, &p_tbl->prod[i]);
				break;
			case STATE_ACKED:
			case STATE_NACKED:
			case STATE_FAILED:
			case STATE_DEAD:
			case STATE_FREE:
			default:
				push_prod(&p_tbl->free_list, &p_tbl->prod[i]);
				break;
		}
	}

	CS_LOG_ERR(ERROR_FP, "%s: After rebuild free = %d, ack = %d, retr = %d\n",
				LOG_PREFIX, p_tbl->free_list.count, p_tbl->ack_list.count,
				p_tbl->retr_list.count);

}

/*******************************************************************************
FUNCTION NAME
	static DIST_INFO *attach_acqshm(void)

FUNCTION DESCRIPTION
	Attach to acq_table shared memory structure and initialize appropriate
	dist_info structure for this client.

PARAMETERS
	Type			Name			I/O	Description

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	int				verbosity		I	log level
	int				shm_region		I	shared memory region
	int				link_id			I	datastream index
	int				host_id			I	host index
	char *			host			I	server hostname
	int				port			I	server port number

RETURNS
	Pointer to acq_table dist_info structure
*******************************************************************************/
#ifdef INCLUDE_ACQ_STATS
static DIST_INFO *attach_acqshm(void)
{
    ACQ_TABLE *p_acqtbl;
	DIST_INFO *p_stats;

	p_stats = NULL;

	/* attach to shared memory acquisition table if available */
	p_acqtbl = NULL;
	if (ClientOpt.shm_region >= 0) {
		GET_SHMPTR(p_acqtbl, ACQ_TABLE,
				SHMKEY_REGION(ACQ_TABLE_SHMKEY, ClientOpt.shm_region),
				ClientOpt.verbosity);
	}

	if (!p_acqtbl) {
		p_stats = malloc(sizeof(DIST_INFO));
	} else {
		p_stats = &p_acqtbl[ClientOpt.link_id].group_info[0].dist_info[ClientOpt.host_id];
	}

	p_stats->client_id = getpid();
	p_stats->client_output_type = CLIENT_OUT_ACQSVR; /* kludge */
	p_stats->host_socket_id = 99; /* kludge */
	p_stats->host_id = ClientOpt.host_id;

	if (ClientOpt.port == DISCARD_PORT) {
		strcpy(p_stats->host_name, "null");
	} else {
		strcpy(p_stats->host_name, ClientOpt.host);
	}
	p_stats->client_exit_status = 0;
	p_stats->client_tot_prods = 0;
	p_stats->client_tot_buffs = 0;
	p_stats->client_tot_bytes_sent = 0;
	p_stats->client_prod_bytes_sent = 0;
	p_stats->client_prod_tot_bytes = 0;
	p_stats->client_prod_delays = 0;
	p_stats->client_buff_delays = 0;
	p_stats->client_prod_seqno = 0;
	*p_stats->host_mount_pathname = '\0';
	p_stats->host_nfs_file_name[0] = '\0';
	p_stats->host_server_port = 0;
	p_stats->host_send_port = ClientOpt.port;

	/* initialize statistics */
	p_stats->host_last_conn_time = 0;
	p_stats->host_last_send_time = 0;
	p_stats->host_write_fails = 0;
	p_stats->host_conn_fails = 0;

	p_stats->client_exec_status = CLIENT_EXEC_IDLE;
	p_stats->client_start_time = time(NULL);
	p_stats->client_stop_time = 0;

	p_stats->client_send_prod_type = PROD_TYPE_NWSTG;
	p_stats->client_send_prod_cat = 0;
	p_stats->client_send_prod_code = 0;
	p_stats->client_send_ctl_status = 0;
	p_stats->client_send_flag = 0;
	p_stats->client_send_write_len = 0;

	p_stats->client_prod_state = PROD_NONE;
	p_stats->client_prod_tot_bytes = 0;
	p_stats->client_tot_prods_lost_errs = 0;
	p_stats->client_prod_error_flag = 0;
	p_stats->client_prod_errs = 0;
	p_stats->client_wr_flag = 0;
	p_stats->client_wait_state = WAIT_NONE; 

	return p_stats;
}
#endif

/*******************************************************************************
FUNCTION NAME
	static prod_info_t *create_conn_msg(prod_tbl_t *p_tbl)

FUNCTION DESCRIPTION
	Create a temporary connection message file to send as first product
	to server.

PARAMETERS
	Type			Name			I/O	Description

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	prod_tbl_t *	p_tbl			I	product list head
	char *			connect_wmo		I	WMO heading of connection message
	char *			source			I	source ID string for this datastream 
	int				link_id			I	datastream index

RETURNS
	Pointer to prod_info structure for connection product
*******************************************************************************/
static prod_info_t *create_conn_msg(prod_tbl_t *p_tbl)
{
	prod_info_t *p_prod;
	int fd;
	FILE *fp;
	time_t now;
	struct tm *p_tms;
	char buff[512];

	if (!(p_prod = pop_prod(&p_tbl->free_list))) {
		/* This should never happen */
		CS_LOG_ERR(ERROR_FP,
			"%s: ERROR, free list underflow, count = %d\n",
			LOG_PREFIX, p_tbl->free_list.count);
		rebuild_lists(p_tbl);
		return NULL;
	}

	memset(p_prod, '\0', sizeof(prod_info_t));

	/* make file in temporary directory, NOT input directory */
	sprintf(p_prod->filename, "%s/connXXXXXX", TEMP_DIR_NAME);
	if ((fd = mkstemp(p_prod->filename)) < 0) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL mkstemp, %s\n",
					LOG_PREFIX, strerror(errno));
		push_prod(&p_tbl->free_list, p_prod);
		return NULL;
	}

	if (!(fp = fdopen(fd, "w"))) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL fdopen %d, %s\n",
					LOG_PREFIX, fd, strerror(errno));
		push_prod(&p_tbl->free_list, p_prod);
		return NULL;
	}

	time(&now);
	p_tms = gmtime(&now);
	gethostname(buff, sizeof(buff));

	p_prod->size = fprintf(fp, "%s %.2d%.2d%.2d\r\r\n",
					ClientOpt.connect_wmo,
					p_tms->tm_mday, p_tms->tm_hour, p_tms->tm_min);
	p_prod->size += fprintf(fp, "\n%s\n", CONN_MSG_START);
	p_prod->size += fprintf(fp, "%s %s\n",
					SOURCE_ID, ClientOpt.source?ClientOpt.source:"UNKNOWN");
	p_prod->size += fprintf(fp, "%s %d\n", LINK_ID, ClientOpt.link_id);

	p_prod->size += fprintf(fp, "%s %s\n", REMOTE_ID, buff);

	fclose(fp);

	time(&p_prod->queue_time);

	return p_prod;
}
