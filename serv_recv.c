/*******************************************************************************
FILE NAME
	serv_recv.c

FILE DESCRIPTION
	Product receipt service routines.

FUNCTIONS
	service			- read products from socket and store them
	recv_msghdr		- read and parse a message header from the socket
	recv_prod		- get product data from socket
	recv_conn_msg	- get connection message from socket
	open_out_file	- open an output file
	send_ack		- send product acknowledgement to client
	recv_block		- read a block of data from a socket
	write_block		- write a block of data to disk
	parse_conn_msg	- parse a connection message

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_serv_recv_c[]= "@(#)serv_recv.c 0.6 05/11/2004 12:43:054";

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>

#include "share.h"
#include "server.h"

/* Big enough block to always contain a complete WMO */
#define FIRST_BLK_SIZE	1024

static int recv_msghdr(int sock_fd, int seqno, prod_info_t *p_prod);
static int recv_prod(int sock_fd, char *recvbuf, size_t bufsiz, prod_info_t *p_prod);
static int open_out_file(prod_info_t *p_prod);
static int send_ack(int sock_fd, int seqno, char code);
static int recv_block(int sock_fd, char *blkbuf, size_t minsiz, size_t maxsiz);
static int write_block(int fd, char *blkbuf, size_t blksiz);
static int recv_conn_msg(int sock_fd, char *recvbuf, size_t buflen, prod_info_t *p_prod);
static int parse_conn_msg(char *buf);

/*******************************************************************************
FUNCTION NAME
	int service(int sock_fd, char *rhost)

FUNCTION DESCRIPTION
	Allocate receive buffer, call routines to read product headers and
	data from socket, update seqno.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	char *			rhost			I	remote host name

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	size_t			bufsize			O	size to allocate for read buffer
	int				Flags			I	Control Flags

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
int service(int sock_fd, char *rhost)
{
	prod_info_t prod;
	unsigned int seqno;
	char *recvbuf;

	/* initialize */
	seqno = 0;

	if (!(recvbuf = malloc(ServOpt.bufsize))) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL malloc %d bytes for recvbuf, %s\n",
				LOG_PREFIX, ServOpt.bufsize, strerror(errno));
		return -1;
	}

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: Begin service for client on host %s\n",
					LOG_PREFIX, rhost);
	}

	/* read and process data */
	while (!(Flags & (SHUTDOWN_FLAG|DISCONNECT_FLAG))) {

		if (recv_msghdr(sock_fd, seqno, &prod) < 0) {
			break;
		}

		if (Flags & DISCONNECT_FLAG) {
			break;
		}

		if (recv_prod(sock_fd, recvbuf, ServOpt.bufsize, &prod) < 0) {
			break;
		}

		seqno = prod.seqno + 1;
	}

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: End service for client on host %s\n",
					LOG_PREFIX, rhost);
	}

	free(recvbuf);

	if (Flags & (SHUTDOWN_FLAG|DISCONNECT_FLAG)) {
		/* clean up */
		if (shutdown(sock_fd, SHUT_RDWR) < 0) {
			CS_LOG_ERR(ERROR_FP, "%s: FAIL shutdown socket %d, %s\n",
					LOG_PREFIX, sock_fd, strerror(errno));
		}
		return 0;
	}

	return -1;
}

/*******************************************************************************
FUNCTION NAME
	static int recv_msghdr(int sock_fd, int seqno, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Read and parse a message header from the socket.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	int				seqno			I	expected product sequence #
	prod_info_t *	p_prod			O	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	size_t			bufsize			O	size to allocate for read buffer
	char			verbosity		I	debugging verbosity level

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
static int recv_msghdr(int sock_fd, int seqno, prod_info_t *p_prod)
{
	int bytes_rcvd;
	char recvbuf[MSG_HDR_LEN+PROD_HDR_LEN];

	/* read and parse the header */
	if ((bytes_rcvd = recv_block(sock_fd, recvbuf,
			MSG_HDR_LEN+PROD_HDR_LEN, MSG_HDR_LEN+PROD_HDR_LEN)) <= 0) {
		/* fail read header */
		return -1;
	}

	/* parse the header */
	if (parse_msghdr(recvbuf, bytes_rcvd, p_prod) < 0) {
		/* fatal protocol error */
		return -1;
	}

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: prod seqno=%d size=%d time=%s",
				LOG_PREFIX, p_prod->seqno, p_prod->size,
				ctime(&p_prod->queue_time));
	}
	
	/* check seqno, size, etc. */
	if (p_prod->seqno != seqno) {
		/* seqno reset? */
		if (p_prod->seqno != 0) {
			/* seqno error */
			CS_LOG_ERR(ERROR_FP, "%s: ERROR expected seqno %d but got %d\n",
					LOG_PREFIX, seqno, p_prod->seqno);
			return -1;
		}
	}

	if (p_prod->size <= 0 || p_prod->size > MAX_PROD_SIZE) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR invalid prod size %d, max %d\n",
				LOG_PREFIX, p_prod->size, MAX_PROD_SIZE);
		return -1;
	}

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	static int recv_prod(int sock_fd, char *recvbuf, size_t bufsiz, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Read product data from socket, write data to file call data handler,
	and send ack/nack.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	char *			recvbuf			I	buffer for data read from socket
	size_t			bufsiz			I	size of buffer
	prod_info_t *	p_prod			I	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	size_t			bufsize			O	size to allocate for read buffer
	char			verbosity		I	debugging verbosity level
	int				outfile_flags	I	flags for perms toggle

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
static int recv_prod(int sock_fd, char *recvbuf, size_t bufsiz, prod_info_t *p_prod)
{
	int bytes_rcvd;
	int bytes_left;
	size_t recvsiz;
	size_t minsiz;
	int out_fd;
	int	rc;
	char ack_code;

	/* initialize */
	out_fd = -1;

	/* make sure 1st buffer is big enough to contain the WMO */
	minsiz = MIN(p_prod->size, FIRST_BLK_SIZE);

	/* read and process the product */
	for (bytes_left = p_prod->size; bytes_left > 0; bytes_left -= bytes_rcvd) {
		recvsiz = MIN(bytes_left, bufsiz);	/* don't read past end of product */
		bytes_rcvd = recv_block(sock_fd, recvbuf, minsiz, recvsiz);
		if (bytes_rcvd < 0) {
			/* fail read block, close and abort product */
			if (out_fd >= 0) {
				close(out_fd);
				out_fd = -1;
			}
			if (bytes_left < p_prod->size) {
				/* abort this product if we have already started it */
				abort_recv(p_prod);
			}
			return -1;
		} else if (bytes_rcvd == 0) {
			/* interrupted?, try again */
			continue;
		}

		/* write each block */
		if (bytes_left == p_prod->size) {
			/* 1st block */

			/* get wmo heading */
			if (parse_wmo(recvbuf, bytes_rcvd, p_prod) < 0) {
				CS_LOG_ERR(ERROR_FP,
						"%s: FAIL parse wmo prod %d buf [%s], ttaaii=%s\n",
						LOG_PREFIX, p_prod->seqno,
						debug_buf(recvbuf, bytes_rcvd>50?50:bytes_rcvd),
						p_prod->wmo_ttaaii);
				/* process anyway */
			}

			/* Only need a minimum size for the 1st block */
			minsiz = 1;

			/* check for connection message */
			if (p_prod->seqno == 0 && ServOpt.connect_wmo &&
					!strcmp(p_prod->wmo_ttaaii, ServOpt.connect_wmo)) {
				return recv_conn_msg(sock_fd, recvbuf, bytes_rcvd, p_prod);
			}

			/* get output file */
			if (get_out_path(p_prod) < 0) { /* NOT connection msg */

				/*  FAIL create file name... assume we want to discard */
				CS_LOG_ERR(ERROR_FP,
							"%s: FAIL get_out_path, discard prod %d\n",
							LOG_PREFIX, p_prod->seqno);
				ack_code = ACK_FAIL;
			}

			/* open output file */
			if (p_prod->filename) {
				if ((out_fd = open_out_file(p_prod)) < 0) {
					/* can't open file, assume we want to retry later */
					ack_code = ACK_RETRY;
				}
			}
		}

		/* write block of data */
		if (out_fd < 0) {
			/* assume we want to discard */
			if (ServOpt.verbosity > 0) {
				CS_LOG_DBUG(DEBUG_FP, "%s: discarding %d bytes\n",
						LOG_PREFIX, p_prod->size);
			}
		} else if (write_block(out_fd, recvbuf, bytes_rcvd) < bytes_rcvd) {
			/* can't write, close and abort product */
			close(out_fd);
			out_fd = -1;
			abort_recv(p_prod);

			/* set nack code but keep reading socket to stay in sync */
			ack_code = ACK_RETRY;
		}
	}

	/* close file */
	if (out_fd >= 0) {
		close(out_fd);
		out_fd = -1;

		if (ServOpt.outfile_flags & TOGGLE_PERMS_FLAG) {
			/* set permissions to rw for ugo for the output file */
			if (chmod(p_prod->filename, DFLT_FILE_PERMS) < 0) {
				/* perhaps file was moved/removed before completion? */
				CS_LOG_ERR(ERROR_FP,
					"%s: Fail change permissions of file <%s>, Error: <%s>\n",
					LOG_PREFIX, p_prod->filename, strerror(errno));
				abort_recv(p_prod);
				ack_code = ACK_RETRY;
				if (send_ack(sock_fd, p_prod->seqno, ack_code) < 0) {
					/* fatal socket error */
					return -1;
				}
				return 0;
			}
		}

		/* Do whatever else is required to finish this product */
		if ((rc = finish_recv(p_prod)) < 0) {
			ack_code = ACK_FAIL;
		} else  if (rc > 0) {
			ack_code = ACK_RETRY;
		} else {
			ack_code = ACK_OK;
		}
	}

	/* send acknowledgement */
	if (send_ack(sock_fd, p_prod->seqno, ack_code) < 0) {
		/* fatal socket error */
		return -1;
	}

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	static int open_out_file(prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Open output file.  Handle errors by retrying when the problem appears to
	be with the file system or output directory.   If the problem appears to
	be file-specific, or we can't figure out what the problem is, return -1
	and let the service nack-retry to the client.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			I	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	int				outfile_flags	I	flags for perms toggle and overwrite
	int				Flags			I	Control Flags

RETURNS
	 output file descriptor
	-1	Error
*******************************************************************************/
static int open_out_file(prod_info_t *p_prod)
{
	int out_fd;
	int retry;
	time_t sleeptime;
	mode_t perms;
	int oflags;
	char *dirslash;

	out_fd = -1;

	if (ServOpt.outfile_flags & TOGGLE_PERMS_FLAG) {
		perms = S_IWUSR;
	} else {
		perms = DFLT_FILE_PERMS;
	}

	if (ServOpt.outfile_flags & OVER_WRITE_FLAG) {
		oflags = O_WRONLY|O_CREAT|O_TRUNC;
	} else {
		oflags = O_WRONLY|O_CREAT|O_EXCL;
	}

	for (retry = 0; !(Flags & DISCONNECT_FLAG); retry++) {
		if ((out_fd = open(p_prod->filename, oflags, perms)) < 0) {
			/* don't log missing directory, we will create it */
			if (errno != ENOENT && retry == 0) {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL %d open file %s, %s\n",
								LOG_PREFIX, retry+1, p_prod->filename,
								strerror(errno));
			}
			switch (errno) {
				case EEXIST: /* no-overwrite mode queue over-run?*/
				case ENOSPC: /* full file system */
					/* break out of switch to sleep-retry */
					break;
				case ENOTDIR:
					/* the directory is a file... remove it and retry */
					if ((dirslash = strrchr(p_prod->filename, '/'))) {
						*dirslash = '\0';
						if (unlink(p_prod->filename) < 0) {
							CS_LOG_ERR(ERROR_FP, "%s: FAIL unlink file %s, %s\n",
												LOG_PREFIX, p_prod->filename,
												strerror(errno));
						} else if (my_mkdir(p_prod->filename) < 0) {
							CS_LOG_ERR(ERROR_FP, "%s: FAIL mkdir %s, %s\n",
												LOG_PREFIX, p_prod->filename,
												strerror(errno));
						} else if (retry == 0) {
							*dirslash = '/';
							continue;		/* first retry, don't sleep */
						}
						*dirslash = '/';
					} else {
						return -1;	/* should not happen */
					}
					break;
				case ENOENT:
					/* the directory is missing... create it and retry */
					if ((dirslash = strrchr(p_prod->filename, '/'))) {
						*dirslash = '\0';
						if (my_mkdir(p_prod->filename) < 0) {
							CS_LOG_ERR(ERROR_FP, "%s: FAIL mkdir %s, %s\n",
												LOG_PREFIX, p_prod->filename,
												strerror(errno));
						} else if (retry == 0) {
							*dirslash = '/';
							continue;		/* first retry, don't sleep */
						}
						*dirslash = '/';
					} else {
						return -1;	/* should not happen */
					}
					break;
				case EISDIR:
					/* the file is a directory, remove it and retry */
					if (rmdir(p_prod->filename) < 0) {
						CS_LOG_ERR(ERROR_FP, "%s: FAIL rmdir %s, %s\n",
								LOG_PREFIX, p_prod->filename, strerror(errno));
						return -1;			/* give up */
					} else if (retry == 0) {
						continue;			/* first retry, don't sleep */
					} else {
						return -1;			/* give up */
					}
					break;
				case EINTR:
					/* interrupted by signal, check flags and try again */
					continue;			/* don't sleep */
				default:
					/* anything else is fatal */
					return -1;
			}
			if (Flags & SHUTDOWN_FLAG) {
				/* don't retry if shutting down */
				return -1;
			}
			/* sleep and retry */
			if (retry <  3) {
				sleeptime = SHORT_RETRY_SLEEP;
			} else {
				sleeptime = LONG_RETRY_SLEEP;
			}
			if (ServOpt.verbosity > 1) {
				CS_LOG_DBUG(DEBUG_FP, "%s: Retry #%d in %ld seconds\n",
					LOG_PREFIX, retry+1, sleeptime);
			}
			sleep(sleeptime);
		} else {
			/* file is open */
			if (retry > 0) {
				CS_LOG_ERR(ERROR_FP, "%s: OK open file %s, after %d retries\n",
							LOG_PREFIX, p_prod->filename, retry);
			}
			break; /* out of for loop */
		}
	}

	if (ServOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: open next file %s returning fd %d\n",
					LOG_PREFIX, p_prod->filename, out_fd);
	}

	return out_fd;
}

/*******************************************************************************
FUNCTION NAME
	static int recv_block(int sock_fd, char *blkbuf, size_t minsiz, size_t maxsiz) 

FUNCTION DESCRIPTION
	Read a block at least minsiz but no larger than maxsiz from socket.  Uses
	alarm syscall and signal handler for timeout.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	char *			blkbuf			O	buffer for data read from socket
	size_t			minsiz			I	minimum size to read
	size_t			maxsiz			I	maximum size to read

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	time_t			timeout			I	timeout interval (on socket)
	int				Flags			I	Control Flags

RETURNS
	 total bytes read into buffer
	-1	Error
*******************************************************************************/
static int recv_block(int sock_fd, char *blkbuf, size_t minsiz, size_t maxsiz) 
{
	int bytes_rcvd;
	size_t bytes_total;
	size_t recvsiz;
	char *recvbuf;

	if (ServOpt.timeout > 0) {
		alarm(ServOpt.timeout);
	}

	bytes_total = 0;
	bytes_rcvd = 0;
	while (!(Flags & DISCONNECT_FLAG) && bytes_total < minsiz) {
		recvbuf = blkbuf + bytes_total;
		recvsiz = maxsiz - bytes_total;
		if ((bytes_rcvd = recv(sock_fd, recvbuf, recvsiz, 0)) < 0) {
			/* error, unless due to an interrupt */
			if (errno == EINTR) {
				CS_LOG_DBUG(DEBUG_FP, "%s: recv syscall interrupted\n",
						LOG_PREFIX);
				if ((Flags & SHUTDOWN_FLAG) && bytes_total == 0) {
					/* let caller decide whether to exit or retry */
					return 0;
				}
			} else {
				CS_LOG_ERR(ERROR_FP, "%s: FAIL recv from socket, %s\n",
						LOG_PREFIX, strerror(errno));
				break;
			}
		} else if (bytes_rcvd == 0) {
			/* this is usually due to a disconnect */
			CS_LOG_ERR(ERROR_FP,
					"%s: Recv 0 bytes from socket, flag disconnect\n",
					LOG_PREFIX);
			Flags |= DISCONNECT_FLAG;
			break;
		} else {
			bytes_total += bytes_rcvd;
		}
	}

	if (ServOpt.timeout > 0) {
		alarm(ServOpt.timeout);
	}

	if (bytes_rcvd <= 0) {
		return -1;
	}

	if (ServOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: received %d bytes\n", LOG_PREFIX, bytes_total);
	}

	return bytes_total;
}

/*******************************************************************************
FUNCTION NAME
	static int write_block(int fd, char *blkbuf, size_t blksiz)

FUNCTION DESCRIPTION
	Read a block at least minsiz but no larger than maxsiz from socket.  Uses
	alarm syscall and signal handler for timeout.

PARAMETERS
	Type			Name			I/O	Description
	int				fd				I	output file descriptor
	char *			blkbuf			I	buffer of data to write
	size_t			blksiz			I	amount of data to write

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	int				Flags			I	Control Flags

RETURNS
	 bytes of data written
	-1	Error
*******************************************************************************/
static int write_block(int fd, char *blkbuf, size_t blksiz)
{
	size_t bytes_left;
	int wbytes;
	int retry;
	int sleeptime;

	retry = 0;
	bytes_left = blksiz;

	while (bytes_left > 0) {
		if ((wbytes = write(fd, blkbuf, bytes_left)) < 0) {
			CS_LOG_ERR(ERROR_FP,
				"%s: FAIL %d write %d bytes to file desc %d, %s\n",
				LOG_PREFIX, retry+1, blksiz, fd, strerror(errno));
			switch (errno) {
				case EINTR:
					/* interrupted by signal, check flags and retry */
					break;
				case ENOSPC:
					/* don't retry if shutting down */
					if (Flags & SHUTDOWN_FLAG) {
						return -1;
					}
					/* full file system, sleep and retry */
					if (retry <  3) {
						sleeptime = SHORT_RETRY_SLEEP;
					} else {
						sleeptime = LONG_RETRY_SLEEP;
					}
					if (ServOpt.verbosity > 1) {
						CS_LOG_DBUG(DEBUG_FP, "%s: Retry #%d in %d seconds\n",
							LOG_PREFIX, retry+1, sleeptime);
					}
					sleep(sleeptime);
					break;
				default:
					/* anything else is fatal */
					return -1;
			}
			retry++;
		} else {
			/* write succeeded */
			blkbuf += wbytes;
			bytes_left -= wbytes;
		}
	}

	if (bytes_left == 0 && retry > 0) {
		CS_LOG_ERR(ERROR_FP, "%s: OK write to fd %d, after %d retries\n",
						LOG_PREFIX, fd, retry);
	}

	if (ServOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: wrote %d of %d bytes to file\n",
						LOG_PREFIX, blksiz - bytes_left, blksiz);
	}

	return blksiz - bytes_left;
}

/*******************************************************************************
FUNCTION NAME
	static int send_ack(int sock_fd, int seqno, char code)

FUNCTION DESCRIPTION
	Create an ack message and write it to the socket.  Use alarm syscall
	with sighandler to timeout to prevent infinite block.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	int				seqno			I	product seqno to ack
	char			code			I	ack code (ACK, NACK, or RETRY)

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	int				Flags			I	Control Flags

RETURNS
	 bytes of data written
	-1	Error
*******************************************************************************/
static int send_ack(int sock_fd, int seqno, char code)
{
	char ackbuf[ACK_MSG_LEN+1];		/* sized for ack_msg + null terminator */
	int	acklen;
	int bytes_sent;

	if ((acklen = format_ack(ackbuf, seqno, code)) < 0) {
		return -1;
	}

	/* set alarm for timeout on send */
	if 	(ServOpt.timeout > 0) {
		alarm(ServOpt.timeout);
	}
	while ((bytes_sent = send(sock_fd, ackbuf, acklen, 0)) < 0) {
		if (errno == EINTR) {
			/* interrupted by signal */
			if (Flags & DISCONNECT_FLAG) {
				/* disconnect (SIGPIPE) detected, break out of send loop */
				break;
			} else {
				/* try send again */
				continue;
			}
		}

		/* any other error is a failure, break out of send loop */
		CS_LOG_ERR(ERROR_FP, "%s: FAIL send ack for prod %d to socket, %s\n",
				LOG_PREFIX, seqno, strerror(errno));
		break;
	}

	/* cancel alarm if pending */
	if (ServOpt.timeout > 0) {
		alarm(0);
	}

	if (bytes_sent != acklen) {
		return -1;
	}

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	static int recv_conn_msg(int sock_fd, char *buf, size_t buflen, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Read connection msg data from socket, parse data, load ConnInfo struct,
	and send ack/nack.

PARAMETERS
	Type			Name			I/O	Description
	int				sock_fd			I	socket file descriptor
	char *			buf				I	start of conn msg data
	size_t			buflen			I	bytes of conn msg data in buffer
	prod_info_t *	p_prod			I	address of prod info structure

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	struct			ConnInfo		O	Connection information

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
static int recv_conn_msg(int sock_fd, char *buf, size_t buflen, prod_info_t *p_prod)
{
	char *msgbuf;
	int bytes_left;
	int bytes_rcvd;
	char ack_code;
	char *p_suff;
	struct tm *p_tm;
	char timebuf[DATESTR_MAX_LEN];
	time_t now;

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: recv connect msg [%s %s %s] %d bytes\n",
				LOG_PREFIX, p_prod->wmo_ttaaii, p_prod->wmo_cccc,
				p_prod->wmo_ddhhmm, p_prod->size); 
	}

	if (!(msgbuf = malloc(p_prod->size))) {
		CS_LOG_ERR(ERROR_FP, "%s: FAIL malloc buflen=%d, %s\n",
				LOG_PREFIX, buflen, strerror(errno));
		return -1;
	}

	memcpy(msgbuf, buf, buflen);
	bytes_left = p_prod->size - buflen;

	if (bytes_left > 0) {
		if ((bytes_rcvd = recv_block(
				sock_fd, msgbuf+buflen, bytes_left, bytes_left)) <= 0) {
			/* fail read connect message */
			free(msgbuf);
			return -1;
		}
	}
	msgbuf[p_prod->size] = '\0';

	if (parse_conn_msg(msgbuf) < 0) {
		ack_code = ACK_FAIL;
	} else {
		ack_code = ACK_OK;
	}
	free(msgbuf);

	strcpy(ConnInfo.wmo_cccc, p_prod->wmo_cccc);

	if (send_ack(sock_fd, p_prod->seqno, ack_code) < 0) {
		/* fatal socket error */
		return -1;
	}

	if (!(p_suff = strrchr(Program, '_'))) {
		p_suff = Program+strlen(Program);
	}

	if (ConnInfo.source[0]) {
		sprintf(p_suff, "-%s", ConnInfo.source);
	} else if (ConnInfo.remotehost[0]) {
		sprintf(p_suff, "-%s", ConnInfo.remotehost);
	}

	/* update product log file name */
	if (PRODUCT_FP == &LogFile) {
		rename_log(PRODUCT_FP, Program);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	CS_LOG_PROD(PRODUCT_FP,
		"CONNECT %s WMO[%-6s %-4s %-6s %-3s] {%s} REMOTE=%s SOURCE=%s LINK=%d\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb, p_prod->wmo_nnnxxx,
		ConnInfo.remotehost, ConnInfo.source, ConnInfo.link_id);

	return 0;
}

/*******************************************************************************
FUNCTION NAME
	static int parse_conn_msg(char *buf)

FUNCTION DESCRIPTION
	Parse null-terminated connection msg data and load ConnInfo struct.

PARAMETERS
	Type			Name			I/O	Description
	char *			buf				I	null-terminated connection msg data

GLOBAL VARIABLES (from ServOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	debugging verbosity level
	struct			ConnInfo		O	Connection information

RETURNS
	 0	Normal return
	-1	Error
*******************************************************************************/
static int parse_conn_msg(char *buf)
{
	char *line;
	char *tok;
	char *val;

	if (ServOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP,
				"%s: parsing connect msg %s\n",
				LOG_PREFIX, buf);
	}

	memset(&ConnInfo, '\0', sizeof(ConnInfo));

	for (line = strtok(buf, "\r\n"); line; line = strtok(NULL, "\r\n")) {
		if (!strcmp(line, CONN_MSG_START)) {
			break;
		}
	}

	if (!line) {
		return -1;
	}

	for (tok = strtok(NULL, "\r\n\t "); tok; tok = strtok(NULL, "\r\n\t ")) {
		if (!strcmp(tok, REMOTE_ID)) {
			if (val = strtok(NULL, "\r\n\t ")) {
				sprintf(ConnInfo.remotehost, "%.*s",
						sizeof(ConnInfo.remotehost), val);
			}
		} else if (!strcmp(tok, SOURCE_ID)) {
			if (val = strtok(NULL, "\r\n\t ")) {
				sprintf(ConnInfo.source, "%.*s",
						sizeof(ConnInfo.source), val);
			}
		} else if (!strcmp(tok, LINK_ID)) {
			if (val = strtok(NULL, "\r\n\t ")) {
				ConnInfo.link_id = atoi(val);
			}
		} else {
			CS_LOG_ERR(ERROR_FP, "%s: Invalid connect message, token=%s\n",
					LOG_PREFIX, tok);
			return -1;
		}
		if (!val) {
			CS_LOG_ERR(ERROR_FP,
					"%s: Invalid connect message, no value for token=%s\n",
					LOG_PREFIX, tok);
			return -1;
		}
	}

	return 0;
}
