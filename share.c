/*******************************************************************************
FILE NAME
	share.c

FILE DESCRIPTION
	message formatting and utility functions

FUNCTIONS
	format_msghdr	- format message header
	parse_msghdr	- parse message header
	format_ack		- format ack message
	parse_ack		- parse ack message
	daemonize		- become a daemon process
	my_mkdir		- mkdir w/ wrapper
	my_rename		- rename w/ wrapper
	my_copy			- copy w/ wrapper
	get_ccb_len		- get length of CCB heading
	write_pidfile	- write pid to file

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_share_c[]= "@(#)share.c 0.7 04/12/2005 14:02:36";

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "share.h"

/*******************************************************************************
FUNCTION NAME
	int format_msghdr(char *buf, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Sprintf a message message for p_prod into buf.  Buffer must be at least
	MSG_HDR_LEN+PROD_HDR_LEN+1 bytes (sprintf appends a null to the string.)
	Assume buf is big enough since the message header length is fixed!

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			O		buffer for message header
	prod_info_t *	p_prod		I		prod info for the header

RETURNS
	 length of message
	-1: otherwise
*******************************************************************************/
int format_msghdr(char *buf, prod_info_t *p_prod)
{
	int msg_size;
	char scratchbuf[MSG_HDR_LEN+PROD_HDR_LEN+1024];

	if (p_prod->size <= 0 || p_prod->size > MAX_PROD_SIZE) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR invalid prod size %d, max %d\n",
				LOG_PREFIX, p_prod->size, MAX_PROD_SIZE);
		return -1;
	}

	if (p_prod->seqno < 0 || p_prod->seqno > MAX_PROD_SEQNO) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR invalid prod seqno %d, max %d\n",
				LOG_PREFIX, p_prod->seqno, MAX_PROD_SEQNO);
		return -1;
	}
	
	/* note PROD_HDR_LEN does not include the 10 byte message header */
	msg_size = PROD_HDR_LEN + p_prod->size;

	sprintf (scratchbuf, "%.8dBI\001\r\r\n%.5d%.10ld\r\r\n",
			msg_size, p_prod->seqno, p_prod->queue_time);

	/* header length must always equal MSG_HDR_LEN+PROD_HDR_LEN */
	if (strlen(scratchbuf) != MSG_HDR_LEN+PROD_HDR_LEN) {
		CS_LOG_ERR(ERROR_FP,"%s: ERROR invalid header length for prod %d, %s\n",
				LOG_PREFIX, p_prod->seqno, p_prod->filename);
		return -1;
	}

	/* copy header (without trailing '\0') into user buffer */
	memcpy(buf, scratchbuf, MSG_HDR_LEN+PROD_HDR_LEN);

	return MSG_HDR_LEN+PROD_HDR_LEN;
}	/* end format_msghdr */

/*******************************************************************************
FUNCTION NAME
	int parse_msghdr(char *buf, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	sscanf a message header for p_prod from buf.  Store seqno, queue_time,
	and size in p_prod struct.

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			I		message header buffer
	prod_info_t *	p_prod		O		prod info struct for parsed info 

RETURNS
	 length of message
	-1: otherwise
*******************************************************************************/
int parse_msghdr(char *buf, size_t buflen, prod_info_t *p_prod)
{
	int msg_size;
	char anbi[3];
	char scanbuf[MSG_HDR_LEN+PROD_HDR_LEN+1];

	/* header length must always equal MSG_HDR_LEN+PROD_HDR_LEN */
	if (buflen < MSG_HDR_LEN+PROD_HDR_LEN) {
		CS_LOG_ERR(ERROR_FP,
				"%s: can not parse header, min length is %d bytes\n",
				LOG_PREFIX, MSG_HDR_LEN+PROD_HDR_LEN);
		return -1;
	}

	/* copy header into a null-terminated scan buffer */
	memcpy(scanbuf, buf, MSG_HDR_LEN+PROD_HDR_LEN);
	scanbuf[MSG_HDR_LEN+PROD_HDR_LEN] = '\0';

	if (sscanf(scanbuf, "%8d%2s\001\r\r\n%5d%10ld\r\r\n",
			&msg_size, anbi, &p_prod->seqno, &p_prod->queue_time) != 4) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR invalid header received [%s]\n",
				LOG_PREFIX, scanbuf);
		return -1;
	}

	p_prod->size = msg_size - PROD_HDR_LEN;

	return MSG_HDR_LEN+PROD_HDR_LEN;
}	/* end parse_msghdr */

/*******************************************************************************
FUNCTION NAME
	int format_ack(char *buf, int seqno, char code)

FUNCTION DESCRIPTION
	Sprintf an ack message for seqno/code into buf.  Buffer must be at least
	ACK_MSG_LEN+1 bytes (sprintf appends a null to the string.)  Assume buf
	is big enough since the ack message length is fixed!

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			O		buffer for ack message
	int				seqno		I		seqno being acked
	char			code		I		ack status code

RETURNS
	 length of message
	-1: otherwise
*******************************************************************************/
int format_ack(char *buf, int seqno, char code)
{
	sprintf (buf, "%5d%c", seqno, code);

	/* ack message length must always equal ACK_MSG_LEN */
	if (strlen(buf) != ACK_MSG_LEN) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR invalid header length for ack %d\n",
				LOG_PREFIX, seqno);
		return -1;
	}

	return ACK_MSG_LEN;
}	/* end format_ack */

/*******************************************************************************
FUNCTION NAME
	int parse_ack(char *buf, int * p_seqno, char *p_code)

FUNCTION DESCRIPTION
	sscanf an ack message from buf.  Store seqno and code in p_seqno
	and p_code respectively.

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			I		ack message buffer
	int *			p_seqno		O		address for seqno 
	char *			p_code		O		address for ack status code

RETURNS
	 length of message
	-1: otherwise
*******************************************************************************/
int parse_ack(char *buf, size_t buflen, int *p_seqno, char *p_code)
{
	char scanbuf[MSG_HDR_LEN+PROD_HDR_LEN+1];

	/* ack message length must always equal ACK_MSG_LEN */
	if (buflen < ACK_MSG_LEN) {
		CS_LOG_ERR(ERROR_FP, "%s: can not parse ack, min length is %d bytes\n",
				LOG_PREFIX, ACK_MSG_LEN);
		return -1;
	}

	/* copy ack message into a null-terminated scan buffer */
	memcpy(scanbuf, buf, ACK_MSG_LEN);
	scanbuf[ACK_MSG_LEN] = '\0';

	if (sscanf(scanbuf, "%d%c", p_seqno, p_code) != 2) {
		CS_LOG_ERR(ERROR_FP, "%s: ERROR Invalid ack received [%s]\n",
				LOG_PREFIX, scanbuf);
		return -1;
	}

	return ACK_MSG_LEN;
}	/* end parse_ack */

/*******************************************************************************
FUNCTION NAME
	void daemonize(void)

FUNCTION DESCRIPTION
	Become a daemon process.  No controlling terminal, session leader, etc.

PARAMETERS
	Type			Name			I/O	Description
	void

GLOBAL VARIABLES 
	Type			Name			I/O	Description
	None

RETURNS
	void 
*******************************************************************************/
void daemonize(void)
{
	struct sigaction  act;	/* POSIX signal handler */

	switch (fork()) {
		case 0:
			/* child */
			setsid();

			/* redirect stdin, stdout, stderr to /dev/null */
			fclose(stdin);
			fclose(stdout);
			fclose(stderr);

			open("/dev/null", O_RDWR);	/* stdin (0) */
			dup(0);						/* stdout(1) */
			dup(0);						/* stderr(2) */

			sigemptyset(&act.sa_mask);
			act.sa_flags   = 0;
			act.sa_handler = SIG_IGN;
			sigaction(SIGHUP, &act, 0);	/* ignore SIGHUP */

			chdir("/");

			umask(0);
			break;

		case -1:
			/* error */
			CS_LOG_ERR(ERROR_FP,"%s: Fork failed, %s",
							LOG_PREFIX, strerror(errno));
			exit(1);
		default:
			/* parent */
			exit(0);
			break;
	}
} /* end daemonize */

/*******************************************************************************
FUNCTION NAME
	my_mkdir(const char *path)

FUNCTION DESCRIPTION
	Make a directory including any missing path components

PARAMETERS
	Type			Name		I/O		Description
	const char *	path		I		directory to create

RETURNS
	 0: successful directory creation
	-1: otherwise
*******************************************************************************/
int my_mkdir(const char *path)
{
	struct stat	stbuf;
	char *		p;

	if (stat(path, &stbuf) == 0) {
		if (S_ISDIR(stbuf.st_mode)) {
			return 0;
		} else if (unlink(path) == 0) {
			return mkdir (path, (mode_t)(S_IRWXU|S_IRWXG|S_IRWXO));
		}
	}
	else if (errno == ENOENT) {
		if ((p = strrchr(path, '/'))) {
			if (p != path) {
				*p = '\0';
				if (my_mkdir(path) == -1) {
					*p = '/';
					return -1;
				}
				*p = '/';
			}
		}
		return mkdir (path, (mode_t)(S_IRWXU|S_IRWXG|S_IRWXO));
	}
	return -1;
}	/* end my_mkdir */

/*******************************************************************************
FUNCTION NAME
	my_rename(const char *source, const char *target)

FUNCTION DESCRIPTION
	Rename a file

PARAMETERS
	Type			Name		I/O		Description
	const char *	source		I		file to be renamed
	const char *	target		I		new name

RETURNS
	 0: successful rename
	-1: otherwise
*******************************************************************************/
int my_rename(const char *source, const char *target)
{
	int last_errno = 0;
	char *p_dirslash;
	int rc;

	while ((rc = rename(source, target)) < 0) {
		if (errno == last_errno) {
			/* give up if we fail twice with the same error */
			return -1;
		}
		last_errno = errno;
		if (errno == EXDEV) {
			/* try a copy */
			if ((rc = my_copy (source, target)) != 0) {
				CS_LOG_ERR(ERROR_FP,
					"%s: FAIL copy %s to %s\n",
					LOG_PREFIX, source, target);
				return -1;
			}
			else {
				/* copy succeeded, now remove the source file */
				if (unlink(source) == -1) {
					CS_LOG_ERR(ERROR_FP,
						"%s: FAIL unlink source file %s, %s\n",
						LOG_PREFIX, source, strerror(errno));
					/* non-fatal */
				}
			}
			return 0;
		}
		else if (errno == ENOENT) {
			if ((p_dirslash = strrchr(target, '/'))) {
				if (p_dirslash != target) {
					*p_dirslash = '\0';
					rc = my_mkdir(target);
					*p_dirslash = '/';
					if (rc < 0) {
						CS_LOG_ERR(ERROR_FP,
							"%s: FAIL mkdir for file %s, %s\n",
							LOG_PREFIX, target, strerror(errno));
						return -1;
					}
					/* assume that mkdir suceeded, retry */
				}
			}
		}
		else {
			/* No known recovery method, fail */
			return -1;
		}
	}

	return 0;
}	/* end my_rename */

/*******************************************************************************
FUNCTION NAME
	my_copy(const char *source, const char *target)

FUNCTION DESCRIPTION
	Copy a file.
	Set perms to 200 while file is being copied, 666 when complete.
	Create any necessary directories to complete the path to the target.

PARAMETERS
	Type			Name		I/O		Description
	const char *	source		I		file to be copied
	const char *	target		I		new file name

RETURNS
	 0: successful file copy
	-1: otherwise
*******************************************************************************/
int my_copy(const char *source, const char *target)
{
	char buffer[8192];
	int ifd;
	int ofd;
	size_t readlen;
	size_t writelen;
	char *p_dirslash;

	if ((ifd = open (source, O_RDONLY)) < 0) {
		/**** don't log error here, let caller log it ****/
		return(-1);
	}

	/* create output file without read permissions to ensure that
	   no other process has access to it until we are done writing 
	   to it														*/

	if ((ofd = open (target, O_WRONLY|O_TRUNC|O_CREAT, S_IWUSR)) < 0) {
		/* open error handling */
		if (errno == ENOENT) {
			if ((p_dirslash = strrchr(target, '/'))) {
				if (p_dirslash != target) {
					*p_dirslash = '\0';
					if (my_mkdir(target) == -1) {
						CS_LOG_ERR(ERROR_FP,
							"%s: FAIL mkdir for directory %s, %s\n",
							LOG_PREFIX, target, strerror(errno));
						*p_dirslash = '/';
						close(ifd);
						return (-1);
					}
					*p_dirslash = '/';
					if ((ofd = open (target,
							O_WRONLY|O_TRUNC|O_CREAT, S_IWUSR)) < 0) {
						CS_LOG_ERR(ERROR_FP,
							"%s: FAIL open/create %s for writing, %s\n",
							LOG_PREFIX, target, strerror(errno));
						close(ifd);
						return(-1);
					}
				} /* end if (p_dirslash != '/') */
			} /* end if strchr(target, '/') */
		} /* end if errno == ENOENT */
	}
	if (ofd < 0) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL open/create %s for writing, %s\n",
			LOG_PREFIX, target, strerror(errno));
		close(ifd);
		return(-1);
	}

	writelen = 0;
	while ((readlen = read(ifd, buffer, sizeof(buffer))) > 0) {
		if ((writelen = write(ofd,buffer,readlen)) != readlen) {
			CS_LOG_ERR(ERROR_FP,
				"%s: FAIL write of %d bytes to %s, %s\n",
				LOG_PREFIX, readlen, target, strerror(errno));
			break;
		}
	}
	if (readlen == -1) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL read from %s, %s\n",
			LOG_PREFIX, source, strerror(errno));
		return(-1);
	}

	close(ifd);
	close(ofd);

	/* set permissions to rw for ugo for the output file */
	if (chmod(target, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH) < 0) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL change permissions of file <%s>, Error: <%s>\n",
			LOG_PREFIX, target, strerror(errno));
	}

	if (readlen == -1 || writelen == -1) {
		if (unlink(target) == -1) {
			CS_LOG_ERR(ERROR_FP,
				"%s: FAIL unlink faulty target file %s, %s\n",
				LOG_PREFIX, target, strerror(errno));
		}
		return(-1);
	}
	return(0);
}	/* end my_copy */

/*******************************************************************************
FUNCTION NAME
	get_ccb_len(char *buf, size_t buflen)

FUNCTION DESCRIPTION
	get length of CCB header if present

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			I		buffer to search
	size_t 			buflen		I		length to search

RETURNS
	>0: CCB header length
	 0: No CCB header
*******************************************************************************/
int get_ccb_len(char *buf, size_t buflen)
{
	int ccb_len;

	if (buf[CCB_FLAG_BYTE] != CCB_FLAG_VAL) {
		/* Not a CCB */
		return 0;
	}

	/* length is in 2-byte pairs so multiply by 2 */
	ccb_len = (int)(buf[CCB_LENGTH_BYTE])*2;

	/* check for valid CCB length */
	if (ccb_len > CCB_MAX_HDR_LEN
			|| ccb_len < CCB_MIN_HDR_LEN
				|| ccb_len > buflen) {
		return 0;
	}

	return ccb_len;
}

static void remove_pidfile(void);
static char *PidFile;

/*******************************************************************************
FUNCTION NAME
	write_pidfile(char *path)

FUNCTION DESCRIPTION
	write pid to pidfile

PARAMETERS
	Type			Name		I/O		Description
	char *			path		O		pidfile path

RETURNS
	void
*******************************************************************************/
void write_pidfile(char *path)
{
	FILE *fp;
	char *p_env;
	
	if ((p_env = getenv("PID_FILE"))) {
		path = p_env;
	}

	if (!(fp = fopen(path, "w"))) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL open pidfile %s, %s\n",
			LOG_PREFIX, path, strerror(errno));
		return;
	}
	
	if (fprintf(fp, "%d\n", getpid()) <= 0) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL fprintf pidfile %s, %s\n",
			LOG_PREFIX, path, strerror(errno));
	}

	PidFile = strdup(path);
	atexit(remove_pidfile);

	fclose(fp);
}

/*******************************************************************************
FUNCTION NAME
	remove_pidfile(void)

FUNCTION DESCRIPTION
	remove current pidfile

PARAMETERS
	Type			Name		I/O		Description
	void

RETURNS
	void
*******************************************************************************/
static void remove_pidfile(void)
{
	if (PidFile) {
		unlink(PidFile);

		free(PidFile);
		PidFile = NULL;
	}
}
