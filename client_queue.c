/*******************************************************************************
FILE NAME
	client_queue.c

FILE DESCRIPTION
	Routines to manipulate input queue.

FUNCTIONS
	get_next_file - returns path of next item to send
	compare_items - used internally by get_next_file to sort queue
	finish_send -	marks file as sent successfully 
	abort_send -	marks file as un-sendable

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

	02/28/06                 - Move ignore .dot files to start of while
	Jun 20 14:01:43 EDT 2005 - Added TTL option and ignore .dot files

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_client_queue_c[]= "@(#)client_queue.c 0.9 07/12/2005 15:37:37";

#include "client.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

int compare_items(const void *p_v1, const void *p_v2);
int check_window(prod_tbl_t *p_tbl, char *filename);

#define PERM_MASK S_IRUSR|S_IRGRP|S_IROTH  /* permissions for read */
#define A_FEW_SECONDS 3

/*******************************************************************************
FUNCTION NAME
	int get_next_file (prod_tbl_t *p_tbl, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Gets the path, timestamp, size, and priority attributes of the next
	file in the input queue and returns them via the p_prod argument.

	Files are polled from those listed in ClientOpt.indir_list.  The last
	path in indir_list must be NULL.  The list of directories is assumed to
	be in order of decreasing priority.  I.e. products will be taken from
	the first directory before being taken from the other directories.  Files
	polled from the indir_list are checked against files in the table to
	determine whether they are already in progress.  If they are found
	in the prod_tbl they are ignored, otherwise they are added to the queue.

	The ClientOpt.refresh_interval is the interval at which the queue is
	rebuilt.  A higher refresh_interval increases throughput by decreasing
	the number of times the directories are polled for new items.  However, 
	when there is a backlog, higher priority items may have to wait for the
	refresh interval before they are inserted into the queue.  The queue is
	always refreshed when it is empty, regardless of how long it has been
	since the last refresh.

	The ClientOpt.max_queue_len is the maximum number of items to read from
	the input directories before sorting and returning the next item.  This 
	increases throughput in severe backlogged conditions by decreasing the
	amount of time required to poll the input directories and sort the
	items.

	The order of files returned is determied by priority then timestamp
	(st_mtime).  If a file does not have read permission, it is assumed
	to be in-progress and is not returned.  If a file has a size of 0,
	it is assumed to be in-progress and is not returned unless it is more
	than A_FEW_SECONDS old.
	
	If the ClientOpt.wait_last_file option is set, the most recent item to
	arrive in the queue is not sent until another item arrives.  This option
	is used when there is no method to determine whether a file in the input
	queue is complete and ready to send.  This option can affect timeliness
	of products so it should only be used when the files can not be 
	atomically moved into the input directory, or have their mode toggled
	to indicate that the file is complete.

PARAMETERS
	Type			Name			I/O	Description
	prod_tbl_t *	p_tbl			O	address of prod table 
	prod_info_t *	p_prod			O	address of prod info stucture 

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char **			indir_list		I	null-terminated list of input dirs
	char *			sent_dir		I	holding directory for queued files
	time_t			refresh_interval I	queue resort/refresh interval
	int 			max_queue_len	I	max number of items to sort
	int				wait_last_file	I	don't send the last file
	char			verbosity		I	verbosity level

RETURNS
	 Length of queue INCLUDING the returned product
	 0 indicates an empty queue, and no product is returned
	-1 indicates an error
	(If queue length is >= 0, the p_prod argument is not modified.)
*******************************************************************************/
int get_next_file(prod_tbl_t *p_tbl, prod_info_t *p_prod)
{
	static prod_info_t *queue;
	static int qcnt;
	static int qidx;
	static int polltime;
	DIR *p_dir;
	struct dirent *p_dirent;
	struct stat stat_struct;
	int priority;
	int i_dir;
	char *poll_dir;
	char pathbuf[FILENAME_LEN];

	if (ClientOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s: qlen = %d refresh timer = %ld\n",
				LOG_PREFIX, qcnt - qidx,
				(ClientOpt.refresh_interval>0 && polltime>0)?
				polltime+ClientOpt.refresh_interval-time(NULL):0);
	}

	/* check if we need to poll the directory */
	if (qcnt - qidx == 0 ||
			(ClientOpt.refresh_interval > 0
			&& time(NULL) >= polltime + ClientOpt.refresh_interval)) {

		if (queue) {
			free(queue);
			queue = NULL;
		}

		qcnt = 0;
		qidx = 0;

		/* Assume Poll Directories are in prioritized order.  Count
		   directories and assign a relative priority to items found
		   in each directory.  Higher priority value items are taken 
		   before lower priority items (see compare_items).
		*/

		priority = -1;
		for (i_dir = 0; ClientOpt.indir_list[i_dir]; i_dir++) {
			priority++;
		}

		for (i_dir = 0; ClientOpt.indir_list[i_dir]; i_dir++, priority--) {

			poll_dir = ClientOpt.indir_list[i_dir];

			/* read all directory entries in from input directories */
			if (!(p_dir = opendir(poll_dir))) {
				CS_LOG_ERR(ERROR_FP, "%s: Fail open directory %s, %s\n", 
							LOG_PREFIX, poll_dir, strerror(errno));
				/* Perhaps directory was removed?
				   Continue with next dir
				   Poll Interval should keep us from spinning
				 */
				continue;
			}

			while ((p_dirent = readdir(p_dir))) {

				/* skip .dot files */
				if (!strncmp(p_dirent->d_name, ".", 1)) {
					continue;
				}
				sprintf (pathbuf, "%s/%s", poll_dir, p_dirent->d_name);
				if (stat(pathbuf, &stat_struct) < 0) {
					CS_LOG_ERR(ERROR_FP, "%s: Fail stat file %s, %s\n", 
								LOG_PREFIX, pathbuf, strerror(errno));
					continue;
				}
				if (!(stat_struct.st_mode & (S_IFREG|S_IFLNK))) {
					/* not a regular file or link, skip it */
					continue;
				}

				/* skip .dot files **
				if (!strncmp(p_dirent->d_name, ".", 1)) {
					continue;
				}
				*/

				/* Check if have access to file */
				if (!(stat_struct.st_mode & (PERM_MASK))) {
					/* No one has read permission, skip it */
					/* May want to use access() to check if can read file */
					/*    open will fail later anyway */
					continue;
				}

				/* Check that size is > 0 or mtime was a while ago */
				if (stat_struct.st_size == 0) {
					/* File is zero-length, how long has it been that way? */
					if (stat_struct.st_mtime > time(NULL) - A_FEW_SECONDS) {
						/* give it a few seconds */
						continue;
					}
					/* else pass it on so it can fail and be removed */
				}

				/* Check if file has already been sent */
				if (check_window(p_tbl, pathbuf) != 0) {
					/* file is in the window, don't queue it */
					continue;
				}

				qcnt++;
				if (!(queue = realloc(queue, qcnt*sizeof(prod_info_t)))) {
					CS_LOG_ERR(ERROR_FP,
								"%s: FAIL realloc %d prod_info items, %s\n",
								LOG_PREFIX, qcnt, strerror(errno));
					return -1;
				}
				memset(&queue[qcnt-1], '\0', sizeof(prod_info_t));

				strcpy(queue[qcnt-1].filename, pathbuf);
				queue[qcnt-1].queue_time = stat_struct.st_mtime;
				queue[qcnt-1].size = stat_struct.st_size;
				queue[qcnt-1].priority = priority;

				if (ClientOpt.verbosity > 2) {
					CS_LOG_DBUG(DEBUG_FP,
							"%s: Added item %s, cnt=%d p=%d, t=%ld\n",
							LOG_PREFIX,
							queue[qcnt-1].filename,
							qcnt-1,
							queue[qcnt-1].priority,
							queue[qcnt-1].queue_time);
				}

				if (ClientOpt.max_queue_len > 0
						&& qcnt >= ClientOpt.max_queue_len) {
					/* If we have read more that ClientOpt.max_queue_len
					 * files from the polled directories quit and start
					 * sending them so that we don't spend all our time
					 * polling and no time processing.
					 */
					break;
				}
			}

			if (closedir(p_dir) < 0) {
				CS_LOG_ERR(ERROR_FP, "%s: Fail close directory %s, %s\n", 
							LOG_PREFIX, poll_dir, strerror(errno));
				/* ignore error for now */
			}
			p_dir = NULL;
		}

		/* sort directory entries using quicksort */
		if (qcnt > 1) {
			qsort (queue, qcnt, sizeof(prod_info_t), compare_items);
		}

		polltime = time(NULL);
	}

	/* if there is a next entry */
	if (qcnt - qidx > 0) {

		/* if wait_last_file option is on, check if item is the last one */
		if (!ClientOpt.wait_last_file
				|| queue[qidx].queue_time < queue[qcnt-1].queue_time) {

			if (ClientOpt.verbosity > 1) {
				CS_LOG_DBUG(DEBUG_FP, "%s: Next item is %s, p=%d, t=%s",
						LOG_PREFIX,
						queue[qidx].filename,
						queue[qidx].priority,
						ctime(&queue[qidx].queue_time));
			}

			memcpy(p_prod, &queue[qidx], sizeof(prod_info_t));
			qidx++;
			return qcnt - qidx + 1;
		}
	}

	if (ClientOpt.verbosity > 1) {
		CS_LOG_DBUG(DEBUG_FP, "%s: No items to send\n", LOG_PREFIX);
	}
	return 0;

} /* end get_next_file */

/*******************************************************************************
FUNCTION NAME
	int compare_items(const void *p_v1, const void *p_v2)

FUNCTION DESCRIPTION
	Used as argument to qsort to sort queue items.  Returns value based on
	order of item 1 relative to item 2 so that list can be sorted.

PARAMETERS
	Type			Name			I/O	Description
	const void *	p_v1			I	address of first prod_info item
	const void *	p_v2			I	address of second prod_info item

GLOBAL VARIABLES
	Type			Name			I/O	Description
	none

RETURNS
	 1 if item 2 comes before item 1
	-1 if item 1 comes before item 2
	 0 if the items tie
*******************************************************************************/
int compare_items(const void *p_v1, const void *p_v2)
{
	prod_info_t * p_pi1;
	prod_info_t * p_pi2;

	p_pi1 = (prod_info_t *) p_v1;
	p_pi2 = (prod_info_t *) p_v2;

	if (p_pi1->priority < p_pi2->priority) {
		/* item 2 has higher priority */
		return(1);
	}
	else if (p_pi1->priority > p_pi2->priority) {
		/* item 1 has higher priority */
		return(-1);
	}
	else if (p_pi1->queue_time > p_pi2->queue_time) {
		/* item 2 has been waiting longer */
		return(1);
	}
	else if (p_pi1->queue_time < p_pi2->queue_time) {
		/* item 1 has been waiting longer */
		return(-1);
	}
	else {
		/* tie */
		return(0);
	}
} /* end compare_items */

/*******************************************************************************
FUNCTION NAME
	void finish_send(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Perform processing required after product has been successfully sent
	and acknowledged.
	
	Move the sent product file to a sent directory so that it won't be sent
	again.  The sent directory uses a circular list of sent_count files
	to prevent a full file system.

	Log the successful transmission of the file.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			I	address of prod_info structure

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char			verbosity		I	verbosity level
	int				sent_count		I	number of sent files to rotate

RETURNS
	void
*******************************************************************************/
void finish_send(prod_info_t *p_prod) 
{
	time_t now;
	struct tm *p_tm;
	char timebuf[DATESTR_MAX_LEN];
	char delaybuf[DATESTR_MAX_LEN];
	char sentpath[FILENAME_LEN];
	char log_path[FILENAME_LEN];
	char *p_basename;
	char *p_subdir;
	static unsigned int file_count;
	static unsigned long total_count;
	char junkbuf[BUFSIZ];
	char ccb_info[20];

	if (ClientOpt.verbosity > 2) {
		CS_LOG_DBUG(DEBUG_FP, "%s finishing %s\n", LOG_PREFIX,p_prod->filename);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	sprintf(sentpath, "%s/%.*d",
			ClientOpt.sent_dir,
			sprintf(junkbuf, "%d", ClientOpt.sent_count-1), /* # of digits */
			file_count);

	p_subdir = NULL;
	if ((p_basename = strrchr(p_prod->filename, '/'))) {
		if (p_basename > p_prod->filename) {
			if ((p_subdir = strrchr(p_basename-1, '/'))) {
				p_subdir++;
			}
		}
	}
	if (!p_subdir) {
		p_subdir = p_basename ? p_basename : p_prod->filename;
	}
	strcpy(log_path, p_subdir);
	strcat(log_path, ",");
	if ((p_subdir = strrchr(sentpath, '/'))) {
		p_subdir++;
	} else {
		p_subdir = sentpath;
	}
	strcat(log_path, p_subdir);

	if (my_rename(p_prod->filename, sentpath) < 0) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL rename %s to %s, %s\n",
			LOG_PREFIX, p_prod->filename, sentpath, strerror(errno));
	} else {
		/* update path */
		strcpy (p_prod->filename, sentpath);
	}

	if (now > p_prod->queue_time) {
		/* use send delay for first number, ack delay for second */
		sprintf(delaybuf, " +%ld/%lds",
				p_prod->send_time-p_prod->queue_time, now-p_prod->send_time);
	} else {
		delaybuf[0] = '\0';
	}

	if (p_prod->ccb_len > 0) {
		sprintf(ccb_info, "+%d ccb", p_prod->ccb_len);
	} else {
		ccb_info[0] = '\0';
	}

	/* log status every 100 entries */
	if (!(++total_count % 100)) {
		char hostbuf[HOSTNAME_MAX_LEN];
		gethostname(hostbuf, sizeof(hostbuf));
		CS_LOG_PROD(PRODUCT_FP,
		"STATUS [%s] pid(%d) host(%s) %s-m%d-l%d-h%d to=%s tot(%d) dir(%s%s)\n",
			Program, getpid(), hostbuf,
			ClientOpt.source ? ClientOpt.source : "unknown",
			ClientOpt.shm_region, ClientOpt.link_id, ClientOpt.host_id,
			ClientOpt.host, total_count, ClientOpt.indir_list[0],
			ClientOpt.indir_list[1]?",...":"");
	}

	CS_LOG_PROD(PRODUCT_FP,
		"END %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d%s) f(%s) /P%d%s\n",
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		ccb_info,
		log_path,
		p_prod->priority, delaybuf);

	file_count++;
	file_count %= ClientOpt.sent_count;

	return;
} /* end finish_send */

/*******************************************************************************
FUNCTION NAME
	void abort_send(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Perform any processing required after product has fatally failed.

	Move the failed product file to a failure directory so that it won't be
	sent again.  The abort directory uses a circular list of sent_count files
	to prevent a full file system.

	Log the aborted transmission of the file.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			I	address of prod_info structure

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description
	char *			fail_dir		I	holding directory for failed files
	char			verbosity		I	verbosity level
	int				sent_count		I	number of aborted files to rotate

RETURNS
	void
*******************************************************************************/
void abort_send(prod_info_t *p_prod) 
{
	char failpath[FILENAME_LEN];
	static unsigned int file_count;
	struct tm *p_tm;
	time_t	now;
	char log_path[FILENAME_LEN];
	char *p_basename;
	char *p_subdir;
	char timebuf[DATESTR_MAX_LEN];
	char delaybuf[DATESTR_MAX_LEN];
	char reason[20];
	char ccb_info[20];
	char junkbuf[BUFSIZ];
	int fd;
	int bytes;

	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s discarding %s\n",
				LOG_PREFIX, p_prod->filename);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	sprintf(failpath, "%s/%.*d",
			ClientOpt.fail_dir,
			sprintf(junkbuf, "%d", ClientOpt.sent_count-1), /* # of digits */
			file_count);

	p_subdir = NULL;
	if ((p_basename = strrchr(p_prod->filename, '/'))) {
		p_basename++;
		if ((p_subdir = strrchr(p_basename, '/'))) {
			p_subdir++;
		}
	}
	if (!p_subdir) {
		p_subdir = p_basename ? p_basename : p_prod->filename;
	}
	strcpy(log_path, p_subdir);
	strcat(log_path, ",");
	if ((p_subdir = strrchr(failpath, '/'))) {
		p_subdir++;
	} else {
		p_subdir = failpath;
	}
	strcat(log_path, p_subdir);

	/* move product to failure queue */
	if (my_rename(p_prod->filename, failpath) < 0) {
		CS_LOG_ERR(ERROR_FP,
			"%s: FAIL rename %s to %s, %s\n",
			LOG_PREFIX, p_prod->filename, failpath, strerror(errno));
	} else {
		/* update path */
		strcpy(p_prod->filename, failpath);
	}

	if (now > p_prod->queue_time) {
		/* use send delay for first number, nack delay for second */
		sprintf(delaybuf, " +%ld/%lds",
				p_prod->send_time-p_prod->queue_time, now-p_prod->send_time);
	} else {
		delaybuf[0] = '\0';
	}

	if (p_prod->wmo_ttaaii[0] == '\0') {
		if ((fd = open(p_prod->filename, O_RDONLY)) >= 0) {
			if ((bytes = read(fd, junkbuf, sizeof(junkbuf)-1)) > 0) {
				junkbuf[bytes] = '\0';
				parse_wmo(junkbuf, bytes, p_prod);
			}
			close(fd);
		}
	}

	if (p_prod->state == STATE_NACKED) {
		sprintf(reason, "NACK");
	} else if (p_prod->state == STATE_DEAD) {
		sprintf(reason, "TTL %u SECS", ClientOpt.queue_ttl);
	} else {
		sprintf(reason, "%d ERRS", p_prod->send_count);
	}

	if (p_prod->ccb_len > 0) {
		sprintf(ccb_info, "+%d ccb", p_prod->ccb_len);
	} else {
		ccb_info[0] = '\0';
	}

	CS_LOG_PROD(PRODUCT_FP,
	"ABORT(%s) %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d%s) f(%s) /P%d%s\n",
		reason,
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		ccb_info,
		log_path,
		p_prod->priority, delaybuf);

	file_count++;
	file_count %= ClientOpt.sent_count;

	return;
} /* end abort_send */

/*******************************************************************************
FUNCTION NAME
	void retry_send(prod_info_t *p_prod) 

FUNCTION DESCRIPTION
	Perform any processing required when a product transmission is retried.

	Currently this module just logs the retransmission.

PARAMETERS
	Type			Name			I/O	Description
	prod_info_t *	p_prod			I	address of prod_info structure

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description

RETURNS
	void
*******************************************************************************/
void retry_send(prod_info_t *p_prod) 
{
	struct tm *p_tm;
	time_t	now;
	char *p_basename;
	char *p_subdir;
	char log_path[FILENAME_LEN];
	char timebuf[DATESTR_MAX_LEN];
	char delaybuf[DATESTR_MAX_LEN];
	char ccb_info[20];

	if (ClientOpt.verbosity > 0) {
		CS_LOG_DBUG(DEBUG_FP, "%s retrying %s\n",
				LOG_PREFIX, p_prod->filename);
	}

	time(&now);
	p_tm = localtime(&now);
	strftime(timebuf, sizeof(timebuf), "%m/%d/%Y %T", p_tm);

	p_subdir = NULL;
	if ((p_basename = strrchr(p_prod->filename, '/'))) {
		p_basename++;
		if ((p_subdir = strrchr(p_basename, '/'))) {
			p_subdir++;
		}
	}
	if (!p_subdir) {
		p_subdir = p_basename ? p_basename : p_prod->filename;
	}
	strcpy(log_path, p_subdir);

	if (now > p_prod->queue_time) {
		/* use send delay for first number, nack delay for second */
		sprintf(delaybuf, " +%ld/%lds",
				p_prod->send_time-p_prod->queue_time, now-p_prod->send_time);
	} else {
		delaybuf[0] = '\0';
	}

	if (p_prod->ccb_len > 0) {
		sprintf(ccb_info, "+%d ccb", p_prod->ccb_len);
	} else {
		ccb_info[0] = '\0';
	}

	CS_LOG_PROD(PRODUCT_FP,
		"RETRY[%d] %s WMO[%-6s %-4s %-6s %-3s] {%s} #%d bytes(%d%s) f(%s) /P%d%s\n",
		p_prod->send_count,
		timebuf,
		p_prod->wmo_ttaaii, p_prod->wmo_cccc, p_prod->wmo_ddhhmm,
		p_prod->wmo_bbb,
		p_prod->wmo_nnnxxx,
		p_prod->seqno,
		p_prod->size,
		ccb_info,
		log_path,
		p_prod->priority, delaybuf);

	return;
} /* end retry_send */

/*******************************************************************************
FUNCTION NAME
	int check_window(prod_tbl_t *p_tbl, char *filename)

FUNCTION DESCRIPTION
	Check product table to if filename has been transmitted and is still
	awaiting acknowledgement

PARAMETERS
	Type			Name			I/O	Description
	prod_tbl_t *	p_tbl			O	address of prod table 
	char *			filename		I	filename being checked

GLOBAL VARIABLES (from ClientOpt structure)
	Type			Name			I/O	Description

RETURNS
	1 if found (in progress)
	0 otherwise
*******************************************************************************/
int check_window(prod_tbl_t *p_tbl, char *filename)
{
	prod_info_t *p_prod;

	for (p_prod = p_tbl->ack_list.p_head; p_prod; p_prod = p_prod->p_next) {
		if (!strcmp(p_prod->filename, filename)) {
			return 1;
		}
	}
	for (p_prod = p_tbl->retr_list.p_head; p_prod; p_prod = p_prod->p_next) {
		if (!strcmp(p_prod->filename, filename)) {
			return 1;
		}
	}
	return 0;
}
