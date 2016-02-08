/*******************************************************************************
FILE NAME
	log.c - logging library

FILE DESCRIPTION
	log APIs and supporting routines

FUNCTIONS
	write_log	- write message to log file
	flush_log	- flush log stream to log file
	log_prefix	- generate a log prefix string

HISTORY
	Last delta date and time:  08/28/97 19:38:37
	         SCCS identifier:  7.2

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.


*******************************************************************************/
static char Sccsid_log_c[] = "@(#)log.c 0.5 09/30/2003 15:42:59";

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/stat.h>

#include "share.h"

#define LOG_MAX_FILE_SIZE		1024*4096  /* default max log file size */
#define LOG_WRITES_PER_CHECK	50
#define LOG_CHECK_TIME_INTERVAL	30
#define LOG_WRITES_PER_FLUSH	5
#define LOG_FLUSH_TIME_INTERVAL	2

/* print errors from this module to stderr to avoid recursive error loop */
#undef CS_LOG_ERR
#define CS_LOG_ERR				fprintf
#undef ERROR_FP
#define ERROR_FP				stderr

static int check_log_size (logfile_t *p_logfile);
static int rotate_log(char *logfile);
static int open_log (char *logfile, FILE **pp_logstream);
static int close_log (FILE **pp_logstream);
static int log_file_current (FILE *p_logstream, char *logfile);
static void lock_log(char *logfile);
static void unlock_log(char *logfile);
static int check_day_change (logfile_t *p_logfile);
static int archive_log(logfile_t *p_logfile);
static void new_log(logfile_t *p_logfile);
static void init_log(logfile_t *p_logfile);

/*******************************************************************************

FUNCTION NAME
	write_log(logfile_t *p_logfile, const char *format, ...)

FUNCTION DESCRIPTION
	Write log buffer to log file

PARAMETERS
	Type		Name		I/O		Description
	logfile_t *	p_logfile	I		Log file structure
	const char *format, ...	I		printf-style format string and args

RETURNS
	 0: successful log write
	-1: otherwise

ERRORS REPORTED

*******************************************************************************/

int write_log(logfile_t *p_logfile, const char *format, ...)
{
	static int init;
	va_list			ap;
	
	if (!init) {
		init_log(p_logfile);
	}

	if (!p_logfile->p_stream 
			&& open_log(p_logfile->path, &p_logfile->p_stream) < 0) {
		return -1;
	}

	/* check for day change and roll over if necessary */
	if (check_day_change(p_logfile) > 0) {
		new_log(p_logfile);
	}

	/* check log file size and roll over if necessary */
	if (p_logfile->writes_since_last_check > p_logfile->writes_per_check
			|| time(NULL) - p_logfile->last_check_time
				> p_logfile->check_time_interval) {

		if (check_log_size(p_logfile) > 0) {
			new_log(p_logfile);
		}
	}

	va_start(ap, format);
	if (vfprintf(p_logfile->p_stream, format, ap) < 0) {
		va_end(ap);
		CS_LOG_ERR(ERROR_FP,
			"%s: Failed vfprintf to log file %s, %s\n",
			LOG_PREFIX, p_logfile->path, strerror(errno));
		close_log(&p_logfile->p_stream);
		return -1;
	}
	va_end(ap);

	if (p_logfile->flags & LOG_STDERR_FLAG) {
		va_start(ap, format);
		if (vfprintf(stderr, format, ap) < 0) {
			va_end(ap);
			CS_LOG_ERR(ERROR_FP,
				"%s: Failed vfprintf to stderr, %s\n",
				LOG_PREFIX, strerror(errno));
			return -1;
		}
		va_end(ap);
	}        

	if (p_logfile->flags & LOG_STDOUT_FLAG) {
		va_start(ap, format);
		if (vfprintf(stdout, format, ap) < 0) {
			va_end(ap);
			CS_LOG_ERR(ERROR_FP,
				"%s: Failed vfprintf to stdout, %s\n",
				LOG_PREFIX, strerror(errno));
			return -1;
		}
		va_end(ap);
	}        

	p_logfile->last_write_time = time(NULL);
	p_logfile->writes_since_last_check++;
	p_logfile->writes_since_last_flush++;

	if (p_logfile->p_stream &&
			(p_logfile->writes_since_last_flush > p_logfile->writes_per_flush
				|| time(NULL) - p_logfile->last_flush_time
					> p_logfile->flush_time_interval)) {
		if (fflush(p_logfile->p_stream) < 0) {
			CS_LOG_ERR(ERROR_FP,
					"%s: Failed fflush on stream to log file %s, %s\n",
					LOG_PREFIX, p_logfile->path, strerror(errno));
		} else {
			p_logfile->last_flush_time = time(NULL);
			p_logfile->writes_since_last_flush = 0;
		}
	}

	return 0;
}


/*******************************************************************************

FUNCTION NAME
	init_log(logfile_t *p_logfile)

FUNCTION DESCRIPTION
	Initialize log file structure with default values

PARAMETERS
	Type		Name		I/O		Description
	logfile_t *	p_logfile	I		Log file structure

RETURNS
	void

ERRORS REPORTED

*******************************************************************************/

static void init_log(logfile_t *p_logfile)
{
	char *			p_env;

	/* Check if want to override the default log size */
	if (p_logfile->maxsize == 0) {
		/* Check on maximum log size */
		if ((p_env = getenv("LOG_MAX_FILE_SIZE")) != (char *)0  ) {  
			p_logfile->maxsize = atoi((const char *)p_env);
		}                         
		if (p_logfile->maxsize == 0) {
			p_logfile->maxsize = LOG_MAX_FILE_SIZE;        
		}
	}

	/* Check for archiving */
	if (!(p_logfile->flags & (LOG_ROTATE_FLAG|LOG_ARCHIVE_FLAG))) {
		p_logfile->flags |= LOG_ROTATE_FLAG;
		p_env = getenv("LOG_RETENTION");        
		if (p_env  && !strncmp(p_env, "archive", strlen("archive"))) {
			p_logfile->flags |= LOG_ARCHIVE_FLAG;        
		}                                                           
	}

	/* Check on location of log files */
	if (p_logfile->dir[0] == '\0') {
		p_env = getenv("LOG_DIR_PATH");
		if (p_env && *p_env) {  
			strcpy(p_logfile->dir, p_env);
		} else {
			strcpy(p_logfile->dir, LOG_DIR_PATH);
		}
	}

	if (p_logfile->name[0] == '\0') {
		sprintf(p_logfile->name, "%s.log", Program);
	}

	if (p_logfile->path[0] == '\0') {
		sprintf(p_logfile->path, "%s/%s", p_logfile->dir, p_logfile->name);
	}

	/* writes per size check */
	if (p_logfile->writes_per_check == 0) {
		p_logfile->writes_per_check = LOG_WRITES_PER_CHECK;
	}

	/* time per size check */
	if (p_logfile->check_time_interval == 0) {
		p_logfile->check_time_interval = LOG_CHECK_TIME_INTERVAL;
	}

	/* writes per flush */
	if (p_logfile->writes_per_flush == 0) {
		p_logfile->writes_per_flush = LOG_WRITES_PER_FLUSH;
	}

	/* time interval per flush */
	if (p_logfile->flush_time_interval == 0) {
		if ((p_env = getenv("LOG_FLUSH_TIME_INTERVAL"))) {
			p_logfile->flush_time_interval = atoi(p_env);
			if (p_logfile->flush_time_interval == 0) {
				p_logfile->flush_time_interval = -1; /* disable */
			}
		} else {
			p_logfile->flush_time_interval = LOG_FLUSH_TIME_INTERVAL;
		}

		p_logfile->flush_time_interval = LOG_FLUSH_TIME_INTERVAL;
	}
}

/*******************************************************************************

FUNCTION NAME
	flush_log(logfile_t *p_logfile)

FUNCTION DESCRIPTION
	Flush log stream to log file on disk

PARAMETERS
	Type		Name		I/O		Description
	logfile_t *	p_logfile	I		Log file structure

RETURNS
	 0: successful log flush
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/

int flush_log(logfile_t *p_logfile)
{

	if (!p_logfile->p_stream) {
		return -1;
	}

	if (fflush(p_logfile->p_stream) < 0) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed fflush on stream to log file %s, %s\n",
				LOG_PREFIX, p_logfile->path, strerror(errno));
		return -1;
	} else {
		p_logfile->last_flush_time = time(NULL);
		p_logfile->writes_since_last_flush = 0;
		return 0;
	}
}

/*******************************************************************************

FUNCTION NAME
	rename_log(logfile_t *p_logfile, char *newname)

FUNCTION DESCRIPTION
	Rename log file to newname

PARAMETERS
	Type		Name		I/O		Description
	logfile_t *	p_logfile	I		Log file structure

RETURNS
	 0: successful log rename
	-1: otherwise

ERRORS REPORTED

*******************************************************************************/

int rename_log(logfile_t *p_logfile, char *newname)
{
	char *p_env;

	/* make sure logfile dir has been set */
	if (p_logfile->dir[0] == '\0') {
		p_env = getenv("LOG_DIR_PATH");
		if (p_env && *p_env) {  
			strcpy(p_logfile->dir, p_env);
		} else {
			strcpy(p_logfile->dir, LOG_DIR_PATH);
		}
	}

	sprintf(p_logfile->name, "%s.log", newname);

	sprintf(p_logfile->path, "%s/%s", p_logfile->dir, p_logfile->name);

	if (p_logfile->p_stream) {
		close_log(&p_logfile->p_stream);
	}

	if (open_log(p_logfile->path, &p_logfile->p_stream) < 0) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed open log file %s, %s\n",
				LOG_PREFIX, p_logfile->path, strerror(errno));
		return -1;
	}

	return 0;
}

/*******************************************************************************

FUNCTION NAME
	check_log_size

FUNCTION DESCRIPTION
	Check log size 

PARAMETERS
	Type	Name			I/O		Description
	logfile_t *	p_logfile	I		log file structure

RETURNS
	 1: log is greater than nominal log size
	 0: log was NOT greater than nominal log size
	-1: otherwise

ERRORS REPORTED
	

*******************************************************************************/
static int check_log_size (logfile_t *p_logfile)
{
	long		log_length;

	if ((log_length = ftell(p_logfile->p_stream)) < 0) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed ftell on log file %s, %s\n",
				LOG_PREFIX, p_logfile->path, strerror(errno));
		return -1;
	}

	p_logfile->last_check_time = time(NULL);
	p_logfile->writes_since_last_check = 0;

	if (log_length > p_logfile->maxsize) {
		return 1;
	} else {
		return 0;
	}
}

/*******************************************************************************

FUNCTION NAME
	archive_log(logfile_t *p_logfile)

FUNCTION DESCRIPTION
	Move a log file to the archive directory

PARAMETERS
	Type		Name		I/O		Description
	logfile_t *	p_logfile	I		Logfile structure

RETURNS
	 0: successful archive
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/
static int archive_log(logfile_t *p_logfile)
{
	struct tm  *ltm;
	char *		p_dot;
	char *		p_filename;
	char 		old_filename[FILENAME_LEN];
	char 		archive_filename[FILENAME_LEN];
	size_t		buflen;

	/* Assume need to use ARCHIVE directory */
	strcpy(archive_filename, p_logfile->dir);
	strcat(archive_filename, "/ARCHIVE");
	ltm = localtime(&p_logfile->last_write_time); 
	buflen = sizeof(archive_filename) - strlen(archive_filename) - 1;
	strftime(archive_filename+strlen(archive_filename), buflen, "/%b%d", ltm);
	strcpy(old_filename, p_logfile->path);
	if ((p_filename = strrchr(old_filename, '/'))) {
		++p_filename;
		/* p_filename points to basename of the logfile */
	} else {
		strcpy(old_filename, "unknown");
		p_filename = old_filename;
	}
	if ((p_dot = strrchr(p_filename, '.'))) {
		/* Add the timestamp at the end of the filename */
		buflen = old_filename + sizeof(old_filename) - p_dot - 1;
		strftime(p_dot, buflen, ".%H.%M.%S", ltm);
	} else {
		buflen = old_filename + sizeof(old_filename)
				 - p_filename - strlen(p_filename) - 1;
		strftime(p_filename+strlen(p_filename),  buflen, ".%H.%M.%S", ltm);
	}
	strcat(archive_filename, "/");
	strcat(archive_filename, p_filename);

	if (my_rename(p_logfile->path, archive_filename) == -1) {
		CS_LOG_ERR(ERROR_FP,
			"%s: Failed to move log file from %s to %s, %s\n",
			LOG_PREFIX, p_logfile->path, archive_filename, strerror(errno));
		return -1;
	}

	return 0;
}

/*******************************************************************************

FUNCTION NAME
	rotate_log(char *filename)

FUNCTION DESCRIPTION
	Rotate log file to .old 

PARAMETERS
	Type	Name		I/O		Description
	char *	filename	I		filename to rotate

RETURNS
	 0: successful log rotation
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/
static int rotate_log(char *filename)
{
	char *		p_dot;
	char 		old_filename[FILENAME_LEN];

	/* Need to move to .old file as requested */
	strcpy(old_filename, filename);
	if ((p_dot = strrchr(old_filename, '.'))) {
		strcpy(++p_dot, "old");
	} else {
		strcat(old_filename, ".old");
	}
	if (my_rename(filename, old_filename) == -1) {
		CS_LOG_ERR(ERROR_FP,
			"%s: Failed to move log file from %s to %s, %s\n",
			LOG_PREFIX, filename, old_filename, strerror(errno));
		return -1;
	}

	return 0;
}

/*******************************************************************************

FUNCTION NAME
	open_log(char *logfile, FILE **pp_logstream)

FUNCTION DESCRIPTION
	Open log file

PARAMETERS
	Type	Name			I/O		Description
	char *	logfile			I		filename to open
	FILE **	pp_logstream	O		Opened stream

RETURNS
	 0: successful log file open
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/
static int open_log (char *logfile, FILE **pp_logstream)
{
	char *p_slash;

	if (!(*pp_logstream = fopen(logfile, "a"))) {
		if (errno == ENOENT) {
			if ((p_slash = strrchr(logfile, '/'))) {
				*p_slash = '\0';
				if (my_mkdir(logfile) < 0) {
					CS_LOG_ERR(ERROR_FP,
							"%s: Failed make log directory %s, %s\n",
							LOG_PREFIX, logfile, strerror(errno));
					*p_slash = '/';
					return -1;
				}
				*p_slash = '/';
			}
			/* mkdir succeeded, try again */
			if (!(*pp_logstream = fopen(logfile, "a"))) {
				CS_LOG_ERR(ERROR_FP,
						"%s: Failed open log file %s, %s\n",
						LOG_PREFIX, logfile, strerror(errno));
				return -1;
			}
		} else {
			CS_LOG_ERR(ERROR_FP,
					"%s: Failed open log file %s, %s\n",
					LOG_PREFIX, logfile, strerror(errno));
			return -1;
		}
	}

	return 0;
}

/*******************************************************************************

FUNCTION NAME
	close_log(FILE **pp_logstream)

FUNCTION DESCRIPTION
	Close log data stream

PARAMETERS
	Type	Name			I/O		Description
	char **	pp_logstream	I/O		I/O stream to close

RETURNS
	 0: successful close
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/
static int close_log (FILE **pp_logstream)
{
	int rc;

	rc = fclose(*pp_logstream);
	*pp_logstream = NULL;

	return rc;
}

/*******************************************************************************

FUNCTION NAME
	log_file_current(FILE *p_logstream, char *logfile)

FUNCTION DESCRIPTION
	Check if logstream is writing to the current logfile

PARAMETERS
	Type	Name		I/O		Description
	FILE *	p_logstream	I		log data stream
	char *	logfile		I		log file

RETURNS
	 0: log stream is not current
	 1: log stream is current
	-1: otherwise

ERRORS REPORTED


*******************************************************************************/
static int log_file_current (FILE *p_logstream, char *logfile)
{
	struct stat stream_stat;
	struct stat path_stat;
	int log_fd;

	if ((log_fd = fileno(p_logstream)) < 0) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed fileno for log stream, %s\n",
				LOG_PREFIX, strerror(errno));
		return -1;
	}

	if (fstat(log_fd, &stream_stat)) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed stat for log stream fd %d, %s\n",
				LOG_PREFIX, log_fd, strerror(errno));
		return -1;
	}

	if (stat(logfile, &path_stat)) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed stat for log path %s, %s\n",
				LOG_PREFIX, logfile, strerror(errno));
		return -1;
	}

	if (stream_stat.st_ino == path_stat.st_ino) {
		return 1;
	}

	return 0;
}

/*******************************************************************************

FUNCTION NAME
	lock_log(char *logfile)

FUNCTION DESCRIPTION
	Lock a log file (for rollover/archive)

PARAMETERS
	Type	Name		I/O		Description
	char *	logfile		I		file to lock

RETURNS
	void

ERRORS REPORTED


*******************************************************************************/
static void lock_log(char *logfile)
{
	char *p_suffix;
	char lockfile[FILENAME_LEN];
	int retries = 0;
	int lockfd;

	strcpy(lockfile, logfile);
	if ((p_suffix = strstr(lockfile, ".log"))) {
		strcpy(p_suffix, ".lck");
	} else {
		strcat(lockfile, ".lck");
	}

	while ((lockfd = open(lockfile, O_WRONLY|O_CREAT|O_EXCL, 600)) < 0
			&& retries++ < 3) {
		if (errno != EEXIST) {
			CS_LOG_ERR(ERROR_FP,
					"%s: Failed open log lockfile %s, %s\n",
					LOG_PREFIX, lockfile, strerror(errno));
		}
		sleep (1);
	}

	if (lockfd >= 0) {
		close (lockfd);
	}

	return;
}

/*******************************************************************************

FUNCTION NAME
	unlock_log(char *logfile)

FUNCTION DESCRIPTION
	Unlock a log file (for rollover/archive)

PARAMETERS
	Type	Name		I/O		Description
	char *	logfile		I		file to unlock

RETURNS
	void

ERRORS REPORTED


*******************************************************************************/
static void unlock_log(char *logfile)
{
	char *p_suffix;
	char lockfile[FILENAME_LEN];

	strcpy(lockfile, logfile);
	if ((p_suffix = strstr(lockfile, ".log"))) {
		strcpy(p_suffix, ".lck");
	} else {
		strcat(lockfile, ".lck");
	}

	unlink(lockfile);

	return;
}

/*******************************************************************************

FUNCTION NAME
	check_day_change

FUNCTION DESCRIPTION
	Check day change 

PARAMETERS
	Type	Name			I/O		Description
	logfile_t *	p_logfile	I		log file structure

RETURNS
	 1: log day has changed
	 0: log day has NOT changed
	-1: otherwise

ERRORS REPORTED
	

*******************************************************************************/
static int check_day_change (logfile_t *p_logfile)
{
	struct stat stat_buf;
	struct tm *	p_tm;
	struct tm	ltm;
	struct tm	ftm;
	time_t		now;

	if (p_logfile->last_write_time == 0) {
		if (stat(p_logfile->path, &stat_buf)) {
			CS_LOG_ERR(ERROR_FP,
					"%s: Failed stat on log file %s, %s\n",
					LOG_PREFIX, p_logfile->path, strerror(errno));
			return -1;
		}

		p_logfile->last_write_time = stat_buf.st_mtime;
	}

	time(&now);
	p_tm = localtime(&now);
	memcpy (&ltm, p_tm, sizeof(ltm));
	p_tm = localtime(&p_logfile->last_write_time);
	memcpy (&ftm, p_tm, sizeof(ftm));
	if (ltm.tm_yday != ftm.tm_yday) {
		return 1;
	}
	return 0;
}

/*******************************************************************************

FUNCTION NAME
	new_log

FUNCTION DESCRIPTION
	1. If and only if log file is current rotate/archive it
	2. Close the old log file
	3. Open/create a new log file

PARAMETERS
	Type	Name			I/O		Description
	logfile_t *	p_logfile	I		log file structure

RETURNS
	void

ERRORS REPORTED
	
*******************************************************************************/

static void new_log(logfile_t *p_logfile) 
{
	lock_log(p_logfile->path);

	if (log_file_current(p_logfile->p_stream, p_logfile->path) == 1) {
		if (p_logfile->flags & LOG_ARCHIVE_FLAG) {
			archive_log(p_logfile);
		} else {
			rotate_log(p_logfile->path);
		}
	}

	close_log(&p_logfile->p_stream);

	if (open_log(p_logfile->path, &p_logfile->p_stream) < 0) {
		CS_LOG_ERR(ERROR_FP,
				"%s: Failed open log file %s, %s\n",
				LOG_PREFIX, p_logfile->path, strerror(errno));
	}

	unlock_log(p_logfile->path);

	/* implied check on rollover */
	p_logfile->last_check_time = time(NULL);
	p_logfile->writes_since_last_check = 0;

	/* do not want to imply flush on rollover! */

	return;
}

/*******************************************************************************

FUNCTION NAME
	log_prefix

FUNCTION DESCRIPTION
	Generate the log prefix message based on program name, file, and line #.

PARAMETERS
	Type	Name			I/O		Description
	char *	program			I		program
	char *	file			I		source file
	int		line			I		source line #

RETURNS
	log prefix string

ERRORS REPORTED
	
*******************************************************************************/
char *log_prefix(char *program, char *file, int line)
{
	static char prefix_buf[512];
	time_t now;
	struct tm *p_tm;
	char *p_buf;

	sprintf (prefix_buf, "%s ", program);
	p_buf = prefix_buf + strlen(prefix_buf);

	time(&now);
	p_tm = localtime(&now);
	strftime(p_buf, sizeof(prefix_buf) - (p_buf - prefix_buf), "%m/%d/%Y %T", p_tm);
	p_buf = p_buf + strlen(p_buf);

	sprintf (p_buf, " %s:%d", file, line);

	return prefix_buf;
}

