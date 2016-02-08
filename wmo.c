/*******************************************************************************
FILE NAME
	wmo.c

FILE DESCRIPTION
	WMO header parsing routines (parse_wmo)

FUNCTIONS
	parse_wmo	- parse a WMO heading from a buffer

HISTORY
	Last delta date and time:  %G% %U%
	         SCCS identifier:  %I%

NOTICE
		This computer software has been developed at
		Government expense under NOAA
		Contract 50-SPNA-3-00001.

*******************************************************************************/
static char Sccsid_wmo_c[]= "@(#)wmo.c 0.6 05/11/2004 12:56:027";

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include "share.h"

#define WMO_T1	0
#define WMO_T2	1
#define WMO_A1	2
#define WMO_A2	3
#define WMO_I1	4
#define WMO_I2	5

/*******************************************************************************
FUNCTION NAME
	int parse_wmo(char *buf, size_t buflen, prod_info_t *p_prod)

FUNCTION DESCRIPTION
	Parse the wmo heading from buffer and load the appropriate prod
	info fields.  The following regular expressions will satisfy this
	parser.  Note this parser is not case sensative.

	The WMO format is supposed to be...

	TTAAii CCCC DDHHMM[ BBB]\r\r\n
	[NNNXXX\r\r\n]
	
	This parser is generous with the ii portion of the WMO and all spaces
	are optional.  The TTAAII, CCCC, and DDHHMM portions of the WMO are
	required followed by at least 1 <cr> or <lf> with no other unparsed 
	intervening characters. The following quasi-grammar describe what
	is matched.

	WMO = "TTAAII CCCC DDHHMM [BBB] CRCRLF [NNNXXX CRCRLF]"

	TTAAII = "[A-Z]{4}[0-9]{0,1,2}" | "[A-Z]{4} [0-9]" | "[A-Z]{3}[0-9]{3} "
	CCCC = "[A-Z]{4}"
	DDHHMM = "[ 0-9][0-9]{3,5}"
	BBB = "[A-Z0-9]{0-3}"
	CRCRLF = "[\r\n]+"
	NNNXXX = "[A-Z0-9]{0,4-6}"

	Most of the WMO's that fail to be parsed seem to be missing the ii
	altogether or missing part or all of the timestamp (DDHHMM)

PARAMETERS
	Type			Name		I/O		Description
	char *			buf			I		buffer to parse for WMO
	size_t			buflen		I		length of data in buffer
	prod_info_t *	p_prod		O		prod info for the header

RETURNS
	 0 if successful
	-1: otherwise
*******************************************************************************/
int parse_wmo(char *buf, size_t buflen, prod_info_t *p_prod)
{
	char *p_wmo;
	int i_bbb;
	int i_nnnxxx;
	int i_crlf;
	int spaces;

	p_prod->wmo_ttaaii[0] = '\0';
	p_prod->wmo_cccc[0] = '\0';
	p_prod->wmo_ddhhmm[0] = '\0';
	p_prod->wmo_bbb[0] = '\0';
	p_prod->wmo_nnnxxx[0] = '\0';

	for (p_wmo = buf; p_wmo + WMO_I2 + 1 < buf + buflen; p_wmo++) {
		if (isalpha(p_wmo[WMO_T1]) && isalpha(p_wmo[WMO_T2])
				&& isalpha(p_wmo[WMO_A1]) && isalpha(p_wmo[WMO_A2])) {
			/* 'TTAAII ' */
			if (isdigit(p_wmo[WMO_I1]) && isdigit(p_wmo[WMO_I2])
					&& (isspace(p_wmo[WMO_I2+1]) || isalpha(p_wmo[WMO_I2+1]))) {
				sprintf (p_prod->wmo_ttaaii,
							"%.*s", WMO_I2+1, p_wmo);
				p_wmo += WMO_I2 + 1;
				break;
			/* 'TTAAI C' */
			} else if (isdigit(p_wmo[WMO_I1]) && isspace(p_wmo[WMO_I2])
					&& (isspace(p_wmo[WMO_I2+1]) || isalpha(p_wmo[WMO_I2+1]))) {
				sprintf (p_prod->wmo_ttaaii,
							"%.*s0%c", WMO_A2+1, p_wmo, p_wmo[WMO_I1]);
				p_wmo += WMO_I1 + 1;
				break;
			/* 'TTAA I ' */
			} else if (isspace(p_wmo[WMO_I1]) && isdigit(p_wmo[WMO_I2])
					&& (isspace(p_wmo[WMO_I2+1]) || isalpha(p_wmo[WMO_I2+1]))) {
				sprintf (p_prod->wmo_ttaaii,
							"%.*s0%c", WMO_A2+1, p_wmo, p_wmo[WMO_I2]);
				p_wmo += WMO_I2 + 1;
				break;
			/* 'TTAAIC' */
			} else if (isdigit(p_wmo[WMO_I1]) && isalpha(p_wmo[WMO_I2])) {
				sprintf (p_prod->wmo_ttaaii,
							"%.*s0%c", WMO_A2+1, p_wmo, p_wmo[WMO_I1]);
				p_wmo += WMO_I1 + 1;
				break;
			}
		} else if (isalpha(p_wmo[WMO_T1]) && isalpha(p_wmo[WMO_T2])
				&& isalpha(p_wmo[WMO_A1]) && isdigit(p_wmo[WMO_A2])) {
			/* 'TTA#II ' */
			if (isdigit(p_wmo[WMO_I1]) && isdigit(p_wmo[WMO_I2])
					&& (isspace(p_wmo[WMO_I2+1]) || isalpha(p_wmo[WMO_I2+1]))) {
				sprintf (p_prod->wmo_ttaaii,
							"%.*s", WMO_I2+1, p_wmo);
				p_wmo += WMO_I2 + 1;
				break;
			}
		} else if (!strncmp(p_wmo, "\r\r\n", 3)) {
			/* got to EOH with no TTAAII found, check TTAA case below */
			break;
		}
	}

	if (!p_prod->wmo_ttaaii[0]) {
		/* look for TTAA CCCC DDHHMM */
		for (p_wmo = buf; p_wmo + 9 < buf + buflen; p_wmo++) {
			if (isalpha(p_wmo[WMO_T1]) && isalpha(p_wmo[WMO_T2])
					&& isalpha(p_wmo[WMO_A1]) && isalpha(p_wmo[WMO_A2])
					&& isspace(p_wmo[WMO_A2+1]) && isalpha(p_wmo[WMO_A2+2])
					&& isalpha(p_wmo[WMO_A2+3]) && isalpha(p_wmo[WMO_A2+4])
					&& isalpha(p_wmo[WMO_A2+5]) && isspace(p_wmo[WMO_A2+6])) {
				sprintf (p_prod->wmo_ttaaii,
						"%.*s00", WMO_A2+1, p_wmo);
				p_wmo += WMO_A2 + 1;
				break;
			} else if (!strncmp(p_wmo, "\r\r\n", 3)) {
				/* got to EOH with no TTAA found, give up */
				return -1;
			}
		}
	}

	/* skip spaces if present */
	while (isspace(*p_wmo) && p_wmo < buf + buflen) {
		p_wmo++;
	}

	if (p_wmo + WMO_CCCC_LEN > buf + buflen) {
		return -1;
	} else if (isalpha(*p_wmo) && isalnum(*(p_wmo+1))
			&& isalpha(*(p_wmo+2)) && isalnum(*(p_wmo+3))) {
		sprintf (p_prod->wmo_cccc, "%.*s", WMO_CCCC_LEN, p_wmo);
		p_wmo += WMO_CCCC_LEN;
	} else {
		return -1;
	}

	/* skip spaces if present */
	spaces = 0;
	while (isspace(*p_wmo) && p_wmo < buf + buflen) {
		p_wmo++;
		spaces++;
	}

	/* case1: check for 6 digit date-time group */
	if (p_wmo + 6 <= buf + buflen) {
		if (isdigit(*p_wmo) && isdigit(*(p_wmo+1))
				&& isdigit(*(p_wmo+2)) && isdigit(*(p_wmo+3))
				&& isdigit(*(p_wmo+4)) && isdigit(*(p_wmo+5))) {
			sprintf (p_prod->wmo_ddhhmm, "%.*s", 6, p_wmo);
			p_wmo += 6;
		}
	}

	/* case2: check for 4 digit date-time group */
	if (!p_prod->wmo_ddhhmm[0] && p_wmo + 5 <= buf + buflen) {
		if (isdigit(*p_wmo) && isdigit(*(p_wmo+1))
				&& isdigit(*(p_wmo+2)) && isdigit(*(p_wmo+3))
				&& isspace(*(p_wmo+4))) {
			sprintf (p_prod->wmo_ddhhmm, "%.*s00", 4, p_wmo);
			p_wmo += 4;
		}
	}

	/* case3: check for leading 0 in date-time group being a space */
	if (!p_prod->wmo_ddhhmm[0] && p_wmo + 5 <= buf + buflen) {
		if (spaces > 1 && isdigit(*p_wmo) && isdigit(*(p_wmo+1))
				&& isdigit(*(p_wmo+2)) && isdigit(*(p_wmo+3))
				&& isdigit(*(p_wmo+4))) {
			sprintf (p_prod->wmo_ddhhmm, "0%.*s", 5, p_wmo);
			p_wmo += 5;
		}
	}

	/* strip off trailing 'Z' on dddhhmm */
	if (*p_wmo == 'Z') {
		p_wmo++;
	}

	/* make sure we have a <cr> and/or <lf>, parse bbb if present */
	while (p_wmo < buf + buflen) {
		if ((*p_wmo == '\r') || (*p_wmo == '\n')) {
			/* assume this is our cr-cr-lf */
			break;
		} else if (isalpha(*p_wmo)) {
			if (p_prod->wmo_bbb[0]) {
				/* already have a bbb */
				return -1;
			}
			for (i_bbb = 1;
					p_wmo + i_bbb < buf + buflen && i_bbb < WMO_BBB_LEN;
						i_bbb++) {
				if (!isalpha(p_wmo[i_bbb])) {
					break;
				} 
			}
			if (p_wmo + i_bbb < buf + buflen && isspace(p_wmo[i_bbb])) {
				sprintf (p_prod->wmo_bbb, "%.*s", i_bbb, p_wmo);
				p_wmo += i_bbb;
			} else {
				/* bbb is too long or maybe not a bbb at all */
				return -1;
			}
		} else if (isspace(*p_wmo)) {
			p_wmo++;
		} else {
			return -1;
		}
	}

	/* skip cr's, lf's, and spaces */
	while (p_wmo < buf + buflen && isspace(*p_wmo)) {
		p_wmo++;
	}

	for (i_nnnxxx = 1;
			(p_wmo + i_nnnxxx < buf + buflen) && (i_nnnxxx < WMO_NNNXXX_LEN);
				i_nnnxxx++) {
		if (!isalnum(p_wmo[i_nnnxxx])) {
			break;
		} 
	}

	/* check NNNXXX for minimum length */
	if (i_nnnxxx >= WMO_NNNXXX_MIN_LEN) {
		/* The NNNXXX must be on its own line */
		for (i_crlf = i_nnnxxx; p_wmo + i_crlf < buf + buflen; i_crlf++) {
			if ((p_wmo[i_crlf] == '\r') || (p_wmo[i_crlf] == '\n')) {
				sprintf (p_prod->wmo_nnnxxx, "%.*s", i_nnnxxx, p_wmo);
				break;
			}
			if (!isspace(p_wmo[i_crlf])) {
				break;
			}
		}
	}

	return 0;
}

char *debug_buf(char *buf, size_t buflen)
{
	static char outbuf[100];
	int i;

	if (buflen > sizeof(outbuf)) {
		/* silently ignore request for longer buffer */
		buflen = sizeof(outbuf) - 1;
	}

	for (i = 0; i < buflen; i++) {
		if (isprint(buf[i])) {
			outbuf[i] = buf[i];
		} else if (buf[i] == '\r' || buf[i] == '\n') {
			outbuf[i] = '*';
		} else {
			outbuf[i] = '?';
		}
	}

	return outbuf;
}

