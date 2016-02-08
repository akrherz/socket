SCCS_ID_README="@(#)README 0.1 06/03/2003 12:00:00"

PROTOCOL
    This client/server uses a simple TCP socket protocol.
    CLIENT                      SERVER
    ----                        Listen to a socket
    Connect to the socket       ----
    ----                        Accept connection
    Write header to socket      ----
    ----                        Read header from socket
    Write data to socket        ----
    ----                        Read data from socket
    ----                        Write ack to socket
    Read ack from socket        ----

    The client uses an acknowledgement window to limit throughput losses due
    to protocol turn-around-time.  The server does not need to know or care
    about this.


MESSAGE FORMATS
    This message format is based on the WMO, but includes a timestamp field
    following the sequence number and does not bother with the ETX trailer.

    Numbers are represented as ascii strings to eliminate byte ordering
    and alignment issues.

    BYTES   DESCRIPTION
    0-7     message length (beginning from SOH character)
    8-9     message type (AN for ascii, BI for binary, or FX for facsimile)
    10      SOH (\001)
    11-13   <cr><cr><lf>
    14-18   5 digit sequence number
    19-28   10 digit timestamp (as returned from time(2) syscall)
    29-31   <cr><cr><lf>
    32-     product data

    Acknowledgements use the format below.  The ack code is used to tell 
    the client whether the product was processed successfully and request
    retransmissions.  The FAIL code will not generate a retransmission, only
    the RETRANSMIT code generates retransmissions.

    BYTES   DESCRIPTION
    0-4     5 digit sequence number
    5       ack code (K for OK, F for FAIL, R for RETRANSMIT)


FILES
    client.h        - client header file
    client_main.c   - main routine, arg processing, signal handlers, etc.
    client_queue.c  - get path to next file, finish, and abort routines
    client_send.c   - send products and receive acks

    serv.h          - server header file
    serv_dispatch.c - dispatch and manage workers for each connection
    serv_main.c     - main routine, arg processing, signal handlers, etc.
    serv_recv.c     - receive products and send acks
    serv_store.c    - get path for next file, finish, and abort routines

    share.h         - shared header file
    share.c         - message formatting and parsing and utility functions
    log.c           - logging routines
    wmo.c           - wmo parsing routines


CUSTOMIZATION
    The modules in client_queue.c and serv_store.c may be modified to allow
    interfaces to databases or other client/servers. 

    get_next_file   - get path, size, and mtime of next product to send
    finish_send     - client post-processing for file sent successfully 
    abort_send      - client post-processing for file that failed

    get_out_path    - get path for next received product
    finish_recv     - server post-processing for file received successfully
    abort_recv      - server post-processing for file that failed

    Currently get_next_file scans a list of directories for files to send. 
    If it finds a file that looks like it is ready to send and returns the
	relevant file info.  This module could be modified to interface to a
	database instead.  E.g. perform a database query for items to send.  If
	an item is found, select the path to the data and other relevant fields
	from that item.  Then return the path, size, and mtime of the product.

    Finish_send and abort_send are currently used to log success or failure
    and move items out of the input directory.  If items are being polled
	from a database in get_next_file, the database entry should be updated 
	or deleted here.

    Get_out_path generates the filename for received product.  The WMO
    Heading, sequence number, priority, size, queue time, and send times
    are available in the prod_info_t input argument to facilitate 
    file name generation.  The RemoteHost global is set to the hostname
    of the client process and the WorkerIndex global is set to the unique 
    index of this service.

    Finish_recv and abort_recv are currently used to log success or failure
    and remove aborted items from the output directory since they are likely
    incomplete or otherwise errant.  Finish_recv could be used to perform 
    post-processing such as inserting the product into a database, or sending
    a product arrival notification to another process.

