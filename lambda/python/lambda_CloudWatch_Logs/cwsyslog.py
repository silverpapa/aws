import boto3
from base64 import b64decode
from json import loads
from zlib import compress, decompress, MAX_WBITS
import logging
import logging.handlers
import socket

def lambda_handler(event, context):
	# Define syslog logger
    my_logger = logging.getLogger('MyLogger')
    my_logger.setLevel(logging.INFO)
    sysloghandler = logging.handlers.SysLogHandler(address = ('52.78.29.157',514),socktype=socket.SOCK_DGRAM)
    my_logger.addHandler(sysloghandler)
    data = event.get('awslogs',{}).get('data')
    if not data:
        return
    records = loads(decompress(b64decode(data),16 + MAX_WBITS))
    messages = []
    for log_event in records['logEvents']:
        messages.append("%s %s" % (log_event['timestamp'], log_event['message']))
        my_logger.info(log_event['message'])	
    print 'Sent %d messages' % (len(messages),)