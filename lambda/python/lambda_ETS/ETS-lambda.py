# -*- coding: utf-8 -*-
# import hashlib
import json
import boto3

# This is the ID of the Elastic Transcoder pipeline that was created when
# setting up your AWS environment:
# http://docs.aws.amazon.com/elastictranscoder/latest/developerguide/sample-code.html#python-pipeline

# pipeline_id = 'Enter your pipeline id here.'
pipeline_id = '<pipeline_id>'

# This is the name of the input key that you would like to transcode.
# input_key = 'Enter your input key here.' input key will be received by the lambda_handler

# Region where the sample will be run
# region = 'us-east-1'

def lambda_handler(event, context):
    
    print ("Processing lambda handler")

    try:
        if (event!=None and event.has_key('Records') and 
            len(event.get('Records')) == 1 and 
            event.get('Records')[0].has_key('s3') and 
            event.get('Records')[0].get('s3').has_key('object') and 
            event.get('Records')[0].get('s3').get('object').has_key('key')):

            # get S3 object key value    
            event_object = event.get('Records')[0]
            # get AWS region 
            s3_object = event.get('Records')[0].get('s3').get('object')

            input_key = s3_object.get('key')
            region = event_object.get('awsRegion')

            print ("Input media received : {0} from region {1}".format(input_key, region))
            print ("Starting ETS job to convert this media!")

            start_ets_job (region, input_key)

        else :
            return {'status' : 'ignored', 'message' : 'Invalid input'}
    except Exception as exception:
        return {'status' : 'error', 'message' : exception.message}

def start_ets_job(region, input_key):

    # HLS Presets that will be used to create an adaptive bitrate playlist.
    hls_64k_audio_preset_id = '1351620000001-200071';
    hls_0400k_preset_id     = '1351620000001-200050';
    hls_0600k_preset_id     = '1351620000001-200040';
    hls_1000k_preset_id     = '1351620000001-200030';
    hls_1500k_preset_id     = '1351620000001-200020';
    hls_2000k_preset_id     = '1351620000001-200010';    

    # HLS Segment duration that will be targeted.
    segment_duration = '2'

    #All outputs will have this prefix prepended to their output key.
    output_key_prefix = 'output/hls/'
    
    # Creating client for accessing elastic transcoder 
    transcoder_client = boto3.client('elastictranscoder', region)

    # Setup the job input using the provided input key.
    job_input = { 'Key': input_key }

    # Setup the job outputs using the HLS presets.

    # output_key = hashlib.sha256(input_key.encode('utf-8')).hexdigest()
    # above output_key hash clause is for randomizing the output key

    output_key = input_key

    #hls_audio = {
    #    'Key' : 'hlsAudio/' + output_key,
    #    'PresetId' : hls_64k_audio_preset_id,
    #    'SegmentDuration' : segment_duration
    #}
    hls_400k = {
        'Key' : 'hls0400k/' + output_key,
       'PresetId' : hls_0400k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_600k = {
        'Key' : 'hls0600k/' + output_key,
        'PresetId' : hls_0600k_preset_id,
        'SegmentDuration' : segment_duration
    }
    #hls_1000k = {
    #    'Key' : 'hls1000k/' + output_key,
    #    'PresetId' : hls_1000k_preset_id,
    #    'SegmentDuration' : segment_duration
    #}
    #hls_1500k = {
    #    'Key' : 'hls1500k/' + output_key,
    #    'PresetId' : hls_1500k_preset_id,
    #    'SegmentDuration' : segment_duration
    #}
    #hls_2000k = {
    #    'Key' : 'hls2000k/' + output_key,
    #    'PresetId' : hls_2000k_preset_id,
    #    'SegmentDuration' : segment_duration
    #}
    job_outputs = [ hls_400k, hls_600k ]

    # Setup master playlist which can be used to play using adaptive bitrate.
    playlist = {
        'Name' : 'hls_' + output_key,
        'Format' : 'HLSv3',
        'OutputKeys' : map(lambda x: x['Key'], job_outputs)
    }

    # Creating the job.
    create_job_request = {
        'PipelineId' : pipeline_id,
        'Input' : job_input,
        'OutputKeyPrefix' : output_key_prefix + output_key +'/',
        'Outputs' : job_outputs,
        'Playlists' : [ playlist ]
    }
    create_job_result=transcoder_client.create_job(**create_job_request)    
    print 'HLS job has been created: ', json.dumps(create_job_result['Job'], indent=4, sort_keys=True)
