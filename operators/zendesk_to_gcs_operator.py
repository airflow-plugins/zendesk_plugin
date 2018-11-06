import random
import json
import string
import logging
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks.ellevest_zd_hook import EllevestZendeskHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class ZendeskToGCSOperator(BaseOperator):
    """
    :param zendesk_conn_id:           The Zendesk connection id.
    :type zendesk_conn_id:            string
    :param zendesk_endpoint:          The Zendesk endpoint.
    :type zendesk_endpoint:           string
    :param zendesk_params:            Any relevant filter parameters to pass
                                      to Zendesk. This will vary by endpoint.
    :type zendesk_params:             string
    :param gcs_conn_id:               The GCS connection id.
    :type gcs_conn_id:                string
    :param gcs_key:                   The value of the key to be stored in GCS.
    :type gcs_key:                    string
    :param gcs_bucket:                The relevant bucket to store data in GCS.
    :type gcs_bucket:                 string
    """
    template_fields = ('gcs_key',
                       'gcs_bucket')

    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 zendesk_conn_id,
                 zendesk_endpoint=None,
                 zendesk_params={},
                 gcs_conn_id='google_cloud_default',
                 gcs_key=None,
                 gcs_bucket=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.zendesk_conn_id = zendesk_conn_id
        self.zendesk_endpoint = zendesk_endpoint
        self.zendesk_params = zendesk_params
        self.gcs_conn_id = gcs_conn_id
        self.gcs_key = gcs_key
        self.gcs_bucket = gcs_bucket
        self.coerced_fields = coerced_fields

    def execute(self, context):
        zd_hook = EllevestZendeskHook(self.zendesk_conn_id)

        data = zd_hook.call(self._mapper(self.zendesk_endpoint),
                            self.zendesk_params)

        self._output_manager(self._formatter('ndjson', data))

    def _mapper(self, endpoint):

        mapped_dict = {'users': '/api/v2/users.json?page=2',
                       'tickets': '/api/v2/tickets.json',
                       'tags': '/api/v2/tags.json',
                       'ticket_metrics': '/api/v2/ticket_metrics.json'}

        if endpoint.lower() not in mapped_dict.keys():
            raise Exception('Endpoint {} not currently supported...'.format(endpoint))

        return mapped_dict.get(endpoint, None)

    def _formatter(self, format, records):
        logging.info('Writing file to disk...')

        def random_string():
            return ''.join(random.choice(string.ascii_lowercase) for _ in range(10)) + '.json'

        file_name = random_string()

        if format == 'ndjson':
            logging.info('Writing file to disk...')
            with open(file_name, 'w') as f:
                f.write('\n'.join([json.dumps(record) for record in records]))

            logging.info('File written to disk...')

            print('Filesize of {}: '.format(file_name) + str(os.stat(file_name).st_size))

            return file_name
        else:
            raise Exception('Format not currently supported...')

    def _output_manager(self, file_name):
        logging.info('Uploading to GCS Bucket: {}...'.format(self.gcs_bucket))

        gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)

        gcs_hook.upload(self.gcs_bucket,
                        self.gcs_key,
                        file_name,
                        mime_type='application/x-ndjson')

        logging.info('File Uploaded...')
        logging.info('Removing {}...'.format(file_name))
        os.remove(file_name)
