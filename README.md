# Zendesk Plugin


## Operators

### ZendeskToGCSOperator

This operator composes the logic for this plugin. It fetches the Zendesk specified object and saves the result in GCS. The parameters it can accept include the following:
```
  :param zendesk_conn_id:           The Zendesk connection id.
  :type zendesk_conn_id:            string
  :param zendesk_endpoint:          The Zendesk endpoint.
  :type zendesk_endpoint:           string
  :param zendesk_params:            Any relevant filter parameters to pass to Zendesk. This will vary by endpoint.
  :type zendesk_params:             string
  :param gcs_conn_id:               The GCS connection id.
  :type gcs_conn_id:                string
  :param gcs_key:                   The value of the key to be stored in GCS.
  :type gcs_key:                    string
  :param gcs_bucket:                The relevant bucket to store data in GCS.
  :type gcs_bucket:                 string
```
## License
Apache 2.0
