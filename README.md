# MongoDB -> ElasticSearch Connector
This project watches the oplog for actions on the bernie database, and propagates any inserts or updates immediately to elasticsearch.

### Deploy
As with the other pieces of ES4BS, this project is deployed in docker, and requires the following yaml configuration to be in ```/opt/bernie/config.yml```:

```yaml
elasticsearch:
	mongouser: username
	mongopass: password
	mongohost: host
	mongoport: port
	host: es_host
```
