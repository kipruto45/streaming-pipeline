# Security Notes

## What is secured today

- Schema Registry enforces Avro contracts so malformed events are rejected before they enter the main stream.
- Pydantic validation is used for configuration values to reduce the chance of invalid runtime settings.
- The dead-letter queue isolates poison pills from the main pipeline so one bad event does not halt downstream processing.
- Secrets are not committed to source control; local configuration is expected to come from environment files such as .env.

## What is not secured yet

- Kafka currently runs with PLAINTEXT defaults for local development.
- Grafana uses default admin credentials in the local stack.
- PostgreSQL uses a development password that should not be reused in production.

## What to do before production

- Enable SASL_SSL on Kafka and rotate all credentials through a secrets manager such as AWS Secrets Manager or GCP Secret Manager.
- Apply Kafka ACLs per consumer group and topic to restrict access.
- Enable TLS on Schema Registry and protect the service behind network controls.
- Set Grafana behind a reverse proxy with proper authentication and least-privilege access.

For production Kafka security guidance, see the Confluent documentation: https://docs.confluent.io/platform/current/security/index.html
