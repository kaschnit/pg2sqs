# pg2sqs

Change data capture for Postgres. Export Postgres changes to SQS.

## TODO list

- Design / architecture document
- Basic functionality implementation + unit tests
- Split out tools from go.mod into separate tools project + go workspaces
- Implement image + helm chart
- e2e tests (pg2sqs, Postgres, AWS emulator)
- Bench tests
- KIND deployment automation (pg2sqs, Postgres, AWS emulator)
