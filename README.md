# Geode Test Suite

### Requires geode to be started in rest API mode

```bash
$ ./gfsh
$ start locator --name=locator
$ configure pdx --read-serialized=true
$ start server --name=server --J=-Dgemfire.start-dev-rest-api=true --J=-Dgemfire.http-service-port=8000 --J=-Dgemfire.http-service-bind-address=localhost
$ create region --name=simple --type=REPLICATE
$ create region --name=person --type=REPLICATE
$ create region --name=timeseries --type=REPLICATE
$ create region --name=episode --type=REPLICATE
```

NOTE : Geode swagger UI should be accessible at http://localhost:8000/gemfire-api/docs/index.html
