# Rate Limit Scheduler
...

## TODO
```
Key:
_ Not done yet
/ Half done
Z Changed mind, not going to do.
X Done

If core-test.request-test number of requests is increased from 2000 to 5000 on my machine the server receives this error:
ERROR - accept incoming request java.io.IOException: Too many open files
before httpkit :queue-size is reached.  Figure out where to set queue-size.

_ Reduce queue-size & add a test around it.
_ Graceful shutdown that play nicely with Docker (sigterm)
    _ Make test
_ Write a good one sentance description of this project
_ Write a paragraph description
_ Add metrics look in git log for reference
X Make start and stop idempontent
```