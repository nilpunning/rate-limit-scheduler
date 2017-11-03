# Rate Limit Scheduler

The rate limit scheduler is meant to sit as a proxy between your services and a rate limited service you depend on. 

```
                     +------------------------+
Service --40 reqs--->|                        |
Service --40 reqs--->|  Rate Limit Scheduler  |-------------> Rate Limited Service
Service --40 reqs--->|                        |
                     +------------------------+

Requests to the rate limit scheduler are made in batches.  
The requests in each batch are scheduled to be requested of the rate limited service in a round robin fashion.  
The rate limited service's limits are respected.

                     +------------------------+
Service ------------>|                        |
Service ------------>|  Rate Limit Scheduler  |--100 reqs---> Rate Limited Service
Service ------------>|                        |                  (100 limit)
                     +------------------------+

The requesting services receive their fulfilled requests.

                     +------------------------+
Service <--34 resps--|                        |
Service <--33 resps--|  Rate Limit Scheduler  |<--100 resps-- Rate Limited Service
Service <--33 resps--|                        |                  (100 limit)
                     +------------------------+
```

Requests are collected over a two second window.  As the rate limit is approached more requests are made in an attempt to get as close to the limit as possible.


## TODO
```
Key:
_ Not done yet
/ Half done
Z Changed mind, not going to do.
X Done

Z Reduce queue-size & add a test around it.
_ Graceful shutdown that play nicely with Docker (sigterm)
    _ Make test
X Write a one sentance description of this project
X Write a paragraph description
X Add metrics look in git log for reference
X Make start and stop idempontent
_ Make test able to work remotely
```

## Troubleshooting

This error may be encountered when attempting a large number of requests:
```
ERROR - accept incoming request java.io.IOException: Too many open files
```

This may occur even before the specified limit is reached.  As a security precaution the operating system limits the number of possible open files.  This can be adjusted.
 
For example if you wanted to increase the limit to 10000 on Arch Linux you would edit /etc/systemd/system.conf and /etc/systemd/user.conf.  Both files must have the following line added to the Manager section:
```
DefaultLimitNOFILE=10000
```

## License

Copyright 2017 David O'Meara

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
