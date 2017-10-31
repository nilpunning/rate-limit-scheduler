# Rate Limit Scheduler

A work in progress

## TODO
```
Key:
_ Not done yet
/ Half done
Z Changed mind, not going to do.
X Done

_ Reduce queue-size & add a test around it.
_ Graceful shutdown that play nicely with Docker (sigterm)
    _ Make test
_ Write a good one sentance description of this project
_ Write a paragraph description
_ Add metrics look in git log for reference
X Make start and stop idempontent
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