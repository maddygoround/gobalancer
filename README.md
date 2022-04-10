# gobalancer
##  _A dump load balancer routing traffic based on bounded consistent hashing strategy_
Currenlty it is using query params for hashring you can chnage it host or httpheader bu changing
## Features
- Supports healthcheck every 30 seconds
- 3 retries before considering server as unavaiable
- Active and Passive healthcheck supports

```sh
serverpool.Strategy.name = "query"
```

By default, the loadbalancer will expose on port 3031, you change it by suppily port while starting loadbalancer
```sh
go run . --port {port}
```