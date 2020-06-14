redis-monitor-replication
=========================

Replicates stream of Redis commands from `MONITOR` output to another Redis instance.

Build:
```
go build redis-monitor-replication.go
```

Usage:
```
redis-cli -h redis1 monitor | ./redis-monitor-replication -h redis2 -log
```
