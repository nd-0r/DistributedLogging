# DistributedLogging

A distributed log-querying system

## Design

Each logservent serves a directory of logs. Since each logservent starts with a list of addresses of all other logservents in the system, each logservent can act as a client to all other logservents in the system. The dgrep utility (short for distributed grep) queries an individual logservent, which then greps the query locally and collects the grep results from all other servers, sending the aggregate back to the dgrep utility.

## Testing

The core functionality for grepping a directory of log files and yielding the matching lines on a channel is tested in the `chan_grep` package written in Go.

End-to-end tests covering different log sizes, distributions, and query frequency are in the `tests` directory and written in Python.
