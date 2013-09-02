Travis Logs, in Go
==================

This is a Golang based version of http://github.com/travis-ci/travis-logs.


TODO
----

- interface the crap out of database and pusher and message broker

- TESTS!

- Handle AMQP connection issues?

- Sentry Errors

- Sanitize the log output (remove null chars etc.)

- 3 second timeouts for message processing

- Add a channel to talk to the logging go routines for things like shutdown
