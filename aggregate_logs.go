package main

import (
// "database/sql"
// "encoding/json"
// "fmt"
// "github.com/streadway/amqp"
// "log"
// //"strings"
// "sync"
// "time"
)

func AggregateLogParts() {
    setupDatabase()
    defer closeDatabase()

    // find first log which has finished and not being achived
    //  - check we have all log parts, including finished part
    //  - otherwise aggregate log parts which are more than 3 hours old

    // mark it as being aggregated (archiving_started_at)
    //  - use a seralize transaction

    // aggregate log parts into log model (content)

    // delete log parts

    // if archiving is enabled..
    //   archive to s3
}
