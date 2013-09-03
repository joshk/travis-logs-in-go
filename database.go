package main

import (
    "database/sql"
    "fmt"
    "github.com/lib/pq"
    "time"
)

type DB interface {
    FindLogId(int) (int, error)
    CreateLogPart(int, int, string, bool) error
    Close()
}

type RealDB struct {
    conn          *sql.DB
    jobIdFind     *sql.Stmt
    logPartCreate *sql.Stmt
}

// Force the compiler to check that RealDB implements DB.
var _ DB = &RealDB{}

func (db *RealDB) FindLogId(jobId int) (int, error) {
    var logId int
    err := db.jobIdFind.QueryRow(jobId).Scan(&logId)

    switch {
    case err == sql.ErrNoRows:
        return 0, fmt.Errorf("FindLogId: no log with job_id:%s found", jobId)
    case err != nil:
        return 0, fmt.Errorf("FindLogId: db query failed: %v", err)
    }

    return logId, nil
}

func (db *RealDB) CreateLogPart(logId int, number int, content string, final bool) error {
    var logPartId int
    err := db.logPartCreate.QueryRow(logId, number, content, final, time.Now()).Scan(&logPartId)

    switch {
    case err == sql.ErrNoRows:
        return fmt.Errorf("CreateLogPart: log part number:%s for logId:%s could not be created. (%v)", number, logId, err)
    case err != nil:
        return fmt.Errorf("CreateLogPart: db query failed: %v", err)
    }

    return nil
}

func (db *RealDB) Close() {
    db.conn.Close()
}

func NewRealDB(url string) (DB, error) {
    pgUrl, err := pq.ParseURL(url)
    if err != nil {
        return nil, err
    }

    db, err := sql.Open("postgres", pgUrl)
    if err != nil {
        return nil, err
    }

    if err = db.Ping(); err != nil {
        return nil, err
    }

    jobIdFind, err := db.Prepare("SELECT id FROM logs WHERE job_id=$1")
    if err != nil {
        return nil, err
    }

    logPartsCreate, err := db.Prepare("INSERT INTO log_parts (log_id, number, content, final, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id")
    if err != nil {
        return nil, err
    }

    return &RealDB{db, jobIdFind, logPartsCreate}, nil
}
