package main

import (
	"fmt"
	"strings"
	// "strconv"
	"encoding/json"
)

type Payload struct {
	JobId   int    `json:"id"`
	Number  int    `json:"number"`
	Content string `json:"log"`
	Final   bool   `json:"final"`
	UUID    string `json:"uuid"`
}

type LogPartsProcessor struct {
	db           DB
	pusherClient Pusher
}

func (lpp *LogPartsProcessor) Process(message []byte) error {
	var err error

	appMetrics.TimeLogPartProcessing(func() {
		payload, err := lpp.parseMessageBody(message)
		if err != nil {
			return
		}

		logId, err := lpp.findLogId(payload)
		if err != nil {
			return
		}

		err = lpp.createLogPart(logId, payload)
		if err != nil {
			return
		}

		err = lpp.streamToPusher(payload)
		if err != nil {
			return
		}
	})

	if err != nil {
		appMetrics.MarkFailedLogPartCount()
		return err
	}

	return nil
}

func (lpp *LogPartsProcessor) parseMessageBody(message []byte) (*Payload, error) {
	payload := &Payload{}
	err := json.Unmarshal(message, payload)

	if err != nil {
		return nil, fmt.Errorf("parseMessageBody: error during json.unmarshal: %v", err)
	}

	payload.Content = strings.Replace(payload.Content, "\x00", "", -1)
	//fmt.Printf("job_id:%d number:%d\n", payload.JobId, payload.Number)
	//fmt.Printf("%#v\n", payload.Content)

	return payload, nil
}

// add a timeout and retry
func (lpp *LogPartsProcessor) findLogId(payload *Payload) (int, error) {
	logId, err := lpp.db.FindLogId(payload.JobId)

	if err != nil {
		return -1, err
	}

	return logId, nil
}

// add a timeout and retry
func (lpp *LogPartsProcessor) createLogPart(logId int, payload *Payload) error {
	err := lpp.db.CreateLogPart(logId, payload.Number, payload.Content, payload.Final)

	if err != nil {
		return err
	}

	return nil
}

// add a timeout and retry
func (lpp *LogPartsProcessor) streamToPusher(payload *Payload) error {
	var err error

	appMetrics.TimePusher(func() {
		err = lpp.pusherClient.Publish(payload.JobId, payload.Number, payload.Content, payload.Final)
	})

	if err != nil {
		appMetrics.MarkFailedPusherCount()
		return err
	}

	return nil
}
