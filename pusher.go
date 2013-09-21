package main

import (
	"encoding/json"
	"fmt"
	"github.com/pkulak/simpletransport/simpletransport"
	"github.com/timonv/pusher"
	"net/http"
	"time"
)

type Pusher interface {
	Publish(int, int, string, bool) error
}

type LivePusher struct {
	client *pusher.Client
}

type PusherPayload struct {
	JobId   int    `json:"id"`
	Number  int    `json:"number"`
	Content string `json:"_log"`
	Final   bool   `json:"final"`
}

func (p *LivePusher) Publish(jobId int, number int, content string, final bool) error {
	payload := PusherPayload{
		JobId:   jobId,
		Number:  number,
		Content: content,
		Final:   final,
	}

	jsonPayload, err := json.Marshal(&payload)
	if err != nil {
		return fmt.Errorf("Publish: error during json.marshal: %v", err)
	}

	channel := fmt.Sprintf("job-%d", payload.JobId)

	if err = p.client.Publish(string(jsonPayload), "job:log", channel); err != nil {
		return fmt.Errorf("Publish: error publishing to pusher: %v", err)
	}

	return nil
}

func SetupPusher() {
	pusher.HttpClient = http.Client{
		Transport: &simpletransport.SimpleTransport{
			ReadTimeout:    3 * time.Second,
			RequestTimeout: 5 * time.Second,
		},
	}
}

func NewPusher(key string, secret string, appId string) (Pusher, error) {
	if key == "" {
		return nil, fmt.Errorf("pusher key was empty")
	}

	if secret == "" {
		return nil, fmt.Errorf("pusher secret was empty")
	}

	if appId == "" {
		return nil, fmt.Errorf("pusher app id was empty")
	}

	client := pusher.NewClient(appId, key, secret, false)

	return &LivePusher{client}, nil
}
