package events

import (
	"encoding/json"
	"time"

	"github.com/bxcodec/faker/v3"
)

type MobileLog struct {
	EventID            string    `json:"event_id" faker:"uuid_hyphenated"`
	RecordedTimestamp  time.Time `json:"recorded_timestamp"`
	CollectedTimestamp time.Time `json:"collected_timestamp"`
	Application        string    `json:"application" faker:"oneof: alpha, bravo, charlie, delta, echo, foxtrot"`
	Version            string    `json:"application_version" faker:"oneof: v1.0, v1.1, v1.1-ALPHA, v1.1.1"`
	SystemVersion      string    `json:"operating_system_version" faker:"oneof: tau, phi, rho, zeta"`
	DeviceToken        string    `json:"device_token" faker:"jwt"`
	Manufacturer       string    `json:"manufacturer" faker:"oneof: sony, google, nokia, dell"`
	Category           string    `json:"category" faker:"oneof: bishop, knight, rook, pawn, quuen, king"`
	Level              int       `json:"level" faker:"oneof: 1, 2, 3, 4, 5"`
	Message            string    `json:"message" faker:"sentence"`
	MessageData        string    `json:"message_data" faker:"paragraph"`
}

func (ml MobileLog) Serialize() ([]byte, error) {
	return json.Marshal(ml)
}

func MockMobileLog() (MobileLog, error) {
	ml := MobileLog{}
	if err := faker.FakeData(&ml); err != nil {
		return ml, err
	}
	ml.RecordedTimestamp = time.Unix(faker.UnixTime(), 0)
	ml.CollectedTimestamp = ml.RecordedTimestamp.Add(time.Duration(random.Intn(10)) * time.Second)

	return ml, nil
}
