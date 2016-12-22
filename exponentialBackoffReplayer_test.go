package exponentialBackoffReplayer_test

import (
	"exponentialBackoffReplayer"
	"fmt"
	"testing"
)

func TestGeneralUsage(t *testing.T) {
	replayer := exponentialBackoffReplayer.NewExponentialBackoffReplayer(1000, 1.5)
	nextTs := replayer.PeekNextUnackedTimestamp()
	replayer.PutUnackedMessage(
		2,
		"fake_Message",
		2,
	)
	nextTs2 := replayer.PeekNextUnackedTimestamp()

	if nextTs > nextTs2 {
		t.Error("Expected initial timestamp to be less than timestamp after large backoff")
	}

	messageId, encodedMessage, attemptNumber := replayer.PopNextUnacked()
	if messageId != 2 ||
		encodedMessage != "fake_Message" ||
		attemptNumber != 2 {
		t.Error("Values not equal")
	}

}

func TestAckEmpty(t *testing.T) {
	replayer := exponentialBackoffReplayer.NewExponentialBackoffReplayer(1000, 1.5)
	replayer.AckMessage(1)
	replayer.AckMessage(2)
	replayer.AckMessage(3)
}

func TestMaxSize(t *testing.T) {
	replayer := exponentialBackoffReplayer.NewExponentialBackoffReplayer(10, 1.5)
	for i := 0; i < 11; i++ {
		replayer.PutUnackedMessage(
			i,
			fmt.Sprintf("Message %d", i),
			0,
		)
	}
	messageId, _, _ := replayer.PopNextUnacked()
	if messageId == 0 {
		t.Error("Values should have overflowed and initially value should have gone away")
	}
}

func TestAckMessage(t *testing.T) {
	replayer := exponentialBackoffReplayer.NewExponentialBackoffReplayer(10, 1.5)
	for i := 0; i < 11; i++ {
		replayer.PutUnackedMessage(
			i,
			fmt.Sprintf("Message %d", i),
			0,
		)
	}
	replayer.AckMessage(0)
	replayer.AckMessage(1)
	replayer.AckMessage(2)
	replayer.AckMessage(3)
	replayer.AckMessage(5)
	replayer.AckMessage(6)
	messageId, _, _ := replayer.PopNextUnacked()
	if messageId != 4 {
		t.Error("I guess Ack did not work")
	}
}
