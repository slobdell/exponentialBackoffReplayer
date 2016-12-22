package exponentialBackoffReplayer

import (
	"math"
	"sync"
	"time"
)

var mutex sync.Mutex

func GetTimestampMs() float64 {
	return float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000.0
}

type ExponentialBackoffReplayerInterface interface {
	PutUnackedMessage(messageId int, encodedMessage string, attemptNumber int)
	AckMessage(messageId int)
	PeekNextUnackedTimestamp() float64
	PopNextUnacked() (messageId int, encodedMessage string, attemptNumber int)
}

type ExponentialBackoffReplayer struct {
	maxSize                  int
	timeoutSeconds           float64
	idToContent              map[int]string
	idToAttempt              map[int]int
	secondaryIdxTimestamp    map[float64][]int
	secondaryIdxTimestampInv map[int]float64
}

func (e *ExponentialBackoffReplayer) PutUnackedMessage(messageId int, encodedMessage string, attemptNumber int) {
	mutex.Lock()
	defer mutex.Unlock()

	if len(e.idToContent) >= e.maxSize {
		e.popNextUnackedAssumeLocked()
	}

	e.idToContent[messageId] = encodedMessage
	e.idToAttempt[messageId] = attemptNumber
	offset := math.Pow(2, float64(attemptNumber)) * e.timeoutSeconds
	replayAt := GetTimestampMs() + offset

	e.secondaryIdxTimestamp[replayAt] = append(e.secondaryIdxTimestamp[replayAt], messageId)
	e.secondaryIdxTimestampInv[messageId] = replayAt
}

func (e *ExponentialBackoffReplayer) AckMessage(messageId int) {
	mutex.Lock()
	defer mutex.Unlock()
	e.ackMessageAssumeLocked(messageId)
}

func (e *ExponentialBackoffReplayer) ackMessageAssumeLocked(messageId int) {
	if _, ok := e.idToContent[messageId]; !ok {
		return
	}
	delete(e.idToContent, messageId)
	delete(e.idToAttempt, messageId)

	replayAt := e.secondaryIdxTimestampInv[messageId]
	delete(e.secondaryIdxTimestampInv, messageId)

	existingSlice := e.secondaryIdxTimestamp[replayAt]
	if len(existingSlice) <= 1 {
		delete(e.secondaryIdxTimestamp, replayAt)
	} else {
		newSlice := make([]int, len(existingSlice)-1)
		index := 0
		for _, savedMessageId := range existingSlice {
			if savedMessageId != messageId {
				newSlice[index] = savedMessageId
				index++
			}
		}
		e.secondaryIdxTimestamp[replayAt] = newSlice
	}
}

func (e ExponentialBackoffReplayer) getMinimumTimestamp() float64 {
	var minimumValue float64 = math.MaxFloat64
	for k, _ := range e.secondaryIdxTimestamp {
		if k < minimumValue {
			minimumValue = k
		}
	}
	return minimumValue
}

func (e ExponentialBackoffReplayer) PeekNextUnackedTimestamp() float64 {
	mutex.Lock()
	defer mutex.Unlock()

	if len(e.idToContent) == 0 {
		return GetTimestampMs() + e.timeoutSeconds
	}
	return e.getMinimumTimestamp()
}
func (e *ExponentialBackoffReplayer) PopNextUnacked() (int, string, int) {
	mutex.Lock()
	defer mutex.Unlock()
	if len(e.idToContent) == 0 {
		panic("Trying to pop from an empty idToContent structure")
	}
	return e.popNextUnackedAssumeLocked()
}

func (e *ExponentialBackoffReplayer) popNextUnackedAssumeLocked() (int, string, int) {
	nextMessageId := e.secondaryIdxTimestamp[e.getMinimumTimestamp()][0]
	messageContent := e.idToContent[nextMessageId]
	attemptNumber := e.idToAttempt[nextMessageId]

	e.ackMessageAssumeLocked(nextMessageId)

	return nextMessageId, messageContent, attemptNumber
}

func NewExponentialBackoffReplayer(maxSize int, timeout float64) ExponentialBackoffReplayerInterface {
	return &ExponentialBackoffReplayer{
		maxSize:                  maxSize,
		timeoutSeconds:           timeout,
		idToContent:              make(map[int]string),
		idToAttempt:              make(map[int]int),
		secondaryIdxTimestamp:    make(map[float64][]int),
		secondaryIdxTimestampInv: make(map[int]float64),
	}
}
