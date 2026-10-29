package session

import (
	"encoding/binary"
	"errors"
)

// Every frame is carried in one message of the underlying path:
//
//	DATA: 0x01 | seq uint64 | payload
//	FIN:  0x02 | seq uint64
//	ACK:  0x03 | next uint64
//
// seq numbers DATA and FIN frames in one sequence per direction. ACK carries
// the next sequence number the receiver has not yet handed to the application,
// which acknowledges everything before it.
//
// A redundant session also sends frames on a second path, with copyFlag set in
// the type. The receiver treats a copy like the original, except that a
// follower does not move to the path it came on.
const (
	frameData byte = 0x01
	frameFin  byte = 0x02
	frameAck  byte = 0x03

	copyFlag byte = 0x80
)

const frameHeaderLen = 1 + 8

var errBadFrame = errors.New("session: malformed frame")

type frame struct {
	kind    byte
	seq     uint64 // seq for DATA/FIN, next expected for ACK
	payload []byte
	copy    bool
}

func encodeFrame(kind byte, seq uint64, payload []byte) []byte {
	b := make([]byte, frameHeaderLen+len(payload))
	b[0] = kind
	binary.BigEndian.PutUint64(b[1:frameHeaderLen], seq)
	copy(b[frameHeaderLen:], payload)
	return b
}

func decodeFrame(b []byte) (frame, error) {
	if len(b) < frameHeaderLen {
		return frame{}, errBadFrame
	}
	f := frame{
		kind: b[0] &^ copyFlag,
		seq:  binary.BigEndian.Uint64(b[1:frameHeaderLen]),
		copy: b[0]&copyFlag != 0,
	}
	switch f.kind {
	case frameData:
		f.payload = b[frameHeaderLen:]
	case frameFin, frameAck:
		if len(b) != frameHeaderLen {
			return frame{}, errBadFrame
		}
	default:
		return frame{}, errBadFrame
	}
	return f, nil
}
