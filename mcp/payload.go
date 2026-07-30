package mcp

import (
	"encoding/base64"
	"unicode/utf8"
)

// maxInlinePayload is the size above which an outbound payload is reported
// as base64 plus payload_bytes/payload_truncated instead of a raw UTF-8
// string, regardless of how the caller wants to receive it. It exists so a
// single oversize or binary payload cannot blow up a tool result the model
// has to read in full.
const maxInlinePayload = 4096

// decodePayload applies the input policy: exactly one of payload
// (UTF-8 text) or payloadBase64 must be set. Both or neither is a
// validation error, because silently preferring one would make the other
// field's presence a silent no-op.
func decodePayload(payload, payloadBase64 *string) ([]byte, error) {
	havePayload := payload != nil
	haveB64 := payloadBase64 != nil
	switch {
	case havePayload && haveB64:
		return nil, &toolError{Code: "invalid_payload", Message: "set exactly one of payload or payload_base64, not both"}
	case !havePayload && !haveB64:
		return nil, &toolError{Code: "invalid_payload", Message: "set exactly one of payload or payload_base64"}
	case havePayload:
		return []byte(*payload), nil
	default:
		b, err := base64.StdEncoding.DecodeString(*payloadBase64)
		if err != nil {
			return nil, &toolError{Code: "invalid_payload", Message: "payload_base64 is not valid base64: " + err.Error()}
		}
		return b, nil
	}
}

// encodedPayload is the output policy applied to every job the server
// returns. Never silently lossy: a payload that isn't shown in full always
// says so via Truncated and Bytes.
type encodedPayload struct {
	Payload       *string `json:"payload,omitempty"`
	PayloadBase64 *string `json:"payload_base64,omitempty"`
	PayloadBytes  int     `json:"payload_bytes"`
	Truncated     bool    `json:"payload_truncated,omitempty"`
}

// encodePayload renders data as payload (UTF-8 text) when it is valid UTF-8
// and small enough to inline; otherwise as payload_base64, truncated to
// maxInlinePayload bytes with Truncated set so the caller knows it isn't
// seeing everything.
func encodePayload(data []byte) encodedPayload {
	out := encodedPayload{PayloadBytes: len(data)}
	if utf8.Valid(data) && len(data) <= maxInlinePayload {
		s := string(data)
		out.Payload = &s
		return out
	}
	shown := data
	if len(shown) > maxInlinePayload {
		shown = shown[:maxInlinePayload]
		out.Truncated = true
	}
	s := base64.StdEncoding.EncodeToString(shown)
	out.PayloadBase64 = &s
	return out
}
