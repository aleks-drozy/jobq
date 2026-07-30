package mcp

import (
	"encoding/base64"
	"strings"
	"testing"
)

func strPtr(s string) *string { return &s }

func TestDecodePayloadPolicy(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte{0x00, 0xFF, 0x10})

	cases := []struct {
		name          string
		payload       *string
		payloadBase64 *string
		wantBytes     []byte
		wantErrCode   string
	}{
		{"utf8 text", strPtr("hello"), nil, []byte("hello"), ""},
		{"binary via base64", nil, strPtr(b64), []byte{0x00, 0xFF, 0x10}, ""},
		{"neither field set", nil, nil, nil, "invalid_payload"},
		{"both fields set", strPtr("x"), strPtr("eA=="), nil, "invalid_payload"},
		{"invalid base64", nil, strPtr("not-valid-base64!!"), nil, "invalid_payload"},
		{"empty utf8 string is valid", strPtr(""), nil, []byte{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePayload(tc.payload, tc.payloadBase64)
			if tc.wantErrCode != "" {
				te, ok := err.(*toolError)
				if !ok || te.Code != tc.wantErrCode {
					t.Fatalf("err = %v, want *toolError{Code: %q}", err, tc.wantErrCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != string(tc.wantBytes) {
				t.Fatalf("decoded = %v, want %v", got, tc.wantBytes)
			}
		})
	}
}

func TestEncodePayloadPolicy(t *testing.T) {
	t.Run("small utf8 comes back as payload", func(t *testing.T) {
		out := encodePayload([]byte("hello"))
		if out.Payload == nil || *out.Payload != "hello" {
			t.Fatalf("Payload = %v, want \"hello\"", out.Payload)
		}
		if out.PayloadBase64 != nil {
			t.Fatal("PayloadBase64 must be unset when Payload is set")
		}
		if out.PayloadBytes != 5 {
			t.Fatalf("PayloadBytes = %d, want 5", out.PayloadBytes)
		}
		if out.Truncated {
			t.Fatal("Truncated = true, want false")
		}
	})

	t.Run("binary (non-UTF-8) comes back as base64", func(t *testing.T) {
		data := []byte{0xFF, 0xFE, 0x00, 0x80}
		out := encodePayload(data)
		if out.Payload != nil {
			t.Fatal("Payload must be unset for non-UTF-8 data")
		}
		if out.PayloadBase64 == nil {
			t.Fatal("PayloadBase64 must be set for non-UTF-8 data")
		}
		if *out.PayloadBase64 != base64.StdEncoding.EncodeToString(data) {
			t.Fatalf("PayloadBase64 = %q, want the base64 of the raw bytes", *out.PayloadBase64)
		}
		if out.PayloadBytes != len(data) {
			t.Fatalf("PayloadBytes = %d, want %d", out.PayloadBytes, len(data))
		}
		if out.Truncated {
			t.Fatal("Truncated = true, want false — under the inline limit")
		}
	})

	t.Run("oversize payload is truncated, never silently", func(t *testing.T) {
		data := []byte(strings.Repeat("a", maxInlinePayload+100))
		out := encodePayload(data)
		if !out.Truncated {
			t.Fatal("Truncated = false, want true")
		}
		if out.PayloadBytes != len(data) {
			t.Fatalf("PayloadBytes = %d, want the FULL original length %d, not the truncated one", out.PayloadBytes, len(data))
		}
		if out.PayloadBase64 == nil {
			t.Fatal("oversize payload must come back as PayloadBase64, even though it was valid UTF-8")
		}
		shown, err := base64.StdEncoding.DecodeString(*out.PayloadBase64)
		if err != nil {
			t.Fatal(err)
		}
		if len(shown) != maxInlinePayload {
			t.Fatalf("shown bytes = %d, want exactly maxInlinePayload=%d", len(shown), maxInlinePayload)
		}
	})

	t.Run("empty payload", func(t *testing.T) {
		out := encodePayload([]byte{})
		if out.Payload == nil || *out.Payload != "" {
			t.Fatalf("Payload = %v, want empty string", out.Payload)
		}
		if out.PayloadBytes != 0 {
			t.Fatalf("PayloadBytes = %d, want 0", out.PayloadBytes)
		}
	})
}
