package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
)


func splitHeaderBody(raw string) (headers []string, body string) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	parts := strings.SplitN(normalized, "\n\n", 2)

	headerBlock := parts[0]
	for _, line := range strings.Split(headerBlock, "\n") {
		if strings.TrimSpace(line) != "" {
			headers = append(headers, line)
		}
	}

	if len(parts) == 2 {
		body = parts[1]
	}
	return headers, body
}

// buildMessage renders the email template for this recipient and, if an
// attachment is currently configured, wraps it into a multipart/mixed MIME
// message with the file attached. With no attachment configured it returns
// the plain rendered template exactly as before.
func buildMessage(r Recipient) ([]byte, error) {
	rendered, err := Template(r)
	if err != nil {
		return nil, err
	}

	path, filename := getAttachment()
	if path == "" {
		return []byte(rendered), nil
	}

	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("attachment read error: %v", err)
	}

	headers, body := splitHeaderBody(rendered)

	if filename == "" {
		filename = filepath.Base(path)
	}
	mimeType := mime.TypeByExtension(filepath.Ext(filename))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	const boundary = "relay-console-boundary-7a1f2c"

	var buf bytes.Buffer

	// Keep the user's own headers (From, Subject, ...), but drop any
	// Content-Type/MIME-Version they wrote — we set our own multipart ones.
	for _, h := range headers {
		lower := strings.ToLower(h)
		if strings.HasPrefix(lower, "content-type:") || strings.HasPrefix(lower, "mime-version:") {
			continue
		}
		buf.WriteString(h + "\r\n")
	}
	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%q\r\n\r\n", boundary))

	// Body part
	buf.WriteString("--" + boundary + "\r\n")
	buf.WriteString("Content-Type: text/plain; charset=\"UTF-8\"\r\n\r\n")
	buf.WriteString(body)
	buf.WriteString("\r\n\r\n")

	// Attachment part
	buf.WriteString("--" + boundary + "\r\n")
	buf.WriteString(fmt.Sprintf("Content-Type: %s; name=%q\r\n", mimeType, filename))
	buf.WriteString("Content-Transfer-Encoding: base64\r\n")
	buf.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=%q\r\n\r\n", filename))

	encoded := base64.StdEncoding.EncodeToString(fileBytes)
	for i := 0; i < len(encoded); i += 76 {
		end := i + 76
		if end > len(encoded) {
			end = len(encoded)
		}
		buf.WriteString(encoded[i:end] + "\r\n")
	}

	buf.WriteString("\r\n--" + boundary + "--\r\n")

	return buf.Bytes(), nil
}
