package listener

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strings"

	gomail "gopkg.in/gomail.v2"
)

type EmailMessage struct {
	ToAddress string `json:"to_address"`
	Subject   string `json:"subject"`
	BodyHTML  string `json:"body_html"`
	ZipBytes  string `json:"zip_bytes"`
	ZipSHA256 string `json:"zip_sha256"`
}

// ================= SMTP конфиг =================
type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

var smtpCfg = SMTPConfig{
	Host:     "mx.marinet.ru",
	Port:     25,
	Username: "test@marinet.ru",
	Password: "TE%27?25%st@est",
	From:     "test@marinet.ru",
}

type Attachment struct {
	Name        string
	ContentType string
	Data        []byte
}

func sendMessage(msg []byte) error {
	var m EmailMessage
	if err := json.Unmarshal(msg, &m); err != nil {
		return fmt.Errorf("ошибка парсинга JSON: %w", err)
	}

	// ================= base64 -> zip bytes =================
	zipData, err := base64.StdEncoding.DecodeString(stripSpaces(m.ZipBytes))
	if err != nil {
		return fmt.Errorf("ошибка декодирования base64(zip_bytes): %w", err)
	}

	// ================= verify sha256 =================
	sum := sha256.Sum256(zipData)
	if hex.EncodeToString(sum[:]) != normalizeHex(m.ZipSHA256) {
		return fmt.Errorf("ошибка целостности: хэши не совпадают")
	}

	// ================= распаковываем zip, собираем только изображения =================
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return fmt.Errorf("ошибка чтения ZIP: %w", err)
	}

	var attachments []Attachment
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("ошибка открытия файла %s: %w", f.Name, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return fmt.Errorf("ошибка чтения файла %s: %w", f.Name, err)
		}

		ct := mime.TypeByExtension(strings.ToLower(filepath.Ext(f.Name)))
		if ct == "" {
			ct = sniffContentType(data)
		}
		if strings.HasPrefix(ct, "image/") {
			if nm := sanitizeFilename(filepath.Base(f.Name)); nm != "" {
				attachments = append(attachments, Attachment{Name: nm, ContentType: ct, Data: data})
			}
		}
	}

	if err := sendEmailSMTP(smtpCfg, m.ToAddress, m.Subject, m.BodyHTML, attachments); err != nil {
		return fmt.Errorf("ошибка отправки письма: %w", err)
	}
	return nil
}

func sendEmailSMTP(cfg SMTPConfig, to, subject, htmlBody string, attachments []Attachment) error {
	m := gomail.NewMessage()
	m.SetHeader("From", cfg.From)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", htmlBody)

	for _, a := range attachments {
		ct := a.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		m.Attach(a.Name,
			gomail.SetHeader(map[string][]string{
				"Content-Type":        {ct + `; name="` + a.Name + `"`},
				"Content-Disposition": {`attachment; filename="` + a.Name + `"`},
			}),
			gomail.SetCopyFunc(func(w io.Writer) error {
				_, err := w.Write(a.Data)
				return err
			}),
		)
	}

	d := gomail.NewDialer(cfg.Host, cfg.Port, cfg.Username, cfg.Password)

	return d.DialAndSend(m)
}

// ================= helpers =================

func stripSpaces(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, s)
}

func normalizeHex(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	if strings.HasPrefix(s, `\x`) || strings.HasPrefix(s, `\X`) {
		s = s[2:]
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c >= 'A' && c <= 'F' {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
	return b.String()
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\n", "_")
	name = strings.ReplaceAll(name, "\r", "_")
	if name == "" {
		return "file"
	}
	return name
}

func sniffContentType(b []byte) string {
	if len(b) >= 8 {
		if b[0] == 0x89 && b[1] == 0x50 && b[2] == 0x4E && b[3] == 0x47 {
			return "image/png"
		}
		if b[0] == 0xFF && b[1] == 0xD8 {
			return "image/jpeg"
		}
		if string(b[:4]) == "GIF8" {
			return "image/gif"
		}
		if len(b) >= 12 && string(b[:4]) == "RIFF" && string(b[8:12]) == "WEBP" {
			return "image/webp"
		}
	}
	return "application/octet-stream"
}
