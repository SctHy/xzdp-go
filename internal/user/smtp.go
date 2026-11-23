package user

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"time"
)

type SMTP struct{ Address, From, User, Password string }

func SMTPConfig() SMTP {
	return SMTP{os.Getenv("XZDP_SMTP_ADDRESS"), os.Getenv("XZDP_SMTP_FROM"), os.Getenv("XZDP_SMTP_USER"), os.Getenv("XZDP_SMTP_PASSWORD")}
}
func (s SMTP) Send(ctx context.Context, to, code string) error {
	sender, err := mail.ParseAddress(s.From)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	host, _, err := net.SplitHostPort(s.Address)
	if err != nil {
		return err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.Address)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Close()
	if host != "mailpit" || s.User != "" {
		if err = c.StartTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if s.User != "" {
		if err = c.Auth(smtp.PlainAuth("", s.User, s.Password, host)); err != nil {
			return err
		}
	}
	if err = c.Mail(sender.Address); err != nil {
		return err
	}
	if err = c.Rcpt(to); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintf(w, "From: %s\r\nTo: %s\r\nSubject: XZDP verification code\r\n\r\nYour code is %s. It expires in 5 minutes.\r\n", sender.Address, to, code); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
