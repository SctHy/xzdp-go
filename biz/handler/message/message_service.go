package message

import (
	"context"

	service "xzdp/biz/service/message"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/hertz-contrib/sse"
)

// Sse .
// @router /sse [POST]
func Sse(ctx context.Context, c *app.RequestContext) {
	s := sse.NewStream(c)
	//c.Status(consts.StatusOK)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case <-subCtx.Done():
			hlog.Debugf("SSE stream closed")
			return
		default:
			req := ">"
			serv := service.NewSseService(subCtx, c)
			resp, err := serv.Run(req)
			if err != nil {
				hlog.Errorf("Error running SSE service: %v", err)
				continue
			}
			if resp == nil {
				continue
			}
			event := &sse.Event{
				Event: "message",
				Data:  []byte(*resp),
			}
			hlog.Debugf("SSE event: %v", event)

			if err := s.Publish(event); err != nil {
				hlog.Errorf("Error publishing SSE event: %v", err)
				return
			}
		}
	}
}
