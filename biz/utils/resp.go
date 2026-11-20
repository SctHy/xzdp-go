package utils

import (
	"context"
	"xzdp/biz/model/xzdp"

	"github.com/cloudwego/hertz/pkg/app"
)

// SendErrResponse  pack error response
func SendErrResponse(ctx context.Context, c *app.RequestContext, code int, err error) {
	label := "BUSINESS_ERROR"
	switch code {
	case 401:
		label = "UNAUTHENTICATED"
	case 403:
		label = "FORBIDDEN"
	case 503:
		label = "DEPENDENCY_UNAVAILABLE"
	case 500:
		label = "INTERNAL_ERROR"
	}
	message := "invalid request"
	if err != nil {
		message = err.Error()
	}
	c.Set("errorCode", label)
	c.JSON(code, map[string]interface{}{"success": false, "errorMsg": message, "code": label, "requestId": string(c.Response.Header.Peek("X-Request-ID"))})
}

// SendSuccessResponse  pack success response
func SendSuccessResponse(ctx context.Context, c *app.RequestContext, code int, data interface{}) {

	response := xzdp.NewSuccessResponse(data)
	c.JSON(code, response)
}

func SendRawResponse(ctx context.Context, c *app.RequestContext, code int, data interface{}) {

	c.JSON(code, data)
}
