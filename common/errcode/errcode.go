package errcode

const (
	Success     = 0
	ParamError  = 1001
	NotFound    = 1002
	InternalErr = 1003
	ShortenErr  = 2001
	InvalidURL  = 2002
	CodeExists  = 2003
)

var messages = map[int]string{
	Success:     "success",
	ParamError:  "参数错误",
	NotFound:    "资源不存在",
	InternalErr: "服务器内部错误",
	ShortenErr:  "短链接生成失败",
	InvalidURL:  "无效的 URL",
	CodeExists:  "短码已存在",
}

func Message(code int) string {
	if msg, ok := messages[code]; ok {
		return msg
	}
	return "未知错误"
}
