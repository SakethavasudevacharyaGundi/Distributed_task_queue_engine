package handlers

var Registry = map[string]TaskHandler{
	"process_image": &ProcessImageHandler{},
	"send_email":    &SendEmailHandler{},
}
