package logger 
import (
	"go.uber.org/zap"
)
var Log *zap.Logger
func Init(){
	logger,err:=zap.NewProduction()
	if err!=nil{
		panic(err)
	}
	Log=logger
}
