package main

import (
	"github.com/joho/godotenv"
	"github.com/tkp-VL037/employee-updater/constant"
	"github.com/tkp-VL037/employee-updater/db"
	"github.com/tkp-VL037/employee-updater/nsq"
)

func main() {
	godotenv.Load(".env")

	db.ConnectPostgres()

	nsq.StartConsumer(constant.TOPIC_EMPLOYEE_DETAIL, constant.CHANNEL_PROCESSING)
}
