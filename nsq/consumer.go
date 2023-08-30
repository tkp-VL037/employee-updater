package nsq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/tkp-VL037/employee-updater/db"
	"github.com/tkp-VL037/employee-updater/db/model"
	"gorm.io/gorm"
)

type EmployeeResponse struct {
	Employee  Employee  `json:"employee"`
	Statistic Statistic `json:"statistic"`
	Timestamp time.Time
}

type Employee struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Age      int    `json:"age"`
	Position string `json:"position"`
}

type Statistic struct {
	ID        string `json:"id"`
	ViewCount int    `json:"viewCount"`
}

type messageHandler struct{}

type Message struct {
	Sender    string
	Content   interface{}
	Timestamp time.Time
}

func StartConsumer(topic string, channel string) {
	config := nsq.NewConfig()

	config.MaxAttempts = 10
	config.MaxInFlight = 5

	config.MaxRequeueDelay = time.Second * 900
	config.DefaultRequeueDelay = time.Second * 0

	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(&messageHandler{})
	consumer.ConnectToNSQLookupd(os.Getenv("NSQ_LOOKUPD"))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	consumer.Stop()
}

func (h *messageHandler) HandleMessage(m *nsq.Message) error {
	var message Message

	if err := json.Unmarshal(m.Body, &message); err != nil {
		log.Println("error when Unmarshal() the message body, err:", err)
		return err
	}

	fmt.Println("message consumed! from: ", message.Sender)

	contentMap, ok := message.Content.(map[string]interface{})
	if !ok {
		return errors.New("message is not type of map[string]interface{}")
	}

	employeeData, employeeOK := contentMap["employee"].(map[string]interface{})

	var employee *Employee
	if employeeOK {
		employee = &Employee{
			ID:       employeeData["id"].(string),
			Name:     employeeData["name"].(string),
			Age:      int(employeeData["age"].(float64)),
			Position: employeeData["position"].(string),
		}
	}

	var employeeDB *model.Employee
	if err := db.PostgresDB.Preload("Statistic").First(&employeeDB, "id = ?", employee.ID).Error; err != nil {
		return err
	}

	fmt.Println(employeeDB)

	err := db.PostgresDB.Model(&model.Statistic{}).Where("employee_id = ?", employeeDB.ID).
		UpdateColumn("view_count", gorm.Expr("view_count + ?", 1)).Error
	if err != nil {
		return err
	}

	return nil
}
