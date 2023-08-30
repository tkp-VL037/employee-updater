package model

type Employee struct {
	ID        string
	Name      string
	Age       int
	Position  string
	Statistic Statistic `gorm:"foreignKey:EmployeeID"`
}
