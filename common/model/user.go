package model

import "time"

type User struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement:false" json:"id"`
	Username  string    `gorm:"type:varchar(64);uniqueIndex;not null" json:"username"`
	Email     string    `gorm:"type:varchar(128);uniqueIndex;not null" json:"email"`
	Password  string    `gorm:"type:varchar(256);not null" json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (User) TableName() string {
	return "users"
}
