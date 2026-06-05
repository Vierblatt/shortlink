package model

import "time"

type Link struct {
	ID        uint64     `gorm:"primaryKey;autoIncrement:false" json:"id"`
	ShortCode string     `gorm:"type:varchar(16);uniqueIndex;not null" json:"short_code"`
	LongURL   string     `gorm:"type:text;not null" json:"long_url"`
	UserID    uint64     `gorm:"not null;default:0" json:"user_id"`
	ExpireAt  *time.Time `json:"expire_at,omitempty"`
	Password  string     `gorm:"type:varchar(64);default:null" json:"password,omitempty"`
	Status    int8       `gorm:"not null;default:1" json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func (Link) TableName() string {
	return "links"
}

type AccessLog struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	ShortCode string    `gorm:"type:varchar(16);not null;index" json:"short_code"`
	IP        string    `gorm:"type:varchar(64);not null" json:"ip"`
	UserAgent string    `gorm:"type:text;not null" json:"user_agent"`
	Referer   string    `gorm:"type:text" json:"referer,omitempty"`
	Country   string    `gorm:"type:varchar(32)" json:"country,omitempty"`
	Province  string    `gorm:"type:varchar(32)" json:"province,omitempty"`
	City      string    `gorm:"type:varchar(32)" json:"city,omitempty"`
	Device    string    `gorm:"type:varchar(32)" json:"device,omitempty"`
	Browser   string    `gorm:"type:varchar(32)" json:"browser,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (AccessLog) TableName() string {
	return "access_logs"
}

type LinkStat struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	ShortCode string    `gorm:"type:varchar(16);not null;uniqueIndex:idx_code_date" json:"short_code"`
	Date      string    `gorm:"type:date;not null;uniqueIndex:idx_code_date" json:"date"`
	PV        int       `gorm:"not null;default:0" json:"pv"`
	UV        int       `gorm:"not null;default:0" json:"uv"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (LinkStat) TableName() string {
	return "link_stats"
}
