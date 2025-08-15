package models

import "gorm.io/gorm"

type Cluster struct {
	gorm.Model
	Name        string    `gorm:"uniqueIndex;not null" json:"name"`
	Description string    `gorm:"type:text" json:"description"`
	Devices     []Station `gorm:"foreignKey:ClusterId" json:"devices,omitempty"`
}
