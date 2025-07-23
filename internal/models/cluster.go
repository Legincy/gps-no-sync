package models

import "gorm.io/gorm"

type Cluster struct {
	gorm.Model
	Name        string   `gorm:"uniqueIndex;not null" json:"name"`
	Description string   `gorm:"type:text" json:"description"`
	Devices     []Device `gorm:"foreignKey:ClusterID" json:"devices,omitempty"`
}
