package models

import "gorm.io/gorm"

type Cluster struct {
	gorm.Model
	Name        string    `gorm:"uniqueIndex;not null" json:"name"`
	Description string    `gorm:"type:text" json:"description"`
	Devices     []Station `gorm:"foreignKey:ClusterId" json:"devices,omitempty"`
}

type ClusterMqDto struct {
	Name    string   `json:"name"`
	Devices []string `json:"devices"`
}

func (c *Cluster) ToMqDto() *ClusterMqDto {
	clusterDto := &ClusterMqDto{
		Name:    c.Name,
		Devices: make([]string, len(c.Devices)),
	}

	for i, device := range c.Devices {
		clusterDto.Devices[i] = device.MacAddress
	}

	return clusterDto
}
