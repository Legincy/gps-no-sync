package models

import "time"

type Cluster struct {
	ID          uint       `gorm:"primaryKey" json:"id"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
	DeletedAt   *time.Time `gorm:"index" json:"deleted_at"`
	Name        string     `gorm:"uniqueIndex;not null" json:"name"`
	Description string     `gorm:"type:text" json:"description"`
	Stations    []Station  `gorm:"foreignKey:ClusterID" json:"stations,omitempty"`
}

type ClusterDto struct {
	Name     string   `json:"name"`
	Stations []string `json:"stations"`
}

func (c *Cluster) ToDto() *ClusterDto {
	clusterDto := &ClusterDto{
		Name:     c.Name,
		Stations: make([]string, len(c.Stations)),
	}

	for i, station := range c.Stations {
		clusterDto.Stations[i] = station.MacAddress
	}

	return clusterDto
}
