package repositories

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"gps-no-sync/internal/models"
)

type ClusterRepository struct {
	db *gorm.DB
}

func NewClusterRepository(db *gorm.DB) *ClusterRepository {
	return &ClusterRepository{db: db}
}

func (r *ClusterRepository) CreateOrUpdate(ctx context.Context, cluster *models.Cluster) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existingCluster models.Cluster
		err := tx.Where("name = ?", cluster.Name).First(&existingCluster).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return tx.Create(cluster).Error
		} else if err != nil {
			return err
		}

		existingCluster.Description = cluster.Description

		return tx.Save(&existingCluster).Error
	})
}

func (r *ClusterRepository) FindByName(ctx context.Context, name string) (*models.Cluster, error) {
	var cluster models.Cluster
	err := r.db.WithContext(ctx).Preload("Devices").Where("name = ?", name).First(&cluster).Error
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}

func (r *ClusterRepository) FindById(ctx context.Context, id uint) (*models.Cluster, error) {
	var cluster models.Cluster
	err := r.db.WithContext(ctx).Preload("Devices").First(&cluster, id).Error
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}
