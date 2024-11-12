//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cache

import (
	"context"
	"time"
)

const DefaultDeletionInterval = 3 * time.Second

type Cache[T any] interface {
	Get(ctx context.Context, id uint64) ([]T, error)
	GetMultiple(ctx context.Context, docID uint64, vecID uint64) ([]float32, error)
	MultiGetMultiple(ctx context.Context, docIDs []uint64, vecIDs []uint64) ([][]float32, []error)
	PreloadMultiple(docID uint64, vecID uint64, vec []float32)
	MultiGet(ctx context.Context, ids []uint64) ([][]T, []error)
	Len() int32
	CountVectors() int64
	Delete(ctx context.Context, id uint64)
	Preload(id uint64, vec []T)
	PreloadNoLock(id uint64, vec []T)
	SetSizeAndGrowNoLock(id uint64)
	Prefetch(id uint64)
	PrefetchMultiple(docID uint64, vecID uint64)
	Grow(size uint64)
	Drop()
	UpdateMaxSize(size int64)
	CopyMaxSize() int64
	All() [][]T
	LockAll()
	UnlockAll()
}
