/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package storage

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound defines the query item is not found in storage implement.
var ErrNotFound = errors.New("storage: NotFound")

// ErrUniqueConflict defines the item is conflict with unique key in storage implement.
var ErrUniqueConflict = errors.New("storage: UniqueKeyConflict")

// Store defines storage relevant interface
type Store interface {
	Ping() error
	PopulateData(skipDrop bool)
	FindTransGlobalStore(gid string) *TransGlobalStore
	ScanTransGlobalStores(position *string, limit int64) []TransGlobalStore
	FindBranches(ctx context.Context, gid string) []TransBranchStore
	UpdateBranches(branches []TransBranchStore, updates []string) (int, error)
	LockGlobalSaveBranches(ctx context.Context, gid string, status string, branches []TransBranchStore, branchStart int)
	MaySaveNewTrans(ctx context.Context, global *TransGlobalStore, branches []TransBranchStore) error
	ChangeGlobalStatus(ctx context.Context, global *TransGlobalStore, newStatus string, updates []string, finished bool)
	TouchCronTime(global *TransGlobalStore, nextCronInterval int64, nextCronTime *time.Time)
	LockOneGlobalTrans(ctx context.Context, expireIn time.Duration) *TransGlobalStore
	ResetCronTime(ctx context.Context, after time.Duration, limit int64) (succeedCount int64, hasRemaining bool, err error)
	ScanKV(cat string, position *string, limit int64) []KVStore
	FindKV(cat, key string) []KVStore
	UpdateKV(ctx context.Context, kv *KVStore) error
	DeleteKV(cat, key string) error
	CreateKV(ctx context.Context, cat, key, value string) error
}
