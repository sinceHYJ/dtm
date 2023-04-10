/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/dtm-labs/dtm/dtmutil"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/logger"
)

// NowForwardDuration will be set in test, trans may be timeout
var NowForwardDuration = time.Duration(0)

// CronForwardDuration will be set in test. cron will fetch trans which expire in CronForwardDuration
var CronForwardDuration = time.Duration(0)

// CronTransOnce cron expired trans. use expireIn as expire time
func CronTransOnce() (gid string) {
	defer handlePanic(nil)
	ctx, span := dtmutil.StartSpan(context.Background(), "cron", "cronTransOnce")
	defer span.End()
	trans := lockOneTrans(ctx, CronForwardDuration)
	if trans == nil {
		return
	}
	gid = trans.Gid
	trans.WaitResult = true
	branches := GetStore().FindBranches(ctx, gid)
	err := trans.Process(branches)
	dtmimp.PanicIf(err != nil && !errors.Is(err, dtmcli.ErrFailure) && !errors.Is(err, dtmcli.ErrOngoing), err)
	span.End()
	return
}

// CronExpiredTrans cron expired trans, num == -1 indicate for ever
func CronExpiredTrans(num int) {
	for i := 0; i < num || num == -1; i++ {
		gid := CronTransOnce()
		if gid == "" && num != 1 {
			sleepCronTime()
		}
	}
}

// CronUpdateTopicsMap cron updates topics map
func CronUpdateTopicsMap(ctx context.Context) {
	for {
		time.Sleep(time.Duration(conf.ConfigUpdateInterval) * time.Second)
		cronUpdateTopicsMapOnce(ctx)
	}
}

func cronUpdateTopicsMapOnce(ctx context.Context) {
	defer handlePanic(nil)
	updateTopicsMap(ctx)
}

func lockOneTrans(ctx context.Context, expireIn time.Duration) *TransGlobal {
	lockOneTransCtx, span := dtmutil.StartSpan(ctx, "cron", "lockOneTrans")
	defer span.End()
	global := GetStore().LockOneGlobalTrans(lockOneTransCtx, expireIn)
	if global == nil {
		return nil
	}
	logger.Infof("cron job return a trans: %s", global.String())
	return &TransGlobal{TransGlobalStore: *global, Context: ctx}
}

func handlePanic(perr *error) {
	if err := recover(); err != nil {
		logger.Errorf("----recovered panic %v\n%s", err, string(debug.Stack()))
		if perr != nil {
			*perr = fmt.Errorf("dtm panic: %v", err)
		}
	}
}

func sleepCronTime() {
	normal := time.Duration((float64(conf.TransCronInterval) - rand.Float64()) * float64(time.Second))
	interval := dtmimp.If(CronForwardDuration > 0, 1*time.Millisecond, normal).(time.Duration)
	logger.Debugf("sleeping for %v", interval)
	time.Sleep(interval)
}
