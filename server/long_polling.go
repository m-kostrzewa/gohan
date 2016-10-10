// Copyright (C) 2015 NTT Innovation Institute, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"strings"
	"sync"

	"github.com/cloudwan/gohan/db"
	"github.com/cloudwan/gohan/db/transaction"
	"github.com/cloudwan/gohan/schema"
	gohan_sync "github.com/cloudwan/gohan/sync"
)

const (
	longPollPrefix          = "/gohan/long_poll_notifications/"
	longPollNotificationTTL = 10 // sec
)

var longPollDispatch *MessageDispatch
var initOnce sync.Once

func initMessageDispatch() {
	longPollDispatch = NewMessageDispatch(func(input string) string {
		return input
	})
}

func LongPollDispatch() *MessageDispatch {
	initOnce.Do(initMessageDispatch)
	return longPollDispatch
}

func notifyKeyUpdateSubscribers(fullKey string) error {

	log.Critical("%s notifying START.", fullKey)
	md := LongPollDispatch()
	md.Send(fullKey)
	log.Critical("%s notifying DONE.", fullKey)
	return nil
}

// DbLongPollNotifierWrapper notifies long poll subscribers on modifying transactions (create/update/delete).
type DbLongPollNotifierWrapper struct {
	db.DB
	gohan_sync.Sync
}

type transactionLongPollNotifier struct {
	transaction.Transaction
	sync         gohan_sync.Sync
	resourcePath string
}

func newTransactionLongPollNotifier(tx transaction.Transaction, sync gohan_sync.Sync) *transactionLongPollNotifier {
	return &transactionLongPollNotifier{tx, sync, ""}
}

// Begin begins a transaction, which will potentially (only for modifying transactions) notify long poll subscribers.
func (notifierWrapper *DbLongPollNotifierWrapper) Begin() (transaction.Transaction, error) {
	tx, err := notifierWrapper.DB.Begin()
	if err != nil {
		return nil, err
	}
	return newTransactionLongPollNotifier(tx, notifierWrapper.Sync), nil
}

func (notifier *transactionLongPollNotifier) Create(resource *schema.Resource) error {
	if err := notifier.Transaction.Create(resource); err != nil {
		return err
	}
	notifier.resourcePath = resource.Path()
	return nil
}

func (notifier *transactionLongPollNotifier) Update(resource *schema.Resource) error {
	if err := notifier.Transaction.Update(resource); err != nil {
		return err
	}
	notifier.resourcePath = resource.Path()
	return nil
}

func (notifier *transactionLongPollNotifier) Delete(s *schema.Schema, resourceID interface{}) error {
	resource, err := notifier.Fetch(s, transaction.IDFilter(resourceID))
	if err != nil {
		return err
	}
	if err := notifier.Transaction.Delete(s, resourceID); err != nil {
		return err
	}
	notifier.resourcePath = resource.Path()
	return nil
}

func (notifier *transactionLongPollNotifier) Commit() error {
	if err := notifier.Transaction.Commit(); err != nil {
		return err
	}
	if err := addLongPollNotificationEntry(notifier.resourcePath, notifier.sync); err != nil {
		return err
	}
	return nil
}

func addLongPollNotificationEntry(fullKey string, sync gohan_sync.Sync) error {
	postfix := strings.TrimPrefix(fullKey, statePrefix)
	if postfix == "" {
		return nil
	}
	path := longPollPrefix + postfix
	log.Critical("addLongPollNotificationEntry path = %s", path)
	if err := sync.UpdateTTL(path, "dummy", longPollNotificationTTL); err != nil {
		return err
	}
	return nil
}
