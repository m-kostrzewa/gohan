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

	"github.com/cloudwan/gohan/server/middleware"
)

// MessageDispatch implements a thread-safe pub-sub mechanism. Subs wait on specified keys to be broadcasted on.
type MessageDispatch struct {
	groups map[string]*sync.Cond
	mutex  sync.Mutex
}

// NewNamedCond returns a new MessageDispatch object.
func NewNamedCond() *MessageDispatch {
	log.Info("[NamedCond] created")
	md := MessageDispatch{}
	md.groups = make(map[string]*sync.Cond)
	return &md
}

// Wait waits for specified key to be signaled.
func (md *MessageDispatch) Wait(key string) {
	md.mutex.Lock()
	defer md.mutex.Unlock()

	md.waitLocked(key)
}

func (md *MessageDispatch) waitLocked(key string) {
	normalizedKey := normalizeKey(key)
	log.Debug("[NamedCond] waiting for %s as %s", key, normalizedKey)

	cond, ok := md.groups[normalizedKey]
	if !ok {
		cond = sync.NewCond(&md.mutex)
		md.groups[normalizedKey] = cond
	}

	cond.Wait()
}

// GetOrWait compares hash of a resource with oldHash. If hashes match, will wait for corresponding resource path to be signaled. Otherwise, will return newly calculated hash immediately.
func (md *MessageDispatch) GetOrWait(key string, oldHash string, context middleware.Context, getResource func(middleware.Context) error, getHash func(middleware.Context) string) (string, error) {
	md.mutex.Lock()

	if err := getResource(context); err != nil {
		md.mutex.Unlock()
		return "", err
	}

	hash := getHash(context)
	if hash != oldHash {
		md.mutex.Unlock()
		return hash, nil
	}

	defer md.mutex.Unlock()
	md.waitLocked(key)

	delete(context, "response")
	if err := getResource(context); err != nil {
		return "", err
	}

	return getHash(context), nil
}

// Broadcast signals all subs waiting for a specified key and cleans up.
func (md *MessageDispatch) Broadcast(key string) {
	log.Debug("[NamedCond] broadcasting %s", key)

	md.mutex.Lock()
	defer md.mutex.Unlock()

	for _, parent := range getParentKeys(key) {
		cond, ok := md.groups[parent]
		if ok {
			cond.Broadcast()
			delete(md.groups, parent)
		}
	}
}

// Close broadcasts on all keys and cleans up.
func (md *MessageDispatch) Close() {
	md.mutex.Lock()
	defer md.mutex.Unlock()

	for _, cond := range md.groups {
		cond.Broadcast()
	}

	log.Info("[long_polling] NamedCond closed")
}

func getParentKeys(key string) []string {
	keyParts := strings.Split(key, "/") // /key/subkey/subsubkey

	var parentKeys []string
	for i := 1; i < len(keyParts); i++ {
		parentKeys = append(parentKeys, strings.Join(keyParts[:i+1], "/"))
	}

	return parentKeys
}

func normalizeKey(key string) string {
	keyParts := strings.Split(key, "/")
	normalizedKey := ""
	for _, part := range keyParts {
		if len(part) == 0 {
			continue
		}
		normalizedKey += "/" + part
	}
	return normalizedKey
}
