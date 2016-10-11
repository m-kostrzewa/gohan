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
)

type MessageDispatch struct {
	groups map[string]*sync.Cond
	mutex sync.Mutex
}

func NewNamedCond() *MessageDispatch {
	log.Info("[NamedCond] created")
	nc := MessageDispatch{}
	nc.groups = make(map[string]*sync.Cond)
	return &nc
}

func (nc *MessageDispatch) Wait(key string) {
	normalizedKey := normalizeKey(key)
	log.Debug("[NamedCond] registered %s as %s", key, normalizedKey)

	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	cond, ok := nc.groups[normalizedKey]
	if !ok {
		cond = sync.NewCond(&nc.mutex)
		nc.groups[normalizedKey] = cond
	}

	cond.Wait()
}

func (nc *MessageDispatch) Broadcast(key string) {
	log.Debug("[NamedCond] broadcasting %s", key)

	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	for _, parent := range getParentKeys(key) {
		cond, ok := nc.groups[parent]
		if ok {
			cond.Broadcast()
			delete(nc.groups, parent)
		}
	}

}

func (nc *MessageDispatch) Close() {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	for _, cond := range nc.groups {
		cond.Broadcast()
	}

	log.Info("[long_polling] NamedCond closed")
}


func getParentKeys(key string) []string {
	keyParts := strings.Split(key, "/") // /key/subkey/subsubkey

	parentKeys := make([]string, 0)
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
