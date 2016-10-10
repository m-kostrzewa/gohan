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
	input       chan string
	output      map[string][]chan string
	outputMutex sync.Mutex
}

func NewMessageDispatch() *MessageDispatch {
	messageDispatch := MessageDispatch{
		make(chan string),
		make(map[string][]chan string),
		sync.Mutex{},
	}

	go messageDispatch.dispatch()

	return &messageDispatch
}

func (md *MessageDispatch) dispatch() {
	log.Info("[long_polling] starting")
	for key := range md.input {
		md.outputMutex.Lock()
		for _, parent := range getParentKeys(key) {
			for _, client := range md.output[parent] {
				client <- key
				close(client)
			}
			md.output[parent] = nil
		}
		md.outputMutex.Unlock()
	}

	md.outputMutex.Lock()
	md.cleanup()
	md.outputMutex.Unlock()
	log.Info("[long_polling] closed")
}

func getParentKeys(key string) []string {
	keyParts := strings.Split(key, "/") // /key/subkey/subsubkey

	parentKeys := make([]string, 0)
	for i := 1; i < len(keyParts); i++ {
		parentKeys = append(parentKeys, strings.Join(keyParts[:i+1], "/"))
	}

	return parentKeys
}

func (md *MessageDispatch) cleanup() {
	log.Debug("[long_polling] cleanup")
	for key, channels := range md.output {
		for _, ch := range channels {
			close(ch)
		}
		log.Debug("[long_polling] channels closed for %s", key)
		md.output[key] = nil
	}
}

func (md *MessageDispatch) Register(key string) (output chan string) {
	normalizedKey := normalizeKey(key)
	output = make(chan string, 1)

	md.outputMutex.Lock()
	md.output[normalizedKey] = append(md.output[normalizedKey], output)
	md.outputMutex.Unlock()

	log.Debug("[long_polling] registered %s as %s", key, normalizedKey)

	return
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

func (md *MessageDispatch) Send(key string) {
	md.input <- key
}

func (md *MessageDispatch) Close() {
	log.Info("[long_polling] close requested")
	close(md.input)
}
