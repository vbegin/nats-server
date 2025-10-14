// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"sync"
	"unique"
)

type JsHdrIndex struct {
	msgId              JsHdrIndexRange
	expStream          JsHdrIndexRange
	expLastSeq         JsHdrIndexRange
	expLastSubjSeq     JsHdrIndexRange
	expLastSubjSeqSubj JsHdrIndexRange
	expLastMsgId       JsHdrIndexRange
	rollup             JsHdrIndexRange
	ttl                JsHdrIndexRange
	incr               JsHdrIndexRange
	batchId            JsHdrIndexRange
	batchSeq           JsHdrIndexRange
	batchCommit        JsHdrIndexRange
	schedPattern       JsHdrIndexRange
	schedTtl           JsHdrIndexRange
	schedTarget        JsHdrIndexRange
}

var hdrIndexPool sync.Pool

func getJsHdrIndexFromPool() *JsHdrIndex {
	idx := hdrIndexPool.Get()
	if idx != nil {
		return idx.(*JsHdrIndex)
	}
	return new(JsHdrIndex)
}

func (idx *JsHdrIndex) returnToPool() {
	if idx == nil {
		return
	}
	idx.msgId.start, idx.msgId.end = 0, 0
	idx.expStream.start, idx.expStream.end = 0, 0
	idx.expLastSeq.start, idx.expLastSeq.end = 0, 0
	idx.expLastSubjSeq.start, idx.expLastSubjSeq.end = 0, 0
	idx.expLastSubjSeqSubj.start, idx.expLastSubjSeqSubj.end = 0, 0
	idx.expLastMsgId.start, idx.expLastMsgId.end = 0, 0
	idx.rollup.start, idx.rollup.end = 0, 0
	idx.ttl.start, idx.ttl.end = 0, 0
	idx.incr.start, idx.incr.end = 0, 0
	idx.batchId.start, idx.batchId.end = 0, 0
	idx.batchSeq.start, idx.batchSeq.end = 0, 0
	idx.batchCommit.start, idx.batchCommit.end = 0, 0
	idx.schedPattern.start, idx.schedPattern.end = 0, 0
	idx.schedTtl.start, idx.schedTtl.end = 0, 0
	idx.schedTarget.start, idx.schedTarget.end = 0, 0
	hdrIndexPool.Put(idx)
}

var hdrKeysMu sync.Mutex
var hdrKeys map[string]string

var hdrIndexMapPool sync.Pool

func getJsHdrIndexMapFromPool() JsHdrIndexMap {
	idx := hdrIndexMapPool.Get()
	if idx != nil {
		return idx.(JsHdrIndexMap)
	}
	return make(JsHdrIndexMap, 1)
}

func (idx JsHdrIndexMap) returnToPool() {
	if idx == nil {
		return
	}
	clear(idx)
	hdrIndexMapPool.Put(idx)
}

type JsHdrIndexMap map[string]JsHdrIndexRange

type JsHdrIndexRange struct {
	start uint32
	end   uint32
}

func indexJsHdr(hdr []byte) (idx JsHdrIndex) {
	hdrLen := len(hdr)
	if hdrLen == 0 || !bytes.HasPrefix(hdr, []byte(hdrLine)) || !bytes.HasSuffix(hdr, []byte(CR_LF)) {
		return idx
	}
	offset := len(hdrLine)
	// While contains more than just CRLF.
	for offset+2 < hdrLen {
		colon := bytes.IndexByte(hdr[offset:], ':')
		if colon < 0 {
			colon = 0
		}
		end := bytes.Index(hdr[offset+colon:], []byte(CR_LF))
		if colon == 0 {
			offset += colon + end + 2 // CRLF length
			continue
		}
		key := hdr[offset : offset+colon]
		if !bytes.HasPrefix(key, []byte("Nats-")) {
			offset += colon + end + 2 // CRLF length
			continue
		}
		valueStart := offset + colon + 1 // ':' length
		// Skip over whitespace before the value.
		for valueStart < hdrLen && hdr[valueStart] == ' ' {
			valueStart++
		}
		var r JsHdrIndexRange
		r.start = uint32(valueStart)
		r.end = uint32(offset + colon + end)
		offset += colon + end + 2 // CRLF length

		switch bytesToString(key) {
		case JSMsgId:
			idx.msgId = r
		case JSExpectedStream:
			idx.expStream = r
		case JSExpectedLastSeq:
			idx.expLastSeq = r
		case JSExpectedLastSubjSeq:
			idx.expLastSubjSeq = r
		case JSExpectedLastSubjSeqSubj:
			idx.expLastSubjSeqSubj = r
		case JSExpectedLastMsgId:
			idx.expLastMsgId = r
		case JSMsgRollup:
			idx.rollup = r
		case JSMessageTTL:
			idx.ttl = r
		case JSMessageIncr:
			idx.incr = r
		case JSBatchId:
			idx.batchId = r
		case JSBatchSeq:
			idx.batchSeq = r
		case JSBatchCommit:
			idx.batchCommit = r
		case JSSchedulePattern:
			idx.schedPattern = r
		case JSScheduleTTL:
			idx.schedTtl = r
		case JSScheduleTarget:
			idx.schedTarget = r
		}
	}
	return idx
}

func (idx JsHdrIndex) get(key string, hdr []byte) []byte {
	var r JsHdrIndexRange
	switch key {
	case JSMsgId:
		r = idx.msgId
	case JSExpectedStream:
		r = idx.expStream
	case JSExpectedLastSeq:
		r = idx.expLastSeq
	case JSExpectedLastSubjSeq:
		r = idx.expLastSubjSeq
	case JSExpectedLastSubjSeqSubj:
		r = idx.expLastSubjSeqSubj
	case JSExpectedLastMsgId:
		r = idx.expLastMsgId
	case JSMsgRollup:
		r = idx.rollup
	case JSMessageTTL:
		r = idx.ttl
	case JSMessageIncr:
		r = idx.incr
	case JSBatchId:
		r = idx.batchId
	case JSBatchSeq:
		r = idx.batchSeq
	case JSBatchCommit:
		r = idx.batchCommit
	case JSSchedulePattern:
		r = idx.schedPattern
	case JSScheduleTTL:
		r = idx.schedTtl
	case JSScheduleTarget:
		r = idx.schedTarget
	default:
		return nil
	}
	if r.start == 0 || r.end == 0 {
		return nil
	}
	return hdr[r.start:r.end]
}

func indexJsHdrPointer(hdr []byte) (idx *JsHdrIndex) {
	hdrLen := len(hdr)
	if hdrLen == 0 || !bytes.HasPrefix(hdr, []byte(hdrLine)) || !bytes.HasSuffix(hdr, []byte(CR_LF)) {
		return idx
	}
	offset := len(hdrLine)
	// While contains more than just CRLF.
	for offset+2 < hdrLen {
		colon := bytes.IndexByte(hdr[offset:], ':')
		if colon < 0 {
			colon = 0
		}
		end := bytes.Index(hdr[offset+colon:], []byte(CR_LF))
		if colon == 0 {
			offset += colon + end + 2 // CRLF length
			continue
		}
		key := hdr[offset : offset+colon]
		if !bytes.HasPrefix(key, []byte("Nats-")) {
			offset += colon + end + 2 // CRLF length
			continue
		}
		valueStart := offset + colon + 1 // ':' length
		// Skip over whitespace before the value.
		for valueStart < hdrLen && hdr[valueStart] == ' ' {
			valueStart++
		}
		var r JsHdrIndexRange
		r.start = uint32(valueStart)
		r.end = uint32(offset + colon + end)
		offset += colon + end + 2 // CRLF length

		if idx == nil {
			idx = getJsHdrIndexFromPool()
		}
		switch bytesToString(key) {
		case JSMsgId:
			idx.msgId = r
		case JSExpectedStream:
			idx.expStream = r
		case JSExpectedLastSeq:
			idx.expLastSeq = r
		case JSExpectedLastSubjSeq:
			idx.expLastSubjSeq = r
		case JSExpectedLastSubjSeqSubj:
			idx.expLastSubjSeqSubj = r
		case JSExpectedLastMsgId:
			idx.expLastMsgId = r
		case JSMsgRollup:
			idx.rollup = r
		case JSMessageTTL:
			idx.ttl = r
		case JSMessageIncr:
			idx.incr = r
		case JSBatchId:
			idx.batchId = r
		case JSBatchSeq:
			idx.batchSeq = r
		case JSBatchCommit:
			idx.batchCommit = r
		case JSSchedulePattern:
			idx.schedPattern = r
		case JSScheduleTTL:
			idx.schedTtl = r
		case JSScheduleTarget:
			idx.schedTarget = r
		}
	}
	return idx
}

func (idx *JsHdrIndex) getPointer(key string, hdr []byte) []byte {
	if idx == nil {
		return nil
	}
	var r JsHdrIndexRange
	switch key {
	case JSMsgId:
		r = idx.msgId
	case JSExpectedStream:
		r = idx.expStream
	case JSExpectedLastSeq:
		r = idx.expLastSeq
	case JSExpectedLastSubjSeq:
		r = idx.expLastSubjSeq
	case JSExpectedLastSubjSeqSubj:
		r = idx.expLastSubjSeqSubj
	case JSExpectedLastMsgId:
		r = idx.expLastMsgId
	case JSMsgRollup:
		r = idx.rollup
	case JSMessageTTL:
		r = idx.ttl
	case JSMessageIncr:
		r = idx.incr
	case JSBatchId:
		r = idx.batchId
	case JSBatchSeq:
		r = idx.batchSeq
	case JSBatchCommit:
		r = idx.batchCommit
	case JSSchedulePattern:
		r = idx.schedPattern
	case JSScheduleTTL:
		r = idx.schedTtl
	case JSScheduleTarget:
		r = idx.schedTarget
	default:
		return nil
	}
	if r.start == 0 || r.end == 0 {
		return nil
	}
	return hdr[r.start:r.end]
}

func indexJsHdrMap(hdr []byte) (idx JsHdrIndexMap) {
	hdrLen := len(hdr)
	if hdrLen == 0 || !bytes.HasPrefix(hdr, []byte(hdrLine)) || !bytes.HasSuffix(hdr, []byte(CR_LF)) {
		return idx
	}
	offset := len(hdrLine)
	// While contains more than just CRLF.
	for offset+2 < hdrLen {
		colon := bytes.IndexByte(hdr[offset:], ':')
		if colon < 0 {
			colon = 0
		}
		end := bytes.Index(hdr[offset+colon:], []byte(CR_LF))
		if colon == 0 {
			offset += colon + end + 2 // CRLF length
			continue
		}
		key := hdr[offset : offset+colon]
		if !bytes.HasPrefix(key, []byte("Nats-")) {
			offset += colon + end + 2 // CRLF length
			continue
		}
		valueStart := offset + colon + 1 // ':' length
		// Skip over whitespace before the value.
		for valueStart < hdrLen && hdr[valueStart] == ' ' {
			valueStart++
		}
		var r JsHdrIndexRange
		r.start = uint32(valueStart)
		r.end = uint32(offset + colon + end)
		offset += colon + end + 2 // CRLF length

		h := unique.Make[string](bytesToString(key))
		if idx == nil {
			idx = make(JsHdrIndexMap, 1)
			//idx = getJsHdrIndexMapFromPool()
		}
		idx[h.Value()] = r

		//hdrKeysMu.Lock()
		//hdrKey, ok := hdrKeys[bytesToString(key)]
		//if !ok {
		//	hdrKey = string(key)
		//	if hdrKeys == nil {
		//		hdrKeys = make(map[string]string, 1)
		//	}
		//	hdrKeys[hdrKey] = hdrKey
		//}
		//hdrKeysMu.Unlock()
		//if idx == nil {
		//	idx = make(JsHdrIndexMap, 1)
		//}
		//idx[hdrKey] = r
	}
	return idx
}

func (idx JsHdrIndexMap) get(key string, hdr []byte) []byte {
	if r, ok := idx[key]; !ok {
		return nil
	} else if r.start == 0 || r.end == 0 {
		return nil
	} else {
		return hdr[r.start:r.end]
	}
}
