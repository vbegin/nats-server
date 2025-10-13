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
