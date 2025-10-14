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

//go:build !skip_js_tests

package server

import (
	"testing"
)

func BenchmarkJsHdrSliceHeader(b *testing.B) {
	hdr := genHeader(nil, JSBatchId, "uuid")
	for range b.N {
		for i := 0; i < 10; i++ {
			sliceHeader(JSBatchId, hdr)
		}
	}
}

func BenchmarkJsHdrIndexStruct(b *testing.B) {
	hdr := genHeader(nil, JSBatchId, "uuid")
	for range b.N {
		idx := indexJsHdr(hdr)
		for i := 0; i < 10; i++ {
			idx.get(JSBatchId, hdr)
		}
	}
}

func BenchmarkJsHdrIndexStructNone(b *testing.B) {
	hdr := genHeader(nil, "Xats-Batch-Id", "uuid")
	for range b.N {
		idx := indexJsHdr(hdr)
		for i := 0; i < 10; i++ {
			idx.get(JSBatchId, hdr)
		}
	}
}

func BenchmarkJsHdrIndexStructPointer(b *testing.B) {
	hdr := genHeader(nil, JSBatchId, "uuid")
	for range b.N {
		idx := indexJsHdrPointer(hdr)
		for i := 0; i < 10; i++ {
			idx.getPointer(JSBatchId, hdr)
		}
		idx.returnToPool()
	}
}

func BenchmarkJsHdrIndexStructPointerNone(b *testing.B) {
	hdr := genHeader(nil, "Xats-Batch-Id", "uuid")
	for range b.N {
		idx := indexJsHdrPointer(hdr)
		for i := 0; i < 10; i++ {
			idx.getPointer(JSBatchId, hdr)
		}
		idx.returnToPool()
	}
}

func BenchmarkJsHdrIndexMap(b *testing.B) {
	hdr := genHeader(nil, JSBatchId, "uuid")
	for range b.N {
		idx := indexJsHdrMap(hdr)
		for i := 0; i < 10; i++ {
			idx.get(JSBatchId, hdr)
		}
		//idx.returnToPool()
	}
}

func BenchmarkJsHdrIndexMapNone(b *testing.B) {
	hdr := genHeader(nil, "Xats-Batch-Id", "uuid")
	for range b.N {
		idx := indexJsHdrMap(hdr)
		for i := 0; i < 10; i++ {
			idx.get(JSBatchId, hdr)
		}
		//idx.returnToPool()
	}
}
