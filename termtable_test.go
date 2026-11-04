//  Copyright (c) 2026 The AtomAI.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"strconv"
	"testing"
)

// Terms get consecutive ids in order of first use, are found again
// across growth, and a term that equals the prefix of another is not
// confused with it.
func TestTermTable(t *testing.T) {
	var tt termTable
	if _, ok := tt.find(termHash([]byte("x")), []byte("x")); ok {
		t.Fatal("found in an empty table")
	}
	const n = 10000
	for i := range n {
		term := []byte("t" + strconv.Itoa(i))
		h := termHash(term)
		if _, ok := tt.find(h, term); ok {
			t.Fatalf("%s found before added", term)
		}
		if id := tt.add(h, string(term)); id != uint32(i) {
			t.Fatalf("%s got id %d, want %d", term, id, i)
		}
	}
	if tt.len() != n {
		t.Fatalf("len %d, want %d", tt.len(), n)
	}
	for i := range n {
		term := []byte("t" + strconv.Itoa(i))
		id, ok := tt.find(termHash(term), term)
		if !ok || id != uint32(i) {
			t.Fatalf("%s: id %d ok %v, want %d", term, id, ok, i)
		}
	}
	for _, miss := range []string{"t", "t1000000", "", "t12x"} {
		if _, ok := tt.find(termHash([]byte(miss)), []byte(miss)); ok {
			t.Fatalf("%q found", miss)
		}
	}
	if 4*n > 3*len(tt.slots) {
		t.Fatalf("%d slots for %d terms: over three quarters full", len(tt.slots), n)
	}
}
