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

package compress

import "github.com/minio/minlz"

// MinLZCompressionLevel is minlz.LevelSuperFast, LevelFastest, LevelBalanced
// or LevelSmallest.
var MinLZCompressionLevel = minlz.LevelBalanced

// MinLZDecompress decompresses a block using minlz algorithm.
func MinLZDecompress(dst, src []byte) ([]byte, error) {
	return minlz.Decode(dst, src)
}

// MinLZCompress compresses a block (up to minlz.MaxBlockSize) using minlz
// algorithm.
func MinLZCompress(dst, src []byte, level int) ([]byte, error) {
	return minlz.Encode(dst, src, level)
}
