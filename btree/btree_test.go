// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btree

import (
	"flag"
	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Int implements the Item interface for integers.
type Int int

// Less returns true if int(a) < int(b).
func (a Int) Less(b bItem) bool {
	return a < b.(Int)
}

// perm returns a random permutation of n Int items in the range [0, n).
func perm(n int) (out []bItem) {
	for _, v := range rand.Perm(n) {
		out = append(out, Int(v))
	}
	return
}

// rang returns an ordered list of Int items in the range [0, n).
func rang(n int) (out []bItem) {
	for i := 0; i < n; i++ {
		out = append(out, Int(i))
	}
	return
}

// rang returns a reversed ordered list of Int items in the range (n, 0].
func rangInv(n int) (out []bItem) {
	for i := n - 1; i >= 0; i-- {
		out = append(out, Int(i))
	}
	return
}

// all extracts all items from a tree in order as a slice.
func all(t *bTree) (out []bItem) {
	t.Ascend(func(a bItem) bool {
		out = append(out, a)
		return true
	})
	return
}

var btreeDegree = flag.Int("degree", 32, "B-Tree degree")

var _ = Describe("bTree", func() {

	It("should insert and delete", func() {
		tr := newTree(*btreeDegree)
		const treeSize = 10000
		for i := 0; i < 10; i++ {
			for _, item := range perm(treeSize) {
				Expect(tr.ReplaceOrInsert(item)).To(BeNil())
			}
			for _, item := range perm(treeSize) {
				Expect(tr.ReplaceOrInsert(item)).NotTo(BeNil())
			}
			Expect(all(tr)).To(Equal(rang(treeSize)))

			for _, item := range perm(treeSize) {
				Expect(tr.Delete(item)).NotTo(BeNil())
			}
			Expect(all(tr)).To(BeEmpty())
		}
	})

	It("should delete min", func() {
		tr := newTree(3)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		for v := tr.DeleteMin(); v != nil; v = tr.DeleteMin() {
			coll = append(coll, v)
		}
		Expect(coll).To(Equal(rang(100)))
	})

	It("should delete max", func() {
		tr := newTree(3)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		for v := tr.DeleteMax(); v != nil; v = tr.DeleteMax() {
			coll = append(coll, v)
		}
		Expect(coll).To(Equal(rangInv(100)))
	})

	It("should ascend range #1", func() {
		tr := newTree(2)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendRange(Int(40), Int(60), func(a bItem) bool {
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[40:60]))
	})

	It("should ascend range #2", func() {
		tr := newTree(2)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendRange(Int(40), Int(60), func(a bItem) bool {
			if a.(Int) > 50 {
				return false
			}
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[40:51]))
	})

	It("should ascend less than #1", func() {
		tr := newTree(*btreeDegree)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendLessThan(Int(60), func(a bItem) bool {
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[:60]))
	})

	It("should ascend less than #2", func() {
		tr := newTree(*btreeDegree)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendLessThan(Int(60), func(a bItem) bool {
			if a.(Int) > 50 {
				return false
			}
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[:51]))
	})

	It("should ascend goe #1", func() {
		tr := newTree(*btreeDegree)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendGreaterOrEqual(Int(40), func(a bItem) bool {
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[40:]))
	})

	It("should ascend goe #3", func() {
		tr := newTree(*btreeDegree)
		for _, v := range perm(100) {
			tr.ReplaceOrInsert(v)
		}

		coll := make([]bItem, 0, 100)
		tr.AscendGreaterOrEqual(Int(40), func(a bItem) bool {
			if a.(Int) > 50 {
				return false
			}
			coll = append(coll, a)
			return true
		})
		Expect(coll).To(Equal(rang(100)[40:51]))
	})

})
