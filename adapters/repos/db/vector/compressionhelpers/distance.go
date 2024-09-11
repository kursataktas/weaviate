//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var LAQDotImpl func(x []float32, y []byte) float32 = func(x []float32, y []byte) float32 {
	sum := float32(0)
	for i := range x {
		sum += x[i] * float32(y[i])
	}

	return sum
}

var LAQDotExpImpl func(x []float32, y1, y2 []byte, a1, a2 float32) float32 = func(x []float32, y1, y2 []byte, a1, a2 float32) float32 {
	sum := float32(0)
	for i := range x {
		sum += x[i] * (a1*float32(y1[i]) + a2*float32(y2[i]))
	}

	return sum
}
