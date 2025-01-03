package bytesop

import "encoding/hex"

func BytesListToMap(data [][]byte) map[string]struct{} {
	m := make(map[string]struct{})
	for _, item := range data {
		strKey := hex.EncodeToString(item)
		m[strKey] = struct{}{}
	}

	return m
}

func CalRightDiff(base, ref [][]byte) [][]byte {
	difference := make([][]byte, 0)

	baseMap := BytesListToMap(base)
	//refMap := BytesListToMap(ref)

	for _, key := range ref {
		strKey := hex.EncodeToString(key)
		if _, ok := baseMap[strKey]; !ok {
			difference = append(difference, key)
		}
	}

	return difference
}

// A - B - C
// B - C - D
// D

func CalLeftDiff(base, ref [][]byte) [][]byte {
	difference := make([][]byte, 0)

	//baseMap := BytesListToMap(base)
	refMap := BytesListToMap(ref)

	for _, key := range base {
		strKey := hex.EncodeToString(key)
		if _, ok := refMap[strKey]; !ok {
			difference = append(difference, key)
		}
	}

	return difference
}
