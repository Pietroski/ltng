package random

import (
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"strings"
	"time"
)

// Constants for all supported currencies
const (
	USD = "USD"
	EUR = "EUR"
	CAD = "CAD"
	GBP = "GBP"
	JPY = "JPY"
	KRW = "KRW"
	CNY = "CNY"
	RUB = "RUB"
	BRL = "BRL"
)

const (
	LowerCaseAlphabet = "abcdefghijklmnopqrstuvwxyz"
	UpperCaseAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	NumberChars       = "0123456789"
	SpecialChars      = "!@#$%^&*()-_=+[]{}|;:,.<>?" // "!@#$%^&*()-_=+[]{}|;:'\",.<>/?`~"

	Charset        = LowerCaseAlphabet + UpperCaseAlphabet + NumberChars
	SpecialCharset = Charset + SpecialChars
)

// Int generates a random integer between min and max
func Int(min, max int64) int64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return min + r.Int63n(max-min+1)
}

// String generates a random string of length n
func String(n int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	k := len(Charset)

	for i := 0; i < n; i++ {
		c := Charset[r.Intn(k)]
		sb.WriteByte(c)
	}

	return sb.String()
}

// StringWithPrefix generates a random string of length n and a given prefix
func StringWithPrefix(n int, prefix string) string {
	return prefix + String(n)
}

// StringWithPrefixWithSep generates a random string of length n, a given prefix and a separator
func StringWithPrefixWithSep(n int, prefix, sep string) string {
	return prefix + sep + String(n)
}

// StringWithCustomCharset generates a random string with a custom character set
func StringWithCustomCharset(length int, charset string) string {
	result := make([]byte, length)

	for i := range result {
		result[i] = charset[randv2.IntN(len(charset))]
	}

	return string(result)
}

// StringWithSpecialCharset generates a random string with special character set
func StringWithSpecialCharset(length int) string {
	return StringWithCustomCharset(length, SpecialCharset)
}

// StringWithPrefixForCustomCharset generates a random string of length n and a given prefix
func StringWithPrefixForCustomCharset(n int, prefix string, charset string) string {
	return prefix + StringWithCustomCharset(n, charset)
}

// StringWithPrefixWithSepForCustomCharset generates a random string of length n, a given prefix and a separator
func StringWithPrefixWithSepForCustomCharset(n int, prefix, sep string, charset string) string {
	return prefix + sep + StringWithCustomCharset(n, charset)
}

// Currency generates a random currency code
func Currency() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	currencies := []string{USD, EUR, CAD}
	n := len(currencies)
	return currencies[r.Intn(n)]
}

// Email generates a random email
func Email() string {
	return fmt.Sprintf(
		"%s@email.com",
		String(int(Int(3, 32))),
	)
}
