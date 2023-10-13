package go_random

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Constants for all supported currencies
const (
	USD = "USD"
	EUR = "EUR"
	CAD = "CAD"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz"

// Deprecated
//func init() {
//	// rand.Seed(time.Now().UnixNano())
//
//	rand.New(rand.NewSource(time.Now().UnixNano()))
//}

// RandomInt generates a random integer between min and max
func RandomInt(min, max int64) int64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return min + r.Int63n(max-min+1)
}

// RandomString generates a random string of length n
func RandomString(n int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	k := len(alphabet)

	for i := 0; i < n; i++ {
		c := alphabet[r.Intn(k)]
		sb.WriteByte(c)
	}

	return sb.String()
}

// RandomStringWithPrefix generates a random string of length n and a given prefix
func RandomStringWithPrefix(n int, prefix string) string {
	return prefix + RandomString(n)
}

// RandomStringWithPrefixWithSep generates a random string of length n, a given prefix and a separator
func RandomStringWithPrefixWithSep(n int, prefix, sep string) string {
	return prefix + sep + RandomString(n)
}

// RandomOwner generates a random owner name
func RandomOwner() string {
	return RandomString(6)
}

// RandomMoney generates a random amount of money
func RandomMoney() int64 {
	return RandomInt(0, 1000)
}

// RandomCurrency generates a random currency code
func RandomCurrency() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	currencies := []string{USD, EUR, CAD}
	n := len(currencies)
	return currencies[r.Intn(n)]
}

// RandomEmail generates a random email
func RandomEmail() string {
	return fmt.Sprintf(
		"%s@email.com",
		RandomString(int(RandomInt(3, 32))),
	)
}
