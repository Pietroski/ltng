package data

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
)

type (
	User struct {
		Username  string
		Password  string
		Email     string
		Name      string
		Surname   string
		Age       uint8
		CreatedAt int64
		UpdatedAt int64
	}
)

type BytesValues struct {
	BsKey, BsValue, SecondaryIndexBs []byte
	Item                             *ltng_engine_v1.Item
}

func GenerateRandomUser[T TestBench](tb T) *User {
	timeNow := time.Now().UTC().Unix()
	user := &User{
		Username:  go_random.RandomStringWithPrefixWithSep(12, "username", "-"),
		Password:  go_random.RandomStringWithPrefixWithSep(12, "password", "-"),
		Email:     go_random.RandomEmail(),
		Name:      go_random.RandomStringWithPrefixWithSep(12, "name", "-"),
		Surname:   go_random.RandomStringWithPrefixWithSep(12, "surname", "-"),
		Age:       uint8(go_random.RandomInt(0, math.MaxUint8)),
		CreatedAt: timeNow,
		UpdatedAt: timeNow,
	}

	return user
}

func GenerateRandomUsers[T TestBench](tb T, n int) []*User {
	users := make([]*User, n)
	for i := 0; i < n; i++ {
		users[i] = GenerateRandomUser(tb)
	}

	return users
}

func GetUserBytesValues[T TestBench](tb T, ts *TestSuite, userData *User) *BytesValues {
	bsKey, err := ts.Serializer.Serialize(userData.Email)
	require.NoError(tb, err)
	require.NotNil(tb, bsKey)
	//tb.Log(string(bsKey))

	bsValue, err := ts.Serializer.Serialize(userData)
	require.NoError(tb, err)
	require.NotNil(tb, bsValue)
	//tb.Log(string(bsValue))

	secondaryIndexBs, err := ts.Serializer.Serialize(userData.Username)
	require.NoError(tb, err)
	require.NotNil(tb, secondaryIndexBs)
	//tb.Log(string(secondaryIndexBs))

	return &BytesValues{
		BsKey:            bsKey,
		BsValue:          bsValue,
		SecondaryIndexBs: secondaryIndexBs,
		Item: &ltng_engine_v1.Item{
			Key:   bsKey,
			Value: bsValue,
		},
	}
}

func DeserializeUser(t *testing.T, ts *TestSuite, rawData []byte) *User {
	var user User
	err := ts.Serializer.Deserialize(rawData, &user)
	require.NoError(t, err)
	t.Log(user)

	return &user
}
