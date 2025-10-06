package data

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

type (
	User struct {
		UUID      string
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
	BsKey, BsValue, SecondaryIndexBs, ExtraUpsertIndex []byte
	Item                                               *ltngenginemodels.Item
}

func GenerateRandomUser[T TestBench](tb T) *User {
	newUUID, err := uuid.NewUUID()
	require.NoError(tb, err)

	timeNow := time.Now().UTC().Unix()
	user := &User{
		UUID:      newUUID.String(),
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

	extraUpsertIndexBs, err := ts.Serializer.Serialize(userData.UUID)
	require.NoError(tb, err)
	require.NotNil(tb, extraUpsertIndexBs)
	//tb.Log(string(extraUpsertIndexBs))

	return &BytesValues{
		BsKey:            bsKey,
		BsValue:          bsValue,
		SecondaryIndexBs: secondaryIndexBs,
		ExtraUpsertIndex: extraUpsertIndexBs,
		Item: &ltngenginemodels.Item{
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
