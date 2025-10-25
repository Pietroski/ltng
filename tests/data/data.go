package data

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/random"

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
		Username:  random.StringWithPrefixWithSep(12, "username", "-"),
		Password:  random.StringWithPrefixWithSep(12, "password", "-"),
		Email:     random.Email(),
		Name:      random.StringWithPrefixWithSep(12, "name", "-"),
		Surname:   random.StringWithPrefixWithSep(12, "surname", "-"),
		Age:       uint8(random.Int(0, math.MaxUint8)),
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
