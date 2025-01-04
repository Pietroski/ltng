package memorystorev1

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializermodels "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

type (
	testSuite struct {
		ctx         context.Context
		cancel      context.CancelFunc
		users       []*User
		cacheEngine *LTNGCacheEngine
		testsuite   *TestSuite
	}
)

func setupTestSuite[T TestBench](tb T) *testSuite {
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	ts := &testSuite{
		ctx:         ctx,
		cancel:      cancel,
		users:       GenerateRandomUsers(tb, 50),
		cacheEngine: New(ctx),
		testsuite: &TestSuite{
			Ctx:        ctx,
			Serializer: serializer.NewRawBinarySerializer(),
		},
	}
	return ts
}

type TestSuite struct {
	Ctx        context.Context
	Serializer serializermodels.Serializer
}

type TestBench interface {
	*testing.T | *testing.B | *require.TestingT | *assert.TestingT

	Cleanup(f func())
	Errorf(format string, args ...interface{})
	FailNow()
	Logf(format string, args ...interface{})
	Log(args ...interface{})
}

type (
	User struct {
		Username  string
		Password  string
		Email     string
		Name      string
		Surname   string
		Age       uint8
		RandomKey string
		CreatedAt int64
		UpdatedAt int64
	}
)

type BytesValues struct {
	BsKey, BsValue, SecondaryIndexBs, TertiaryIndexBs []byte
	Item                                              *ltngenginemodels.Item
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
		RandomKey: go_random.RandomString(10),
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

	tertiaryIndexBs, err := ts.Serializer.Serialize(userData.RandomKey)
	require.NoError(tb, err)
	require.NotNil(tb, tertiaryIndexBs)
	//tb.Log(string(tertiaryIndexBs))

	return &BytesValues{
		BsKey:            bsKey,
		BsValue:          bsValue,
		SecondaryIndexBs: secondaryIndexBs,
		TertiaryIndexBs:  tertiaryIndexBs,
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
