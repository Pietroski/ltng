package list_operator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator/mocks/list_operator_fakes"
)

var testErr = errors.New("test-error")

func TestListOperator_Operate(t *testing.T) {
	testCases := map[string]struct {
		stubs     func(*list_operator_fakes.FakeCallers)
		assertErr func(t assert.TestingT, err error)
	}{
		"success": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(nil)
				callers.ActionFunc3Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.NoError(t, err)
			},
		},
		"success from second action on the first retry": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc1ReturnsOnCall(0, testErr)
				callers.ActionFunc1ReturnsOnCall(1, nil)
				callers.ActionFunc3Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.NoError(t, err)
			},
		},
		"success from second action on the second retry": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc1ReturnsOnCall(0, testErr)
				callers.ActionFunc1ReturnsOnCall(1, testErr)
				callers.ActionFunc1ReturnsOnCall(2, nil)
				callers.ActionFunc3Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.NoError(t, err)
			},
		},
		"failed on last action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(nil)
				callers.ActionFunc3Returns(testErr)
				callers.RollbackActionFunc2Returns(nil)
				callers.RollbackActionFunc1Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on first rollback action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(nil)
				callers.ActionFunc3Returns(testErr)
				callers.RollbackActionFunc2Returns(testErr)
				callers.RollbackActionFunc1Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second rollback action only": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(nil)
				callers.ActionFunc3Returns(testErr)
				callers.RollbackActionFunc2Returns(testErr)
				callers.RollbackActionFunc1Returns(testErr)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second and first rollback action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(nil)
				callers.ActionFunc3Returns(testErr)
				callers.RollbackActionFunc2Returns(nil)
				callers.RollbackActionFunc1Returns(testErr)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(testErr)
				callers.RollbackActionFunc2Returns(nil)
				callers.RollbackActionFunc1Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second action and on first rollback action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(testErr)
				callers.RollbackActionFunc2Returns(testErr)
				callers.RollbackActionFunc1Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second action and on first and second rollback actions": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(testErr)
				callers.RollbackActionFunc2Returns(testErr)
				callers.RollbackActionFunc1Returns(testErr)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on second action and on second rollback action only": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(nil)
				callers.ActionFunc2Returns(testErr)
				callers.RollbackActionFunc2Returns(nil)
				callers.RollbackActionFunc1Returns(testErr)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on first action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(testErr)
				callers.RollbackActionFunc1Returns(nil)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"failed on first action and rollback action": {
			stubs: func(callers *list_operator_fakes.FakeCallers) {
				callers.ActionFunc1Returns(testErr)
				callers.RollbackActionFunc1Returns(testErr)
			},
			assertErr: func(t assert.TestingT, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			st := setupTest(t)
			tc.stubs(st.callers)

			listOperator := New(st.args...)
			require.NotNil(t, listOperator)

			err := listOperator.Operate()
			tc.assertErr(t, err)
		})
	}
}

func TestListOperator_Operate_FaultyActions(t *testing.T) {
	faultyTestCases := map[string]struct {
		lop       func(t *testing.T) *ListOperator
		assertErr func(t *testing.T, err error)
	}{
		"operation error on empty list": {
			lop: func(t *testing.T) *ListOperator {
				listOperator := New()
				require.NotNil(t, listOperator)
				return listOperator
			},
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
			},
		},
		"empty or nil action on operation list": {
			lop: func(t *testing.T) *ListOperator {
				listOperator := New(
					&Operation{
						Action:   nil,
						Rollback: nil,
					})
				require.NotNil(t, listOperator)
				return listOperator
			},
			assertErr: func(t *testing.T, err error) {
				assert.Nil(t, err)
			},
		},
		"empty or nil rollback action list": {
			lop: func(t *testing.T) *ListOperator {
				st := setupTest(t)
				listOperator := New(
					&Operation{
						Action: &Action{
							Act:         st.callers.ActionFunc1,
							RetrialOpts: DefaultRetrialOps,
						},
						Rollback: nil,
					})
				st.callers.ActionFunc1Returns(testErr)
				require.NotNil(t, listOperator)
				return listOperator
			},
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"empty or nil rollback action": {
			lop: func(t *testing.T) *ListOperator {
				st := setupTest(t)
				listOperator := New(
					&Operation{
						Action: &Action{
							Act:         st.callers.ActionFunc1,
							RetrialOpts: DefaultRetrialOps,
						},
						Rollback: &RollbackAction{
							RollbackAct: nil,
							RetrialOpts: nil,
						},
					})
				st.callers.ActionFunc1Returns(testErr)
				require.NotNil(t, listOperator)
				return listOperator
			},
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
		"nil retry options": {
			lop: func(t *testing.T) *ListOperator {
				st := setupTest(t)
				listOperator := New(
					&Operation{
						Action: &Action{
							Act:         st.callers.ActionFunc1,
							RetrialOpts: nil,
						},
						Rollback: nil,
					})
				st.callers.ActionFunc1Returns(testErr)
				require.NotNil(t, listOperator)
				return listOperator
			},
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, testErr)
			},
		},
	}

	for testName, tc := range faultyTestCases {
		t.Run(testName, func(t *testing.T) {
			listOperator := tc.lop(t)
			err := listOperator.Operate()
			tc.assertErr(t, err)
		})
	}
}

func TestNewChainOperator(t *testing.T) {
	t.Run("not nil list operator", func(t *testing.T) {
		args := []*Operation{
			{
				Action: &Action{
					Act: func() error {
						t.Log("first action")
						return nil
					},
					RetrialOpts: DefaultRetrialOps,
				},
				Rollback: nil,
			},
			{
				Action: &Action{
					Act: func() error {
						t.Log("second action")
						return nil
					},
					RetrialOpts: DefaultRetrialOps,
				},
				Rollback: nil,
			},
		}

		listOperator := New(args...)
		if listOperator == nil {
			t.Error("listOperator is nil")
		}
	})
}

type testSuite struct {
	callers *list_operator_fakes.FakeCallers
	args    []*Operation
}

func setupTest(_ *testing.T) *testSuite {
	callers := &list_operator_fakes.FakeCallers{}

	args := []*Operation{
		{
			Action: &Action{
				Act:         callers.ActionFunc1,
				RetrialOpts: DefaultRetrialOps,
			},
			Rollback: &RollbackAction{
				RollbackAct: callers.RollbackActionFunc1,
				RetrialOpts: DefaultRetrialOps,
			},
		},
		{
			Action: &Action{
				Act:         callers.ActionFunc2,
				RetrialOpts: DefaultRetrialOps,
			},
			Rollback: &RollbackAction{
				RollbackAct: callers.RollbackActionFunc2,
				RetrialOpts: DefaultRetrialOps,
			},
		},
		{
			Action: &Action{
				Act:         callers.ActionFunc3,
				RetrialOpts: DefaultRetrialOps,
			},
		},
	}

	ts := &testSuite{
		callers: callers,
		args:    args,
	}

	return ts
}
