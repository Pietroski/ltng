package chainded_operator

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	chained_mock "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator/mocks"
)

func TestChainOperator_Operate(t *testing.T) {
	tests := []struct {
		name   string
		ops    func() *Ops
		assert func(t *testing.T, err error)
	}{
		{
			name: "nil ops",
			ops: func() *Ops {
				return nil
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "nil ops action",
			ops: func() *Ops {
				return &Ops{
					Action:         nil,
					RollbackAction: nil,
				}
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "happy action path",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					return nil
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					return nil
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					return nil
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					return nil
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					return nil
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "rollback from third chained action",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					return nil
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					return nil
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					return nil
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					log.Println("returning error from actionFunc3 call")
					return fmt.Errorf("any-error-to-force-rollback")
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					return nil
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "rollback from second chained action",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					return nil
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					log.Println("returning error from actionFunc2 call")
					return fmt.Errorf("any-error-to-force-rollback")
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					return nil
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					return nil
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					return nil
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "rollback from first chained action",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					log.Println("returning error from actionFunc1 call")
					return fmt.Errorf("any-error-to-force-rollback")
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					return nil
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					return nil
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					return nil
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					return nil
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "error on first rollback chained action",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					return nil
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					return nil
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					return nil
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					log.Println("returning error from actionFunc1 call")
					return fmt.Errorf("any-error-to-force-rollback")
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					log.Println("returning error from rollbackActionFunc2 call")
					return fmt.Errorf("any-error-on-rollback")
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "error on second rollback chained action",
			ops: func() *Ops {
				var actionFunc1 = func() error {
					log.Println("actionFunc1 call")
					return nil
				}

				var actionFunc2 = func() error {
					log.Println("actionFunc2 call")
					return nil
				}
				var rollbackActionFunc1 = func() error {
					log.Println("rollbackActionFunc1 call")
					log.Println("returning error from rollbackActionFunc1 call")
					return fmt.Errorf("any-error-on-rollback")
				}

				var actionFunc3 = func() error {
					log.Println("actionFunc3 call")
					log.Println("returning error from actionFunc1 call")
					return fmt.Errorf("any-error-to-force-rollback")
				}
				var rollbackActionFunc2 = func() error {
					log.Println("rollbackActionFunc2 call")
					return nil
				}

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         actionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         actionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: rollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         actionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				t.Log(err)
			},
		},
		{
			name: "happy path - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(0).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "rollback from third chained action - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "rollback from second chained action - with retry policy",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().ActionFunc3().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "rollback from first chained action - with retry policy",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().ActionFunc2().Times(0).Return(nil)
				callers.EXPECT().ActionFunc3().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(0).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "error on first rollback chained action - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(fmt.Errorf("any-error-on-rollback"))
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "error on second rollback chained action - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(fmt.Errorf("any-error-on-rollback"))

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "error on second chained action - with retry policy - success on second attempt - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(0).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "error on third chained action - with retry policy - success on second attempt - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(1).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().ActionFunc3().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(0).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "error on third chained action - with retry policy - failure on second attempt - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(3).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "error on third chained action - with retry policy - success on last attempt - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(2).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().ActionFunc3().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc2().Times(0).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(0).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "error on first rollback chained action - with retry policy - failure in all attempts - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(3).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(2).Return(fmt.Errorf("any-error-on-rollback"))
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 1,
						},
						Next: nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
		{
			name: "error on first rollback chained action - with retry policy - success on the last attempt - with mocked interface check",
			ops: func() *Ops {
				ctrl := gomock.NewController(t)
				callers := chained_mock.NewMockCallers(ctrl)

				callers.EXPECT().ActionFunc1().Times(1).Return(nil)
				callers.EXPECT().ActionFunc2().Times(1).Return(nil)
				callers.EXPECT().ActionFunc3().Times(3).Return(fmt.Errorf("any-error-to-force-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(fmt.Errorf("any-error-on-rollback"))
				callers.EXPECT().RollbackActionFunc2().Times(1).Return(nil)
				callers.EXPECT().RollbackActionFunc1().Times(1).Return(nil)

				actionFunc3Ops := &Ops{
					Action: &Action{
						Act: callers.ActionFunc3,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 2,
						},
						Next: nil,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc2,
						RetrialOpts: &RetrialOpts{
							RetrialOnErr: true,
							RetrialCount: 1,
						},
						Next: nil,
					},
				}

				actionFunc2Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc2,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc3Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: callers.RollbackActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc3Ops.RollbackAction.Next = actionFunc2Ops

				actionFunc1Ops := &Ops{
					Action: &Action{
						Act:         callers.ActionFunc1,
						RetrialOpts: &RetrialOpts{},
						Next:        actionFunc2Ops,
					},
					RollbackAction: &RollbackAction{
						RollbackAct: nil,
						RetrialOpts: &RetrialOpts{},
						Next:        nil,
					},
				}
				actionFunc2Ops.RollbackAction.Next = actionFunc1Ops

				return actionFunc1Ops
			},
			assert: func(t *testing.T, err error) {
				require.Error(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &ChainOperator{}

			err := o.Operate(tt.ops())
			tt.assert(t, err)
		})
	}
}

func TestNewChainOperator(t *testing.T) {
	tests := []struct {
		name string
		want *ChainOperator
	}{
		{
			name: "happy path",
			want: NewChainOperator(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewChainOperator(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewChainOperator() = %v, want %v", got, tt.want)
			}
		})
	}
}
