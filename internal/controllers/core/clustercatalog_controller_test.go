package core

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	catalogdv1alpha1 "github.com/operator-framework/catalogd/api/core/v1alpha1"
	catalogdv1alpha1Errors "github.com/operator-framework/catalogd/internal/errors"
	"github.com/operator-framework/catalogd/internal/source"
	"github.com/operator-framework/catalogd/internal/storage"
)

var _ source.Unpacker = &MockSource{}

// MockSource is a utility for mocking out an Unpacker source
type MockSource struct {
	// result is the result that should be returned when MockSource.Unpack is called
	result *source.Result

	// shouldError determines whether or not the MockSource should return an error when MockSource.Unpack is called
	shouldError bool

	// shouldErrorUnrecoverble determines whether or not the MockSource should return an error when MockSource.Unpack is called
	// is Unrecoverable
	shouldErrorUnrecoverable bool
}

func (ms *MockSource) Unpack(_ context.Context, _ *catalogdv1alpha1.ClusterCatalog) (*source.Result, error) {
	if ms.shouldErrorUnrecoverable {
		return nil, catalogdv1alpha1Errors.NewUnrecoverable(errors.New("mocksource Unrecoverable error"))
	}
	if ms.shouldError {
		return nil, errors.New("mocksource error")
	}

	return ms.result, nil
}

func (ms *MockSource) Cleanup(_ context.Context, _ *catalogdv1alpha1.ClusterCatalog) error {
	if ms.shouldError {
		return errors.New("mocksource error")
	}

	return nil
}

var _ storage.Instance = &MockStore{}

type MockStore struct {
	shouldError bool
}

func (m MockStore) Store(_ context.Context, _ string, _ fs.FS) error {
	if m.shouldError {
		return errors.New("mockstore store error")
	}
	return nil
}

func (m MockStore) Delete(_ string) error {
	if m.shouldError {
		return errors.New("mockstore delete error")
	}
	return nil
}

func (m MockStore) ContentURL(_ string) string {
	return "URL"
}

func (m MockStore) StorageServerHandler() http.Handler {
	panic("not needed")
}

func (m MockStore) ContentExists(_ string) bool {
	return true
}

func TestCatalogdControllerReconcile(t *testing.T) {
	for _, tt := range []struct {
		name            string
		catalog         *catalogdv1alpha1.ClusterCatalog
		shouldErr       bool
		expectedCatalog *catalogdv1alpha1.ClusterCatalog
		source          source.Unpacker
		store           storage.Instance
	}{
		{
			name:   "invalid source type, returns error",
			source: &MockSource{},
			store:  &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "invalid",
					},
				},
			},
			shouldErr: true,
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "invalid",
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonRetrying,
						},
					},
				},
			},
		},
		{
			name:      "valid source type, unpack returns error, status updated to reflect error state and error is returned",
			shouldErr: true,
			source: &MockSource{
				shouldError: true,
			},
			store: &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonRetrying,
						},
					},
				},
			},
		},
		{
			name:      "valid source type, unpack returns unrecoverable error, status updated to reflect unrecoverable error state and error is returned",
			shouldErr: true,
			source: &MockSource{
				shouldErrorUnrecoverable: true,
			},
			store: &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonUnrecoverable,
						},
					},
				},
			},
		},
		{
			name: "valid source type, unpack state == Unpacked, should reflect in status that it's not progressing anymore, and is serving",
			source: &MockSource{
				result: &source.Result{
					State: source.StateUnpacked,
					FS:    &fstest.MapFS{},
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref: "someimage@someSHA256Digest",
						},
					},
				},
			},
			store: &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonSucceeded,
						},
					},
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref: "someimage@someSHA256Digest",
						},
					},
				},
			},
		},
		{
			name:      "valid source type, unpack state == Unpacked, storage fails, failure reflected in status and error returned",
			shouldErr: true,
			source: &MockSource{
				result: &source.Result{
					State: source.StateUnpacked,
					FS:    &fstest.MapFS{},
				},
			},
			store: &MockStore{
				shouldError: true,
			},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonRetrying,
						},
					},
				},
			},
		},
		{
			name:   "storage finalizer not set, storage finalizer gets set",
			source: &MockSource{},
			store:  &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name: "catalog",
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
		},
		{
			name:   "storage finalizer set, catalog deletion timestamp is not zero (or nil), finalizer removed, serving status condition is set to false",
			source: &MockSource{},
			store:  &MockStore{},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "catalog",
					Finalizers:        []string{fbcDeletionFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Date(2023, time.October, 10, 4, 19, 0, 0, time.UTC)},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "catalog",
					Finalizers:        []string{},
					DeletionTimestamp: &metav1.Time{Time: time.Date(2023, time.October, 10, 4, 19, 0, 0, time.UTC)},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonUnavailable,
						},
					},
				},
			},
		},
		{
			name:      "storage finalizer set, catalog deletion timestamp is not zero (or nil), storage delete failed, error returned and finalizer not removed",
			shouldErr: true,
			source:    &MockSource{},
			store: &MockStore{
				shouldError: true,
			},
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "catalog",
					Finalizers:        []string{fbcDeletionFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Date(2023, time.October, 10, 4, 19, 0, 0, time.UTC)},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedCatalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "catalog",
					Finalizers:        []string{fbcDeletionFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Date(2023, time.October, 10, 4, 19, 0, 0, time.UTC)},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonRetrying,
						},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &ClusterCatalogReconciler{
				Client: nil,
				Unpacker: source.NewUnpacker(
					map[catalogdv1alpha1.SourceType]source.Unpacker{
						catalogdv1alpha1.SourceTypeImage: tt.source,
					},
				),
				Storage: tt.store,
			}
			ctx := context.Background()
			res, err := reconciler.reconcile(ctx, tt.catalog)
			assert.Equal(t, ctrl.Result{}, res)

			if !tt.shouldErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			diff := cmp.Diff(tt.expectedCatalog, tt.catalog,
				cmpopts.IgnoreFields(metav1.Condition{}, "Message", "LastTransitionTime"),
				cmpopts.SortSlices(func(a, b metav1.Condition) bool { return a.Type < b.Type }))
			assert.Empty(t, diff, "comparing the expected Catalog")
		})
	}
}

func TestPollingRequeue(t *testing.T) {
	for name, tc := range map[string]struct {
		catalog              *catalogdv1alpha1.ClusterCatalog
		expectedRequeueAfter time.Duration
	}{
		"ClusterCatalog with tag based image ref without any poll interval specified, requeueAfter set to 0, ie polling disabled": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
			},
			expectedRequeueAfter: time.Second * 0,
		},
		"ClusterCatalog with tag based image ref with poll interval specified, requeueAfter set to wait.jitter(pollInterval)": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 5},
						},
					},
				},
			},
			expectedRequeueAfter: time.Minute * 5,
		},
	} {
		t.Run(name, func(t *testing.T) {
			reconciler := &ClusterCatalogReconciler{
				Client: nil,
				Unpacker: source.NewUnpacker(
					map[catalogdv1alpha1.SourceType]source.Unpacker{
						catalogdv1alpha1.SourceTypeImage: &MockSource{result: &source.Result{
							State: source.StateUnpacked,
							FS:    &fstest.MapFS{},
							ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
								Image: &catalogdv1alpha1.ResolvedImageSource{
									Ref: "someImage@someSHA256Digest",
								},
							},
						}},
					},
				),
				Storage: &MockStore{},
			}
			res, _ := reconciler.reconcile(context.Background(), tc.catalog)
			assert.GreaterOrEqual(t, res.RequeueAfter, tc.expectedRequeueAfter)
			// wait.Jitter used to calculate requeueAfter by the reconciler
			// returns a time.Duration between pollDuration and pollDuration + maxFactor * pollDuration.
			assert.LessOrEqual(t, float64(res.RequeueAfter), float64(tc.expectedRequeueAfter)+(float64(tc.expectedRequeueAfter)*requeueJitterMaxFactor))
		})
	}
}

func TestPollingReconcilerUnpack(t *testing.T) {
	for name, tc := range map[string]struct {
		catalog           *catalogdv1alpha1.ClusterCatalog
		expectedUnpackRun bool
	}{
		"ClusterCatalog being resolved the first time, unpack should run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 5},
						},
					},
				},
			},
			expectedUnpackRun: true,
		},
		"ClusterCatalog not being resolved the first time, no pollInterval mentioned, unpack should not run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
					Generation: 2,
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref: "someimage:latest",
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonSucceeded,
						},
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
					},
					ObservedGeneration: 2,
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref:             "someimage:latest",
							ResolvedRef:     "someimage@sha256:asdf123",
							LastPollAttempt: metav1.Time{Time: time.Now().Add(-time.Minute * 5)},
						},
					},
				},
			},
			expectedUnpackRun: false,
		},
		"ClusterCatalog not being resolved the first time, pollInterval mentioned, \"now\" is before next expected poll time, unpack should not run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
					Generation: 2,
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 7},
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonSucceeded,
						},
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
					},
					ObservedGeneration: 2,
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref:             "someimage:latest",
							ResolvedRef:     "someimage@sha256:asdf123",
							LastPollAttempt: metav1.Time{Time: time.Now().Add(-time.Minute * 5)},
						},
					},
				},
			},
			expectedUnpackRun: false,
		},
		"ClusterCatalog not being resolved the first time, pollInterval mentioned, \"now\" is after next expected poll time, unpack should run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
					Generation: 2,
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 3},
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonSucceeded,
						},
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
					},
					ObservedGeneration: 2,
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref:             "someimage:latest",
							ResolvedRef:     "someimage@sha256:asdf123",
							LastPollAttempt: metav1.Time{Time: time.Now().Add(-time.Minute * 5)},
						},
					},
				},
			},
			expectedUnpackRun: true,
		},
		"ClusterCatalog not being resolved the first time, pollInterval mentioned, \"now\" is before next expected poll time, spec.image changed, unpack should run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
					Generation: 3,
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someotherimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 7},
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionFalse,
							Reason: catalogdv1alpha1.ReasonSucceeded,
						},
						{
							Type:   catalogdv1alpha1.TypeServing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonAvailable,
						},
					},
					ObservedGeneration: 2,
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref:             "someimage:latest",
							ResolvedRef:     "someimage@sha256:asdf123",
							LastPollAttempt: metav1.Time{Time: time.Now().Add(-time.Minute * 5)},
						},
					},
				},
			},
			expectedUnpackRun: true,
		},
		"ClusterCatalog not being resolved the first time, condition Serving missing, unpack should run": {
			catalog: &catalogdv1alpha1.ClusterCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-catalog",
					Finalizers: []string{fbcDeletionFinalizer},
					Generation: 3,
				},
				Spec: catalogdv1alpha1.ClusterCatalogSpec{
					Source: catalogdv1alpha1.CatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ImageSource{
							Ref:          "someotherimage:latest",
							PollInterval: &metav1.Duration{Duration: time.Minute * 7},
						},
					},
				},
				Status: catalogdv1alpha1.ClusterCatalogStatus{
					ContentURL: "URL",
					Conditions: []metav1.Condition{
						{
							Type:   catalogdv1alpha1.TypeProgressing,
							Status: metav1.ConditionTrue,
							Reason: catalogdv1alpha1.ReasonRetrying,
						},
					},
					ObservedGeneration: 2,
					ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
						Type: "image",
						Image: &catalogdv1alpha1.ResolvedImageSource{
							Ref:             "someimage:latest",
							ResolvedRef:     "someimage@sha256:asdf123",
							LastPollAttempt: metav1.Time{Time: time.Now().Add(-time.Minute * 5)},
						},
					},
				},
			},
			expectedUnpackRun: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			reconciler := &ClusterCatalogReconciler{
				Client:   nil,
				Unpacker: &MockSource{shouldError: true},
				Storage:  &MockStore{},
			}
			_, err := reconciler.reconcile(context.Background(), tc.catalog)
			if tc.expectedUnpackRun {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
