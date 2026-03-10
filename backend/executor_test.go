package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/grpc/metadata"
)

// mockBackend is a minimal no-op backend for tests.
type mockBackend struct{}

func (mockBackend) CreateTaskHub(context.Context) error                        { return nil }
func (mockBackend) DeleteTaskHub(context.Context) error                        { return nil }
func (mockBackend) Start(context.Context) error                                { return nil }
func (mockBackend) Stop(context.Context) error                                 { return nil }
func (mockBackend) CreateOrchestrationInstance(context.Context, *protos.HistoryEvent, ...OrchestrationIdReusePolicyOptions) error {
	return nil
}
func (mockBackend) RerunWorkflowFromEvent(context.Context, *protos.RerunWorkflowFromEventRequest) (api.InstanceID, error) {
	return "", nil
}
func (mockBackend) AddNewOrchestrationEvent(context.Context, api.InstanceID, *protos.HistoryEvent) error {
	return nil
}
func (mockBackend) NextOrchestrationWorkItem(context.Context) (*OrchestrationWorkItem, error) {
	return nil, nil
}
func (mockBackend) GetOrchestrationRuntimeState(context.Context, *OrchestrationWorkItem) (*OrchestrationRuntimeState, error) {
	return nil, nil
}
func (mockBackend) WatchOrchestrationRuntimeStatus(context.Context, api.InstanceID, func(*OrchestrationMetadata) bool) error {
	return nil
}
func (mockBackend) GetOrchestrationMetadata(context.Context, api.InstanceID) (*OrchestrationMetadata, error) {
	return nil, nil
}
func (mockBackend) CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (mockBackend) AbandonOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (mockBackend) NextActivityWorkItem(context.Context) (*ActivityWorkItem, error) { return nil, nil }
func (mockBackend) CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error { return nil }
func (mockBackend) AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error  { return nil }
func (mockBackend) PurgeOrchestrationState(context.Context, api.InstanceID, bool) error {
	return nil
}
func (mockBackend) CompleteOrchestratorTask(context.Context, *protos.OrchestratorResponse) error {
	return nil
}
func (mockBackend) CancelOrchestratorTask(context.Context, api.InstanceID) error { return nil }
func (mockBackend) WaitForOrchestratorCompletion(*protos.OrchestratorRequest) func(context.Context) (*protos.OrchestratorResponse, error) {
	return nil
}
func (mockBackend) CompleteActivityTask(context.Context, *protos.ActivityResponse) error { return nil }
func (mockBackend) CancelActivityTask(context.Context, api.InstanceID, int32) error     { return nil }
func (mockBackend) WaitForActivityCompletion(*protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	return nil
}
func (mockBackend) ListInstanceIDs(context.Context, *protos.ListInstanceIDsRequest) (*protos.ListInstanceIDsResponse, error) {
	return nil, nil
}
func (mockBackend) GetInstanceHistory(context.Context, *protos.GetInstanceHistoryRequest) (*protos.GetInstanceHistoryResponse, error) {
	return nil, nil
}

// testLogger is a no-op logger for tests.
type testLogger struct{}

func (testLogger) Debug(v ...any)                 {}
func (testLogger) Debugf(format string, v ...any)  {}
func (testLogger) Info(v ...any)                  {}
func (testLogger) Infof(format string, v ...any)   {}
func (testLogger) Warn(v ...any)                  {}
func (testLogger) Warnf(format string, v ...any)   {}
func (testLogger) Error(v ...any)                 {}
func (testLogger) Errorf(format string, v ...any)  {}

// mockStream implements protos.TaskHubSidecarService_GetWorkItemsServer
type mockStream struct {
	ctx      context.Context
	sentCh   chan *protos.WorkItem
	sendErr  error
}

func (s *mockStream) Send(wi *protos.WorkItem) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sentCh <- wi
	return nil
}
func (s *mockStream) SetHeader(metadata.MD) error  { return nil }
func (s *mockStream) SendHeader(metadata.MD) error  { return nil }
func (s *mockStream) SetTrailer(metadata.MD)        {}
func (s *mockStream) Context() context.Context      { return s.ctx }
func (s *mockStream) SendMsg(m any) error           { return nil }
func (s *mockStream) RecvMsg(m any) error           { return nil }

func TestStreamState_SemForWorkItem(t *testing.T) {
	ss := newStreamState(5, 3)

	actWI := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: &protos.ActivityRequest{},
		},
	}
	orchWI := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{
			OrchestratorRequest: &protos.OrchestratorRequest{},
		},
	}

	if sem := ss.semForWorkItem(actWI); sem != ss.activitySem {
		t.Fatal("expected activity semaphore")
	}
	if sem := ss.semForWorkItem(orchWI); sem != ss.orchestratorSem {
		t.Fatal("expected orchestrator semaphore")
	}
	if cap(ss.activitySem) != 5 {
		t.Fatalf("expected activity sem cap 5, got %d", cap(ss.activitySem))
	}
	if cap(ss.orchestratorSem) != 3 {
		t.Fatalf("expected orchestrator sem cap 3, got %d", cap(ss.orchestratorSem))
	}
}

func TestStreamState_UnlimitedWhenZero(t *testing.T) {
	ss := newStreamState(0, 0)
	if ss.activitySem != nil {
		t.Fatal("expected nil activity semaphore for limit=0")
	}
	if ss.orchestratorSem != nil {
		t.Fatal("expected nil orchestrator semaphore for limit=0")
	}

	actWI := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: &protos.ActivityRequest{},
		},
	}
	if sem := ss.semForWorkItem(actWI); sem != nil {
		t.Fatal("expected nil semaphore for unlimited")
	}
}

func TestStreamState_SignalSlotFreed(t *testing.T) {
	ss := newStreamState(1, 0)

	// Signal should be non-blocking
	ss.signalSlotFreed()
	ss.signalSlotFreed() // second call should not block (buffered 1, already full)

	select {
	case <-ss.slotFreed:
	default:
		t.Fatal("expected signal on slotFreed")
	}
}

func TestReleaseActivitySlot(t *testing.T) {
	g := &grpcExecutor{
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		logger:               testLogger{},
	}

	ss := newStreamState(2, 0)
	streamID := "test-stream"
	g.streams.Store(streamID, ss)

	// Simulate an acquired slot
	ss.activitySem <- struct{}{}

	// Register a pending activity with streamID
	key := GetActivityExecutionKey("instance1", 1)
	g.pendingActivities.Store(key, &pendingActivity{
		instanceID: "instance1",
		taskID:     1,
		streamID:   streamID,
	})

	// Release should drain the semaphore and signal slotFreed
	g.releaseActivitySlot("instance1", 1)

	if len(ss.activitySem) != 0 {
		t.Fatalf("expected sem length 0, got %d", len(ss.activitySem))
	}

	select {
	case <-ss.slotFreed:
	default:
		t.Fatal("expected slotFreed signal after release")
	}
}

func TestReleaseOrchestratorSlot(t *testing.T) {
	g := &grpcExecutor{
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		logger:               testLogger{},
	}

	ss := newStreamState(0, 1)
	streamID := "test-stream"
	g.streams.Store(streamID, ss)

	// Simulate an acquired slot
	ss.orchestratorSem <- struct{}{}

	g.pendingOrchestrators.Store(api.InstanceID("orch1"), &pendingOrchestrator{
		instanceID: "orch1",
		streamID:   streamID,
	})

	g.releaseOrchestratorSlot("orch1")

	if len(ss.orchestratorSem) != 0 {
		t.Fatalf("expected sem length 0, got %d", len(ss.orchestratorSem))
	}
}

func TestReleaseSlot_MissingStream_NoOp(t *testing.T) {
	g := &grpcExecutor{
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		logger:               testLogger{},
	}

	// No stream registered — should not panic
	key := GetActivityExecutionKey("instance1", 1)
	g.pendingActivities.Store(key, &pendingActivity{
		instanceID: "instance1",
		taskID:     1,
		streamID:   "gone-stream",
	})

	g.releaseActivitySlot("instance1", 1) // should be a no-op
}

func TestReleaseSlot_NoPending_NoOp(t *testing.T) {
	g := &grpcExecutor{
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		logger:               testLogger{},
	}

	// No pending activity — should not panic
	g.releaseActivitySlot("nonexistent", 99)
}

func TestGetWorkItems_ActivityConcurrencyLimit(t *testing.T) {
	// This test verifies that with a limit of 2, only 2 activity work items are
	// dispatched to a stream. After completing one, a 3rd is dispatched.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownCh := make(chan any)
	g := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		backend:              mockBackend{},
		logger:               testLogger{},
		streamShutdownChan:   shutdownCh,
	}

	stream := &mockStream{
		ctx:    ctx,
		sentCh: make(chan *protos.WorkItem, 10),
	}

	req := &protos.GetWorkItemsRequest{
		MaxConcurrentActivityWorkItems: 2,
	}

	// Run GetWorkItems in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- g.GetWorkItems(req, stream)
	}()

	// Helper: create activity work item and pre-register pending activity
	makeActivityWI := func(iid string, taskID int32) *protos.WorkItem {
		key := GetActivityExecutionKey(iid, taskID)
		g.pendingActivities.Store(key, &pendingActivity{instanceID: api.InstanceID(iid), taskID: taskID})
		return &protos.WorkItem{
			Request: &protos.WorkItem_ActivityRequest{
				ActivityRequest: &protos.ActivityRequest{
					OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
					TaskId:                taskID,
					Name:                  "TestActivity",
				},
			},
		}
	}

	// Send 3 work items; the workItemQueue is unbuffered so each send blocks until dequeued
	wi1 := makeActivityWI("inst1", 1)
	wi2 := makeActivityWI("inst2", 2)
	wi3 := makeActivityWI("inst3", 3)

	// Send first two — should be dispatched immediately (within limit)
	go func() { g.workItemQueue <- wi1 }()
	go func() { g.workItemQueue <- wi2 }()

	var dispatched int
	timeout := time.After(2 * time.Second)
	for dispatched < 2 {
		select {
		case <-stream.sentCh:
			dispatched++
		case <-timeout:
			t.Fatalf("timed out waiting for work items, got %d/2", dispatched)
		}
	}

	// Send third — should be requeued because limit is 2
	var thirdSent atomic.Bool
	go func() {
		g.workItemQueue <- wi3
		thirdSent.Store(true)
	}()

	// Give it a moment; the 3rd item should NOT be dispatched yet
	time.Sleep(200 * time.Millisecond)
	select {
	case <-stream.sentCh:
		t.Fatal("3rd work item should not have been dispatched yet")
	default:
		// good
	}

	// Complete one activity — this releases a slot
	// Find the streamID assigned to one of the pending activities
	if value, ok := g.pendingActivities.Load(GetActivityExecutionKey("inst1", 1)); ok {
		p := value.(*pendingActivity)
		if p.streamID != "" {
			if ssVal, ok := g.streams.Load(p.streamID); ok {
				ss := ssVal.(*streamState)
				<-ss.activitySem
				ss.signalSlotFreed()
			}
		}
	}

	// Now the 3rd item should be dispatched
	select {
	case <-stream.sentCh:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for 3rd work item after releasing a slot")
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("GetWorkItems did not return after context cancellation")
	}
}

func TestGetWorkItems_UnlimitedConcurrency(t *testing.T) {
	// With limit=0, all items should be dispatched without blocking.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownCh := make(chan any)
	g := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		backend:              mockBackend{},
		logger:               testLogger{},
		streamShutdownChan:   shutdownCh,
	}

	stream := &mockStream{
		ctx:    ctx,
		sentCh: make(chan *protos.WorkItem, 10),
	}

	req := &protos.GetWorkItemsRequest{
		MaxConcurrentActivityWorkItems: 0, // unlimited
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.GetWorkItems(req, stream)
	}()

	// Send 5 items rapidly — all should be dispatched
	for i := int32(0); i < 5; i++ {
		iid := "inst"
		key := GetActivityExecutionKey(iid, i)
		g.pendingActivities.Store(key, &pendingActivity{instanceID: api.InstanceID(iid), taskID: i})

		wi := &protos.WorkItem{
			Request: &protos.WorkItem_ActivityRequest{
				ActivityRequest: &protos.ActivityRequest{
					OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
					TaskId:                i,
					Name:                  "TestActivity",
				},
			},
		}
		go func() { g.workItemQueue <- wi }()
	}

	var dispatched int
	timeout := time.After(2 * time.Second)
	for dispatched < 5 {
		select {
		case <-stream.sentCh:
			dispatched++
		case <-timeout:
			t.Fatalf("timed out, only got %d/5 work items", dispatched)
		}
	}

	cancel()
	<-errCh
}

func TestGetWorkItems_StreamDisconnect_CleansUpStreamState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	shutdownCh := make(chan any)
	g := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		streams:              &sync.Map{},
		backend:              mockBackend{},
		logger:               testLogger{},
		streamShutdownChan:   shutdownCh,
	}

	stream := &mockStream{
		ctx:    ctx,
		sentCh: make(chan *protos.WorkItem, 10),
	}

	req := &protos.GetWorkItemsRequest{
		MaxConcurrentActivityWorkItems: 2,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.GetWorkItems(req, stream)
	}()

	// Send one work item so that the stream is registered
	key := GetActivityExecutionKey("inst1", 1)
	g.pendingActivities.Store(key, &pendingActivity{instanceID: "inst1", taskID: 1})
	wi := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: &protos.ActivityRequest{
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "inst1"},
				TaskId:                1,
			},
		},
	}
	go func() { g.workItemQueue <- wi }()

	select {
	case <-stream.sentCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for work item")
	}

	// Verify stream state exists
	var streamCount int
	g.streams.Range(func(_, _ any) bool { streamCount++; return true })
	if streamCount != 1 {
		t.Fatalf("expected 1 stream state, got %d", streamCount)
	}

	// Disconnect the stream
	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("GetWorkItems did not return after disconnect")
	}

	// Verify stream state was cleaned up
	streamCount = 0
	g.streams.Range(func(_, _ any) bool { streamCount++; return true })
	if streamCount != 0 {
		t.Fatalf("expected 0 stream states after disconnect, got %d", streamCount)
	}
}
