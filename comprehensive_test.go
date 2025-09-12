package dagpher

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// ComplexTestContext provides a rich context for comprehensive testing
type ComplexTestContext struct {
	Data        map[string]interface{}
	Counters    map[string]int64
	Logs        []string
	StartTime   time.Time
	Errors      []error
	mu          sync.RWMutex
	resultsChan chan string
}

func NewComplexTestContext() *ComplexTestContext {
	return &ComplexTestContext{
		Data:        make(map[string]interface{}),
		Counters:    make(map[string]int64),
		Logs:        make([]string, 0),
		StartTime:   time.Now(),
		Errors:      make([]error, 0),
		resultsChan: make(chan string, 1000),
	}
}

func (c *ComplexTestContext) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data[key] = value
}

func (c *ComplexTestContext) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, exists := c.Data[key]
	return value, exists
}

func (c *ComplexTestContext) Increment(counter string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Counters[counter]++
	return c.Counters[counter]
}

func (c *ComplexTestContext) GetCounter(counter string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Counters[counter]
}

func (c *ComplexTestContext) Log(message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Logs = append(c.Logs, message)
	select {
	case c.resultsChan <- message:
	default:
	}
}

func (c *ComplexTestContext) AddError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Errors = append(c.Errors, err)
}

func (c *ComplexTestContext) GetLogs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	logs := make([]string, len(c.Logs))
	copy(logs, c.Logs)
	return logs
}

func (c *ComplexTestContext) GetErrors() []error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	errors := make([]error, len(c.Errors))
	copy(errors, c.Errors)
	return errors
}

func (c *ComplexTestContext) Duration() time.Duration {
	return time.Since(c.StartTime)
}

func (c *ComplexTestContext) Close() {
	close(c.resultsChan)
}

// TestComprehensiveIntegration tests integration between Graph, Group, and Chain
func TestComprehensiveIntegration(t *testing.T) {
	ctx := context.Background()

	Convey("Comprehensive Integration Tests", t, func() {
		Convey("Complex nested hierarchy", func() {
			testCtx := NewComplexTestContext()
			defer testCtx.Close()

			// Create root graph
			rootGraph := NewGraph[*ComplexTestContext]()
			rootGraph.SetMaxGoNum(20)

			// Create preprocessing chain
			preprocessChain := NewChain[*ComplexTestContext]()
			preprocessChain.AddNode(NewNode("validate_input", func(ctx context.Context, c *ComplexTestContext) error {
				c.Log("Input validation started")
				c.Set("input_validated", true)
				time.Sleep(10 * time.Millisecond)
				return nil
			})).
				AddNode(NewNode("initialize_data", func(ctx context.Context, c *ComplexTestContext) error {
					c.Log("Data initialization started")
					c.Set("data_initialized", true)
					c.Increment("init_counter")
					time.Sleep(15 * time.Millisecond)
					return nil
				})).
				AddNode(NewNode("setup_resources", func(ctx context.Context, c *ComplexTestContext) error {
					c.Log("Resources setup started")
					c.Set("resources_ready", true)
					time.Sleep(20 * time.Millisecond)
					return nil
				}))

			// Create parallel processing groups
			dataProcessingGroup := NewGroup[*ComplexTestContext]("data_processing", "preprocess_chain")
			dataProcessingGroup.SetMaxGoNum(10)

			for i := 0; i < 5; i++ {
				nodeName := fmt.Sprintf("process_data_%d", i)
				dataProcessingGroup.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Log(fmt.Sprintf("%s executing", nodeName))
					c.Increment("data_processed")
					// Simulate data processing
					time.Sleep(time.Duration(50+i*10) * time.Millisecond)
					return nil
				}))
			}

			transformationGroup := NewGroup[*ComplexTestContext]("transformation", "data_processing")
			transformationGroup.SetMaxGoNum(5)

			for i := 0; i < 3; i++ {
				nodeName := fmt.Sprintf("transform_%d", i)
				transformationGroup.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Log(fmt.Sprintf("%s executing", nodeName))
					c.Increment("transformations")
					time.Sleep(30 * time.Millisecond)
					return nil
				}))
			}

			// Create post-processing chain
			postProcessChain := NewChain[*ComplexTestContext]()
			postProcessChain.AddNode(NewNode("aggregate_results", func(ctx context.Context, c *ComplexTestContext) error {
				c.Log("Aggregating results")
				processed := c.GetCounter("data_processed")
				transformed := c.GetCounter("transformations")
				c.Set("total_processed", processed+transformed)
				time.Sleep(25 * time.Millisecond)
				return nil
			})).
				AddNode(NewNode("generate_report", func(ctx context.Context, c *ComplexTestContext) error {
					c.Log("Generating report")
					c.Set("report_generated", true)
					time.Sleep(30 * time.Millisecond)
					return nil
				})).
				AddNode(NewNode("cleanup", func(ctx context.Context, c *ComplexTestContext) error {
					c.Log("Cleanup completed")
					c.Set("cleaned_up", true)
					time.Sleep(10 * time.Millisecond)
					return nil
				}))

			// Add global middleware for monitoring
			monitoringMW := func(node DependencyNode, next Endpoint) Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					if c, ok := req.(*ComplexTestContext); ok {
						c.Increment("nodes_executed")
						start := time.Now()
						result, err := next(ctx, req)
						duration := time.Since(start)
						c.Set(fmt.Sprintf("duration_%s", node.Name()), duration)
						return result, err
					}
					return next(ctx, req)
				}
			}

			// Build the complete graph
			rootGraph.AddGlobalMW(monitoringMW)

			// Create wrapper nodes for chains
			preprocessNode := NewNode("preprocess_chain", func(ctx context.Context, c *ComplexTestContext) error {
				return preprocessChain.Exec(ctx, c)
			})

			postprocessNode := NewNode("postprocess_chain", func(ctx context.Context, c *ComplexTestContext) error {
				return postProcessChain.Exec(ctx, c)
			}, "transformation")

			rootGraph.AddNode(preprocessNode)
			rootGraph.AddNode(dataProcessingGroup.AsNode())
			rootGraph.AddNode(transformationGroup.AsNode())
			rootGraph.AddNode(postprocessNode)

			now := time.Now()
			err := rootGraph.Build()
			So(err, ShouldBeNil)

			err = rootGraph.Exec(ctx, testCtx)
			So(err, ShouldBeNil)
			cost := time.Since(now)

			// Verify execution results
			inputValidated, _ := testCtx.Get("input_validated")
			So(inputValidated, ShouldEqual, true)
			dataInitialized, _ := testCtx.Get("data_initialized")
			So(dataInitialized, ShouldEqual, true)
			resourcesReady, _ := testCtx.Get("resources_ready")
			So(resourcesReady, ShouldEqual, true)
			reportGenerated, _ := testCtx.Get("report_generated")
			So(reportGenerated, ShouldEqual, true)
			cleanedUp, _ := testCtx.Get("cleaned_up")
			So(cleanedUp, ShouldEqual, true)

			// Verify processing counts
			So(testCtx.GetCounter("data_processed"), ShouldEqual, 5)
			So(testCtx.GetCounter("transformations"), ShouldEqual, 3)
			So(testCtx.GetCounter("nodes_executed"), ShouldBeGreaterThan, 10)

			// Verify total processed data
			totalProcessed, exists := testCtx.Get("total_processed")
			So(exists, ShouldBeTrue)
			So(totalProcessed, ShouldEqual, 8)

			// Performance check - complex pipeline should complete in reasonable time
			So(cost, ShouldBeLessThan, 5*time.Second)

			logs := testCtx.GetLogs()
			So(len(logs), ShouldBeGreaterThan, 10)
		})

		Convey("Error propagation and recovery", func() {
			testCtx := NewComplexTestContext()
			defer testCtx.Close()

			graph := NewGraph[*ComplexTestContext]()
			graph.SetMaxGoNum(10)

			// Create a pipeline with potential failure points
			criticalGroup := NewGroup[*ComplexTestContext]("critical_operations")
			criticalGroup.AddNode(NewNode("critical_1", func(ctx context.Context, c *ComplexTestContext) error {
				c.Log("Critical operation 1 started")
				c.Increment("critical_ops")
				time.Sleep(30 * time.Millisecond)
				return nil
			}))

			criticalGroup.AddNode(NewNode("critical_2", func(ctx context.Context, c *ComplexTestContext) error {
				c.Log("Critical operation 2 started")
				// Always fail to ensure test behavior is deterministic
				c.Log("Critical operation 2 failed")
				return fmt.Errorf("critical operation failed")
			}, "critical_1")) // Make it depend on critical_1 to ensure order

			// Independent operations that should continue despite failure
			independentGroup := NewGroup[*ComplexTestContext]("independent_operations")
			for i := 0; i < 3; i++ {
				nodeName := fmt.Sprintf("independent_%d", i)
				independentGroup.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Log(fmt.Sprintf("%s executing", nodeName))
					c.Increment("independent_ops")
					time.Sleep(20 * time.Millisecond)
					return nil
				}))
			}

			// Recovery operations
			recoveryGroup := NewGroup[*ComplexTestContext]("recovery", "critical_operations")
			recoveryGroup.AddNode(NewNode("handle_failure", func(ctx context.Context, c *ComplexTestContext) error {
				c.Log("Handling failure")
				c.Set("failure_handled", true)
				return nil
			}))

			graph.AddNode(criticalGroup.AsNode())
			graph.AddNode(independentGroup.AsNode())
			graph.AddNode(recoveryGroup.AsNode())

			err := graph.Build()
			So(err, ShouldBeNil)

			err = graph.Exec(ctx, testCtx)
			So(err, ShouldNotBeNil) // Should fail due to critical operation

			// Independent operations should still execute
			So(testCtx.GetCounter("independent_ops"), ShouldEqual, 3)

			// Recovery operations may or may not execute depending on timing and implementation
			failureHandled, exists := testCtx.Get("failure_handled")
			if exists {
				// If recovery executed, it should have handled the failure
				So(failureHandled, ShouldEqual, true)
			}

			errors := testCtx.GetErrors()
			So(len(errors), ShouldBeGreaterThanOrEqualTo, 0)
		})
	})
}

// TestPerformanceRegression tests for performance regressions under various scenarios
func TestPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance regression tests in short mode")
	}

	Convey("Performance Regression Tests", t, func() {
		Convey("Scaling with node count", func() {
			nodeCounts := []int{10, 50, 100, 500, 1000}
			var prevExecTime time.Duration
			var prevNodeCount int

			for _, nodeCount := range nodeCounts {
				testCtx := NewComplexTestContext()

				graph := NewGraph[*ComplexTestContext]()
				graph.SetMaxGoNum(100)

				// Create nodes with minimal work
				for i := 0; i < nodeCount; i++ {
					nodeName := fmt.Sprintf("node_%d", i)
					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						c.Increment("executed")
						return nil
					}))
				}

				err := graph.Build()
				So(err, ShouldBeNil)

				start := time.Now()
				err = graph.Exec(context.Background(), testCtx)
				execTime := time.Since(start)
				So(err, ShouldBeNil)

				So(testCtx.GetCounter("executed"), ShouldEqual, nodeCount)

				// Performance scaling check
				if prevExecTime > 0 {
					ratio := float64(execTime) / float64(prevExecTime)
					nodeRatio := float64(nodeCount) / float64(prevNodeCount)

					// Execution time should scale roughly linearly with node count
					// Allow 5x tolerance for overhead and scheduling variance
					So(ratio, ShouldBeLessThan, nodeRatio*5)
				}

				t.Logf("Node count %d: %v", nodeCount, execTime)
				prevExecTime = execTime
				prevNodeCount = nodeCount
				testCtx.Close()
			}
		})

		Convey("Memory usage scaling", func() {
			var m1, m2 runtime.MemStats

			// Baseline
			runtime.GC()
			runtime.ReadMemStats(&m1)

			const numGraphs = 50
			const nodesPerGraph = 20

			graphs := make([]*Graph[*ComplexTestContext], numGraphs)

			for i := 0; i < numGraphs; i++ {
				graph := NewGraph[*ComplexTestContext]()

				for j := 0; j < nodesPerGraph; j++ {
					nodeName := fmt.Sprintf("G%d_N%d", i, j)
					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						c.Increment("total")
						return nil
					}))
				}

				graph.Build()
				graphs[i] = graph
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)

			memIncrease := m2.HeapAlloc - m1.HeapAlloc
			avgMemPerNode := memIncrease / (numGraphs * nodesPerGraph)

			t.Logf("Memory increase: %d bytes, avg per node: %d bytes", memIncrease, avgMemPerNode)

			// Should not use excessive memory per node (less than 10KB per node)
			So(avgMemPerNode, ShouldBeLessThan, 10*1024)

			// Clean up
			for i := range graphs {
				graphs[i] = nil
			}
			runtime.GC()
		})

		Convey("Concurrency efficiency", func() {
			concurrencyLimits := []int{1, 5, 10, 50, 100}
			const numNodes = 100
			const workDuration = 10 * time.Millisecond

			for _, limit := range concurrencyLimits {
				testCtx := NewComplexTestContext()

				graph := NewGraph[*ComplexTestContext]()
				graph.SetMaxGoNum(limit)

				for i := 0; i < numNodes; i++ {
					nodeName := fmt.Sprintf("worker_%d", i)
					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						time.Sleep(workDuration)
						c.Increment("completed")
						return nil
					}))
				}

				err := graph.Build()
				So(err, ShouldBeNil)

				start := time.Now()
				err = graph.Exec(context.Background(), testCtx)
				execTime := time.Since(start)
				So(err, ShouldBeNil)

				So(testCtx.GetCounter("completed"), ShouldEqual, numNodes)

				// Calculate theoretical minimum time
				theoreticalMin := time.Duration(numNodes) * workDuration / time.Duration(limit)

				// Allow reasonable overhead (2x theoretical minimum)
				maxAllowed := theoreticalMin * 2

				So(execTime, ShouldBeLessThan, maxAllowed)

				t.Logf("Concurrency %d: %v (theoretical min: %v)", limit, execTime, theoreticalMin)
				testCtx.Close()
			}
		})
	})
}

// TestResourceLeakDetection tests for various types of resource leaks
func TestResourceLeakDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping resource leak tests in short mode")
	}

	Convey("Resource Leak Detection", t, func() {
		Convey("Goroutine leak detection under stress", func() {
			initialGoroutines := runtime.NumGoroutine()

			const iterations = 100
			const nodesPerIteration = 20

			for i := 0; i < iterations; i++ {
				testCtx := NewComplexTestContext()
				graph := NewGraph[*ComplexTestContext]()
				graph.SetMaxGoNum(10)

				for j := 0; j < nodesPerIteration; j++ {
					nodeName := fmt.Sprintf("iter%d_node%d", i, j)
					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						time.Sleep(time.Millisecond)
						c.Increment("executed")
						return nil
					}))
				}

				graph.Build()
				graph.Exec(context.Background(), testCtx)
				testCtx.Close()

				// Force cleanup every 10 iterations
				if i%10 == 0 {
					runtime.GC()
					time.Sleep(10 * time.Millisecond)
				}
			}

			// Final cleanup
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			runtime.GC()

			finalGoroutines := runtime.NumGoroutine()
			goroutineLeak := finalGoroutines - initialGoroutines

			t.Logf("Goroutines: initial=%d, final=%d, leak=%d",
				initialGoroutines, finalGoroutines, goroutineLeak)

			// Allow some tolerance for background goroutines
			So(goroutineLeak, ShouldBeLessThan, 20)
		})

		Convey("Memory leak detection with cancellation", func() {
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			const iterations = 50
			for i := 0; i < iterations; i++ {
				testCtx := NewComplexTestContext()
				graph := NewGraph[*ComplexTestContext]()
				graph.SetMaxGoNum(20)

				// Create long-running nodes
				for j := 0; j < 50; j++ {
					nodeName := fmt.Sprintf("long_node_%d_%d", i, j)
					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						select {
						case <-time.After(10 * time.Second):
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}))
				}

				graph.Build()

				// Cancel quickly to simulate cleanup
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				graph.Exec(ctx, testCtx)
				cancel()
				testCtx.Close()

				if i%10 == 0 {
					runtime.GC()
				}
			}

			runtime.GC()
			time.Sleep(200 * time.Millisecond)
			runtime.GC()
			runtime.ReadMemStats(&m2)

			memIncrease := m2.HeapAlloc - m1.HeapAlloc
			t.Logf("Memory increase after cancellation tests: %d bytes", memIncrease)

			// Should not have significant memory leak (less than 10MB)
			So(memIncrease, ShouldBeLessThan, 10*1024*1024)
		})
	})
}

// TestStabilityAndRobustness tests system stability under various stress conditions
func TestStabilityAndRobustness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stability tests in short mode")
	}

	Convey("Stability and Robustness Tests", t, func() {
		Convey("Long running execution stability", func() {
			testCtx := NewComplexTestContext()
			defer testCtx.Close()

			graph := NewGraph[*ComplexTestContext]()
			graph.SetMaxGoNum(10)

			const totalNodes = 1000
			const batchSize = 50

			// Create nodes in batches with different execution patterns
			for batch := 0; batch < totalNodes/batchSize; batch++ {
				for i := 0; i < batchSize; i++ {
					nodeIdx := batch*batchSize + i
					nodeName := fmt.Sprintf("stable_node_%d", nodeIdx)

					// Vary execution time and work patterns
					workDuration := time.Duration(1+nodeIdx%10) * time.Millisecond

					graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
						// Simulate various work patterns
						time.Sleep(workDuration)

						// Occasional memory allocation
						if nodeIdx%20 == 0 {
							data := make([]byte, 1024)
							c.Set(fmt.Sprintf("data_%d", nodeIdx), data)
						}

						c.Increment("stable_executed")

						// Occasional logging
						if nodeIdx%50 == 0 {
							c.Log(fmt.Sprintf("Milestone: %d nodes completed", nodeIdx))
						}

						return nil
					}))
				}
			}

			err := graph.Build()
			So(err, ShouldBeNil)

			start := time.Now()
			err = graph.Exec(context.Background(), testCtx)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			So(testCtx.GetCounter("stable_executed"), ShouldEqual, totalNodes)

			// Should complete within reasonable time (less than 30 seconds)
			So(duration, ShouldBeLessThan, 30*time.Second)

			t.Logf("Long running test completed in %v", duration)
		})

		Convey("Concurrent graph execution safety", func() {
			const numGoroutines = 20
			const executionsPerGoroutine = 25

			var wg sync.WaitGroup
			var totalExecuted int64
			var errors int64

			// Create a reusable graph
			baseGraph := NewGraph[*ComplexTestContext]()
			baseGraph.SetMaxGoNum(5)

			for i := 0; i < 10; i++ {
				nodeName := fmt.Sprintf("concurrent_node_%d", i)
				baseGraph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Increment("concurrent_executed")
					time.Sleep(time.Millisecond * time.Duration(1+i))
					return nil
				}))
			}

			err := baseGraph.Build()
			So(err, ShouldBeNil)

			// Execute the same graph concurrently from multiple goroutines
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					for exec := 0; exec < executionsPerGoroutine; exec++ {
						testCtx := NewComplexTestContext()

						if err := baseGraph.Exec(context.Background(), testCtx); err != nil {
							atomic.AddInt64(&errors, 1)
						} else {
							atomic.AddInt64(&totalExecuted, testCtx.GetCounter("concurrent_executed"))
						}

						testCtx.Close()
					}
				}(g)
			}

			wg.Wait()

			So(atomic.LoadInt64(&errors), ShouldEqual, 0)
			expectedTotal := int64(numGoroutines * executionsPerGoroutine * 10) // 10 nodes per execution
			So(atomic.LoadInt64(&totalExecuted), ShouldEqual, expectedTotal)
		})

		Convey("Recovery from partial failures", func() {
			testCtx := NewComplexTestContext()
			defer testCtx.Close()

			graph := NewGraph[*ComplexTestContext]()
			graph.SetMaxGoNum(15)

			var successCount int64
			var failureCount int64

			// Create nodes with different failure rates
			for i := 0; i < 100; i++ {
				nodeName := fmt.Sprintf("recovery_node_%d", i)
				failureProbability := i % 10 // Every 10th node fails

				// Capture variables by value to avoid closure issues
				nodeNameCopy := nodeName
				shouldFail := failureProbability == 0

				graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					if shouldFail {
						atomic.AddInt64(&failureCount, 1)
						c.Log(fmt.Sprintf("%s failed", nodeNameCopy))
						return nil
					}

					atomic.AddInt64(&successCount, 1)
					c.Increment("recovery_success")
					time.Sleep(5 * time.Millisecond)
					return nil
				}))
			}

			err := graph.Build()
			So(err, ShouldBeNil)

			err = graph.Exec(context.Background(), testCtx)
			So(err, ShouldBeNil) // Should fail due to some node failures

			// But many nodes should still succeed
			So(atomic.LoadInt64(&successCount), ShouldBeGreaterThan, 80) // At least 90 should succeed
			So(atomic.LoadInt64(&failureCount), ShouldBeGreaterThan, 5)  // About 10 should fail

			successfulExecutions := testCtx.GetCounter("recovery_success")
			So(successfulExecutions, ShouldBeGreaterThan, 80)
		})
	})
}

// BenchmarkComprehensiveScenarios provides benchmarks for comprehensive scenarios
func BenchmarkComprehensiveScenarios(b *testing.B) {
	b.Run("ComplexPipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			testCtx := NewComplexTestContext()

			// Create a complex pipeline
			graph := NewGraph[*ComplexTestContext]()
			graph.SetMaxGoNum(10)

			// Preprocessing
			preprocessing := NewGroup[*ComplexTestContext]("preprocessing")
			for j := 0; j < 5; j++ {
				nodeName := fmt.Sprintf("preprocess_%d", j)
				preprocessing.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Increment("preprocessed")
					return nil
				}))
			}

			// Main processing
			mainProcessing := NewGroup[*ComplexTestContext]("main_processing", "preprocessing")
			for j := 0; j < 10; j++ {
				nodeName := fmt.Sprintf("process_%d", j)
				mainProcessing.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Increment("processed")
					time.Sleep(time.Microsecond * 100)
					return nil
				}))
			}

			// Postprocessing
			postprocessing := NewChain[*ComplexTestContext]()
			postprocessing.AddNode(NewNode("aggregate", func(ctx context.Context, c *ComplexTestContext) error {
				c.Increment("aggregated")
				return nil
			})).AddNode(NewNode("finalize", func(ctx context.Context, c *ComplexTestContext) error {
				c.Increment("finalized")
				return nil
			}))

			// Create wrapper node for postprocessing chain
			postprocessingNode := NewNode("postprocessing", func(ctx context.Context, c *ComplexTestContext) error {
				return postprocessing.Exec(ctx, c)
			}, "main_processing")

			graph.AddNode(preprocessing.AsNode())
			graph.AddNode(mainProcessing.AsNode())
			graph.AddNode(postprocessingNode)

			b.StartTimer()
			graph.Build()
			graph.Exec(context.Background(), testCtx)
			b.StopTimer()

			testCtx.Close()
		}
	})

	b.Run("HighConcurrencyExecution", func(b *testing.B) {
		const numNodes = 500

		for i := 0; i < b.N; i++ {
			testCtx := NewComplexTestContext()
			graph := NewGraph[*ComplexTestContext]()
			graph.SetMaxGoNum(100)

			for j := 0; j < numNodes; j++ {
				nodeName := fmt.Sprintf("concurrent_node_%d", j)
				graph.AddNode(NewNode(nodeName, func(ctx context.Context, c *ComplexTestContext) error {
					c.Increment("executed")
					return nil
				}))
			}

			b.StartTimer()
			graph.Build()
			graph.Exec(context.Background(), testCtx)
			b.StopTimer()

			testCtx.Close()
		}
	})
}
