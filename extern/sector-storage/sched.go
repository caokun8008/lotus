package sectorstorage

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	// true if worker is acceptable for performing a task 如果工作人员可以接受执行任务，则返回true
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error)
	// true if a is preferred over b //如果a优先于b则为true
	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error)
}

type scheduler struct {
	spt abi.RegisteredSealProof  //3

	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important  //优先级，值大有限
	sel      WorkerSelector

	prepare WorkerAction  //worker行为，是个函数
	work    WorkerAction

	start time.Time  //开始时间

	index int // The index of the item in the heap. 堆中项目的索引。

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: spt,

		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait) //等待3秒钟
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:  //结构体通道，worker发生变化时，调度激活
			doSched = true
		case dreq := <-sh.workerDisable:  //worker掉线或禁用时，禁用列表更新，调度激活
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule: // 收到调度请求，把请求添加到调度队列，激活调度
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:  //收到工作窗口请求
			sh.openWindows = append(sh.openWindows, req)  //把工作窗口请求添加到打开的窗口组，激活调度
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())  //调度队列和窗口

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:  //收到调度关闭信号，关闭调度
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task 首先收集所有未完成的任务，因此我们为每个添加的任务执行一次调度循环
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {  //循环任务队列
		task := (*sh.schedQueue)[sqi] //具体任务
        log.Info("==========[diag],task = ",task)  // add by ck
		out.Requests = append(out.Requests, SchedDiagRequestInfo{ //把任务添加到 SchedDiagInfo 中的 Requests 切片
			Sector:   task.sector,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock() //给调度上锁
	defer sh.workersLk.RUnlock() //函数执行完成时解锁

	//遍历调度中的openWindeows,并放入SchedDiagInfo中的OpenWindows切片
	for _, window := range sh.openWindows {
		log.Info("==========[diag],window = ",window)  // add by ck
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
			This assigns tasks to workers based on:
			- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
			- Worker resource availability
			- Task-specified worker preference (acceptableWindows array below sorted by this preference)
			- Window request age

			1. For each task in the schedQueue find windows which can handle them
			1.1. Create list of windows capable of handling a task
			1.2. Sort windows according to task selector preferences
			2. Going through schedQueue again, assign task to first acceptable window
			   with resources available
			3. Submit windows with scheduled tasks to workers
		    这基于以下任务将任务分配给工作人员：
		    -任务优先级（通过按顺序处理sh.schedQueue实现，因为它已经按优先级排序了）
		    -工人资源的可用性
		    -任务指定的工作人员首选项（下面的Windows数组按此首选项排序）
		    -窗口请求年龄

		    1.对于schedQueue中的每个任务，找到可以处理它们的窗口
		    1.1。 创建能够处理任务的窗口列表
		    1.2。 根据任务选择器首选项对窗口进行排序
		    2.再次通过schedQueue，将任务分配给第一个可接受的窗口，有可用资源
		    3.将带有计划任务的窗口提交给工作人员

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()
	log.Info("==========[trySched],sh.openWindows = ",sh.openWindows)  // add by ck
	windows := make([]schedWindow, len(sh.openWindows)) //worker windows切片，worker调度窗口的切片
	acceptableWindows := make([][]int, sh.schedQueue.Len()) //可以接受任务的 worker调度窗口切片

	log.Debugf("SCHED %d queued; %d open windows", sh.schedQueue.Len(), len(windows))

	if len(sh.openWindows) == 0 { //如果调度对象的openWindows个数为0，返回
		// nothing to schedule on
		return
	}

	// Step 1
	concurrency := len(sh.openWindows)  //并发个数等于调度对象中openWindows的个数
	throttle := make(chan struct{}, concurrency) //定义一个传结构体的通道，异步个数是并发数

	var wg sync.WaitGroup  //定义一个同步阻塞组
	wg.Add(sh.schedQueue.Len())  //向同步阻塞组中添加阻塞，阻塞个数等于调度队列的长度

	for i := 0; i < sh.schedQueue.Len(); i++ {  // 循环调度任务队列
		throttle <- struct{}{}  //向结构体通道传一个空结构体

		go func(sqi int) {     //使用匿名函数启动一个协程
			defer wg.Done()    //协程函数结束时调用
			defer func() {     //协程函数结束时从通道接收结构体数据
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]  //把队列中的第sqi个任务赋值给task
			log.Info("==========[trySched] task = ",task)  //把task打印出来，看看到底是个什么东东 add by ck
			needRes := ResourceTable[task.taskType][sh.spt]  //任务需要的资源
			log.Info("==========[trySched] needRes = ",needRes)  //把needRes打印出来，看看到底是个什么东东 add by ck
			task.indexHeap = sqi  //task在堆中的索引
			for wnd, windowRequest := range sh.openWindows {  //循环worker窗口
				worker, ok := sh.workers[windowRequest.worker] //通过workerid获取一个具体的worker信息
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}
				log.Info("==========[trySched] worker = ",worker)  //把worker打印出来，看看到底是个什么东东 add by ck
				log.Info("==========[trySched] wnd = ",wnd)  //add by ck

				if !worker.enabled {  //如果worker不可用，跳过这个不可用的worker,并打印出该worker信息，并继续循环
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				//检查worker可分配的内存，cpu,gpu资源是否能满足需要，不能满足继续下一个循环，找下一个worker窗口
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout) //选择器时间超时，返回一个取消函数
				ok, err := task.sel.Ok(rpcCtx, task.taskType, sh.spt, worker)  //worker能否接受任务
				cancel() //调用取消函数
				if err != nil { //出错，继续循环
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {  //worker不接受任务，继续新循环
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd) //添加一个可接受任务窗口
			}

			if len(acceptableWindows[sqi]) == 0 {  //可接受任务窗口等于0，返回
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good) 选择最佳工人（如果有些工人同样出色，则应洗牌）
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool { //优先级排序
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Error("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][sh.spt]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.Number, task.taskType, wnd)

			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		sh.schedQueue.Remove(sqi)
		sqi--
		scheduled++
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, len(sh.openWindows)-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
