package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpcs     *rpc.Server
	me       string
	rpccount int32

	// --- 상태 변수들 ---
	view         View                 
	primaryAcked bool                 
	lastPing     map[string]time.Time 
	currentTick  uint                 
}

//
// [수정된 Ping 함수]
// Primary가 재시작(ViewNum 0)하면 시간을 갱신하지 않아 타임아웃 되게 만듭니다.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server := args.Me
	
	// [핵심 수정] 
	// 만약 현재 Primary가 재시작(ViewNum 0)해서 연락이 왔다면?
	// -> 이는 "장애(Crash)" 상황입니다.
	// -> lastPing을 갱신하지 않음으로써 proceedView()에서 타임아웃(Dead) 처리되게 유도합니다.
	// -> 또한 primaryAcked를 false로 만들지 않아야(기존 true 유지), 
	//    proceedView가 "Ack 되었으니 뷰를 변경하자"고 판단하여 Backup을 승진시킬 수 있습니다.
	
	isPrimaryCrash := (args.Viewnum == 0 && vs.view.Primary == server)

	if !isPrimaryCrash {
		// 정상적인 경우에만 시간 갱신
		vs.lastPing[server] = time.Now()
	}

	// Ack 확인 (보낸 사람이 Primary이고, 뷰 번호가 일치하면 Ack 완료)
	if server == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
		vs.primaryAcked = true
	}

	// 뷰 변경 시도
	vs.proceedView()
	
	reply.View = vs.view

	return nil
}

func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view
	return nil
}

func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.currentTick++
	vs.proceedView()
}

func (vs *ViewServer) proceedView() {
	// [Ack Rule] Primary가 현재 뷰를 Ack하지 않았으면 다음 뷰로 진행 불가
	if vs.view.Viewnum > 0 && !vs.primaryAcked {
		return
	}

	var nextView View = vs.view
	changed := false

	// 헬퍼: 타임아웃 체크
	isDead := func(server string) bool {
		if server == "" { return true }
		last, ok := vs.lastPing[server]
		if !ok { return true } // 본 적 없음
		return time.Since(last) > DeadPings*PingInterval
	}

	primaryDead := isDead(vs.view.Primary)
	backupDead := isDead(vs.view.Backup)

	// 1. Primary가 죽었을 때 (Ping(0) 보내서 타임아웃 된 경우 포함)
	if primaryDead {
		// Backup이 살아있다면 승진
		if !backupDead && vs.view.Backup != "" {
			nextView.Primary = nextView.Backup
			nextView.Backup = ""
			changed = true
		} else if vs.view.Backup == "" {
            // Backup도 없으면 대기 (방법 없음)
		}
	}

	// 2. Backup 채용 (자리가 비었거나 죽었으면)
	if nextView.Backup == "" || isDead(nextView.Backup) {
		for server, last := range vs.lastPing {
			if server != nextView.Primary && server != nextView.Backup {
				// 살아있는 유휴 서버를 Backup으로
				if time.Since(last) <= DeadPings*PingInterval {
					nextView.Backup = server
					changed = true
					break
				}
			}
		}
	}

	// 3. 초기 상태 (View 0 -> 1)
	if nextView.Viewnum == 0 && nextView.Primary == "" {
		for server, last := range vs.lastPing {
			if time.Since(last) <= DeadPings*PingInterval {
				nextView.Primary = server
				nextView.Viewnum = 1
				nextView.Backup = ""
				
                vs.view = nextView
                vs.primaryAcked = false
                return 
			}
		}
	}

	// 변경 적용
	if changed {
		nextView.Viewnum++
		vs.view = nextView
		vs.primaryAcked = false
	}
}

func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

func (vs *ViewServer) isDead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.view = View{Viewnum: 0}
	vs.primaryAcked = false
	vs.lastPing = make(map[string]time.Time)
    vs.rpccount = 0

	rpcs := rpc.NewServer()
	rpcs.Register(vs)
	vs.rpcs = rpcs

	// TCP 설정 유지
	l, e := net.Listen("tcp", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	go func() {
		for vs.isDead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isDead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isDead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	go func() {
		for vs.isDead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}