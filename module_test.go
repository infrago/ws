package ws

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type testConn struct {
	readTypes  []int
	reads      [][]byte
	writeTypes []int
	writes     [][]byte
	writeErr   error
	readLimit  int64
	compress   bool
	level      int
}

func (c *testConn) ReadMessage() (int, []byte, error) {
	if len(c.reads) == 0 {
		return 0, nil, io.EOF
	}
	item := c.reads[0]
	c.reads = c.reads[1:]
	messageType := 1
	if len(c.readTypes) > 0 {
		messageType = c.readTypes[0]
		c.readTypes = c.readTypes[1:]
	}
	return messageType, item, nil
}

func (c *testConn) WriteMessage(messageType int, data []byte) error {
	if c.writeErr != nil {
		return c.writeErr
	}
	c.writeTypes = append(c.writeTypes, messageType)
	c.writes = append(c.writes, append([]byte(nil), data...))
	return nil
}

func (c *testConn) Close() error { return nil }

func (c *testConn) Raw() Any { return c }

func (c *testConn) SetReadLimit(limit int64) {
	c.readLimit = limit
}

func (c *testConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetPongHandler(func(string) error) {}

func (c *testConn) EnableWriteCompression(enabled bool) {
	c.compress = enabled
}

func (c *testConn) SetCompressionLevel(level int) error {
	c.level = level
	return nil
}

func resetModuleForTest() {
	module.mutex.Lock()
	module.opened = false
	module.config = Config{Format: "text", Codec: infra.JSON, PingInterval: 30 * time.Second, ReadTimeout: 75 * time.Second, WriteTimeout: 10 * time.Second, MaxMessageSize: 4 << 20, QueueSize: 128, QueuePolicy: "close"}
	module.messages = make(map[string]map[string]Message)
	module.commands = make(map[string]map[string]Command)
	module.filters = make(map[string]map[string]Filter)
	module.handlers = make(map[string]map[string]Handler)
	module.hooks = make(map[string]map[string]Hook)
	module.filterLists = nil
	module.handlerLists = nil
	module.hookLists = nil
	module.filterChains = nil
	module.hookOpen = nil
	module.hookClose = nil
	module.hookReceive = nil
	module.hookSend = nil
	module.frameMessage = 0
	module.frameCodecV = ""
	module.stats = wsStats{}
	module.mutex.Unlock()

	module.sessionMutex.Lock()
	module.sessions = make(map[string]*Session)
	module.bySpace = make(map[string]map[string]*Session)
	module.groups = make(map[string]map[string]map[string]*Session)
	module.users = make(map[string]map[string]map[string]*Session)
	module.sessionMutex.Unlock()
}

func TestBinaryFormatWritesBinaryMessage(t *testing.T) {
	resetModuleForTest()

	module.Config(Map{"ws": Map{"format": "binary"}})
	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	conn := &testConn{}
	session := &Session{
		ID:     "s1",
		Meta:   infra.NewMeta(),
		Conn:   conn,
		Groups: map[string]Any{},
	}

	if err := module.sendLocal(nil, session, "demo.notice", Map{"ok": true}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if len(conn.writeTypes) != 1 || conn.writeTypes[0] != BinaryMessage {
		t.Fatalf("expected binary websocket frame, got %#v", conn.writeTypes)
	}
}

func TestCustomCodecWritesEncodedPayload(t *testing.T) {
	resetModuleForTest()

	const codecName = "ws_test_codec"
	infra.Register(codecName, infra.Codec{
		Encode: func(v Any) (Any, error) {
			env, ok := v.(Map)
			if !ok {
				return nil, fmt.Errorf("unexpected payload: %T", v)
			}
			data := anyToMap(env["data"])
			return []byte(fmt.Sprint(env["name"]) + "|" + fmt.Sprint(data["text"])), nil
		},
		Decode: func(d Any, v Any) (Any, error) {
			data, ok := d.([]byte)
			if !ok {
				return nil, fmt.Errorf("unexpected data: %T", d)
			}
			env, ok := v.(*Map)
			if !ok {
				return nil, fmt.Errorf("unexpected target: %T", v)
			}
			parts := strings.SplitN(string(data), "|", 2)
			*env = Map{"name": parts[0], "data": Map{}}
			if len(parts) > 1 {
				(*env)["data"].(Map)["text"] = parts[1]
			}
			return env, nil
		},
	})

	module.Config(Map{"ws": Map{"codec": codecName}})
	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	conn := &testConn{}
	session := &Session{
		ID:     "s1",
		Meta:   infra.NewMeta(),
		Conn:   conn,
		Groups: map[string]Any{},
	}

	if err := module.sendLocal(nil, session, "demo.notice", Map{"text": "hello"}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if len(conn.writes) != 1 || string(conn.writes[0]) != "demo.notice|hello" {
		t.Fatalf("expected custom codec output, got %#v", conn.writes)
	}
}

func TestAcceptRoutesMessageAndReply(t *testing.T) {
	resetModuleForTest()

	module.RegisterCommand("demo.echoed", Command{})
	module.RegisterMessage("demo.echo", Message{
		Action: func(ctx *Context) {
			if err := ctx.Reply("demo.echoed", Map{"text": ctx.Value["text"]}); err != nil {
				t.Fatalf("reply failed: %v", err)
			}
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"msg":"demo.echo","args":{"text":"hello"}}`)},
	}

	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	if len(conn.writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(conn.writes))
	}

	env := Map{}
	if err := json.Unmarshal(conn.writes[0], &env); err != nil {
		t.Fatalf("invalid write payload: %v", err)
	}
	if env["name"] != "demo.echoed" {
		t.Fatalf("unexpected command: %v", env["name"])
	}
	if code := env["code"]; code != float64(0) {
		t.Fatalf("unexpected command code: %#v", code)
	}
	if _, ok := env["time"]; !ok {
		t.Fatalf("expected command time")
	}
	if data := anyToMap(env["data"]); data["text"] != "hello" {
		t.Fatalf("unexpected command data: %#v", env["data"])
	}
}

func TestAnswerUsesCodeTextAndDataEnvelope(t *testing.T) {
	resetModuleForTest()

	module.RegisterCommand("demo.notice", Command{})
	module.RegisterMessage("demo.fail", Message{
		Action: func(ctx *Context) {
			_ = ctx.Answer("demo.notice", Map{"field": "bad"}, infra.Invalid.With("字段错误"))
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"name":"demo.fail","data":{}}`)},
	}

	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	env := Map{}
	if err := json.Unmarshal(conn.writes[0], &env); err != nil {
		t.Fatalf("invalid write payload: %v", err)
	}
	if env["code"] != float64(infra.Invalid.Code()) {
		t.Fatalf("unexpected code: %#v", env["code"])
	}
	if env["name"] != "demo.notice" {
		t.Fatalf("unexpected name: %#v", env["name"])
	}
	if env["text"] == "" {
		t.Fatalf("expected error text")
	}
}

func TestAcceptMergesRemainingFieldsIntoArgs(t *testing.T) {
	resetModuleForTest()

	module.RegisterMessage("demo.echo", Message{
		Action: func(ctx *Context) {
			if ctx.Value["text"] != "hello" || ctx.Value["lang"] != "zh" {
				t.Fatalf("unexpected merged value: %#v", ctx.Value)
			}
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"name":"demo.echo","text":"hello","lang":"zh"}`)},
	}

	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}
}

func TestGroupcastLocal(t *testing.T) {
	resetModuleForTest()
	module.Open()

	conn1 := &testConn{}
	conn2 := &testConn{}

	session1 := &Session{ID: "s1", Meta: infra.NewMeta(), Conn: conn1, Groups: map[string]Any{}}
	session2 := &Session{ID: "s2", Meta: infra.NewMeta(), Conn: conn2, Groups: map[string]Any{}}
	module.registerSession(session1)
	module.registerSession(session2)
	module.join(session1, "room1")

	if result := module.deliverGroup(nil, infra.DEFAULT, "room1", "demo.notice", Map{"ok": true}); result.FirstError != "" {
		t.Fatalf("group deliver failed: %v", result.FirstError)
	}

	if len(conn1.writes) != 1 {
		t.Fatalf("expected room member to receive write")
	}
	if len(conn2.writes) != 0 {
		t.Fatalf("unexpected write for non-member")
	}
}

func TestUnknownMessageTriggersHandler(t *testing.T) {
	resetModuleForTest()

	hit := false
	module.RegisterHandler("demo.invalid", Handler{
		Invalid: func(ctx *Context) {
			hit = true
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"msg":"demo.none","args":{"text":"hello"}}`)},
	}

	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}

	if !hit {
		t.Fatalf("expected invalid handler to be called")
	}
}

func TestSendLocalRemovesBrokenSession(t *testing.T) {
	resetModuleForTest()

	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	session := &Session{
		ID:     "broken",
		Meta:   infra.NewMeta(),
		Conn:   &testConn{writeErr: io.ErrClosedPipe},
		Groups: map[string]Any{},
		closed: make(chan struct{}),
	}
	module.registerSession(session)

	if err := module.sendLocal(nil, session, "demo.notice", Map{"ok": true}); err == nil {
		t.Fatalf("expected write failure")
	}
	if got := module.sessionByID("broken"); got != nil {
		t.Fatalf("expected broken session to be removed")
	}
}

func TestPushUserDeliversToBoundSessions(t *testing.T) {
	resetModuleForTest()

	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	conn1 := &testConn{}
	conn2 := &testConn{}
	conn3 := &testConn{}

	session1 := &Session{ID: "s1", User: "u1", Meta: infra.NewMeta(), Conn: conn1, Groups: map[string]Any{}, closed: make(chan struct{})}
	session2 := &Session{ID: "s2", User: "u1", Meta: infra.NewMeta(), Conn: conn2, Groups: map[string]Any{}, closed: make(chan struct{})}
	session3 := &Session{ID: "s3", User: "u2", Meta: infra.NewMeta(), Conn: conn3, Groups: map[string]Any{}, closed: make(chan struct{})}
	module.registerSession(session1)
	module.registerSession(session2)
	module.registerSession(session3)

	if result := module.deliverUser(nil, infra.DEFAULT, "u1", "demo.notice", Map{"ok": true}); result.FirstError != "" {
		t.Fatalf("push user failed: %v", result.FirstError)
	}

	if len(conn1.writes) != 1 || len(conn2.writes) != 1 {
		t.Fatalf("expected bound user sessions to receive write")
	}
	if len(conn3.writes) != 0 {
		t.Fatalf("unexpected write for other user")
	}
}

func TestBroadcastResultCounts(t *testing.T) {
	resetModuleForTest()

	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	session1 := &Session{ID: "s1", Meta: infra.NewMeta(), Conn: &testConn{}, Groups: map[string]Any{}, closed: make(chan struct{})}
	session2 := &Session{ID: "s2", Meta: infra.NewMeta(), Conn: &testConn{writeErr: io.ErrClosedPipe}, Groups: map[string]Any{}, closed: make(chan struct{})}
	module.registerSession(session1)
	module.registerSession(session2)

	result := module.deliverBroadcast(nil, infra.DEFAULT, "demo.notice", Map{"ok": true})
	if result.Hit != 2 || result.Success != 1 || result.Failed != 1 {
		t.Fatalf("unexpected delivery result: %#v", result)
	}
	if result.FirstError == "" {
		t.Fatalf("expected first error")
	}
}

func TestConfigureSessionAppliesReadLimit(t *testing.T) {
	resetModuleForTest()

	module.Config(Map{"ws": Map{"max_message_size": 2048}})
	module.Open()

	conn := &testConn{}
	session := &Session{ID: "s1", Meta: infra.NewMeta(), Conn: conn, Groups: map[string]Any{}, closed: make(chan struct{})}
	module.configureSession(session)

	if conn.readLimit != 2048 {
		t.Fatalf("expected read limit applied, got %d", conn.readLimit)
	}
}

func TestConfigureSessionAppliesCompression(t *testing.T) {
	resetModuleForTest()

	module.Config(Map{"ws": Map{"compression": true, "compress_level": 3}})
	module.Open()

	conn := &testConn{}
	session := &Session{ID: "s1", Meta: infra.NewMeta(), Conn: conn, Groups: map[string]Any{}, closed: make(chan struct{})}
	module.configureSession(session)

	if !conn.compress || conn.level != 3 {
		t.Fatalf("expected compression config applied, got enabled=%v level=%d", conn.compress, conn.level)
	}
}

func TestAcceptCloseFrameTriggersCloseHook(t *testing.T) {
	resetModuleForTest()

	closed := false
	module.RegisterHook("demo.close", Hook{
		Close: func(ctx *Context) {
			closed = true
		},
	})
	module.Open()

	conn := &testConn{
		readTypes: []int{CloseMessage},
		reads:     [][]byte{[]byte("bye")},
	}

	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}

	if !closed {
		t.Fatalf("expected close hook to be called")
	}
}

func TestQueueDropPolicy(t *testing.T) {
	resetModuleForTest()
	module.Config(Map{"ws": Map{"queue_size": 1, "queue_policy": "drop"}})
	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	session := &Session{
		ID:        "s1",
		Meta:      infra.NewMeta(),
		Conn:      &testConn{},
		closed:    make(chan struct{}),
		sendQueue: make(chan preparedFrame, 1),
		Groups:    map[string]Any{},
	}

	first := module.deliverPrepared(session, module.prepareFrame(nil, session, "demo.notice", Map{"ok": true}, nil))
	second := module.deliverPrepared(session, module.prepareFrame(nil, session, "demo.notice", Map{"ok": true}, nil))
	if first.Success != 1 {
		t.Fatalf("expected first enqueue success: %#v", first)
	}
	if second.Failed != 1 || second.FirstError == "" {
		t.Fatalf("expected second enqueue drop: %#v", second)
	}
}

func TestLowPriorityAutoDropsUnderPressure(t *testing.T) {
	resetModuleForTest()
	module.RegisterCommand("demo.low", Command{
		Setting: Map{"priority": "low"},
	})
	module.Open()

	session := &Session{
		ID:        "s1",
		Meta:      infra.NewMeta(),
		Conn:      &testConn{},
		closed:    make(chan struct{}),
		sendQueue: make(chan preparedFrame, 4),
		Groups:    map[string]Any{},
	}
	for i := 0; i < 3; i++ {
		session.sendQueue <- preparedFrame{Name: "queued"}
	}

	result := module.deliverPrepared(session, module.prepareFrame(nil, session, "demo.low", Map{"ok": true}, nil))
	if result.Failed != 1 || result.FirstError == "" {
		t.Fatalf("expected low priority drop under pressure: %#v", result)
	}
}

func TestMetricsQueuedTracksCurrentDepth(t *testing.T) {
	resetModuleForTest()
	module.Config(Map{"ws": Map{"queue_size": 4}})
	module.RegisterCommand("demo.notice", Command{})
	module.Open()

	session := &Session{
		ID:        "s1",
		Meta:      infra.NewMeta(),
		Conn:      &testConn{},
		closed:    make(chan struct{}),
		sendQueue: make(chan preparedFrame, 4),
		Groups:    map[string]Any{},
	}

	module.deliverPrepared(session, module.prepareFrame(nil, session, "demo.notice", Map{"n": 1}, nil))
	module.deliverPrepared(session, module.prepareFrame(nil, session, "demo.notice", Map{"n": 2}, nil))
	if got := module.metrics().Queued; got != 2 {
		t.Fatalf("expected queued depth 2 before drain, got %d", got)
	}

	module.startSessionLoop(session)
	time.Sleep(20 * time.Millisecond)
	if got := module.metrics().Queued; got != 0 {
		t.Fatalf("expected queued depth 0 after drain, got %d", got)
	}
	module.shutdownSession(session)
}

func TestSessionLoopSendsPingWithoutQueue(t *testing.T) {
	resetModuleForTest()
	module.Config(Map{"ws": Map{"ping_interval": "10ms", "write_timeout": "5ms", "queue_size": 0}})
	module.Open()

	conn := &testConn{}
	session := &Session{
		ID:     "s1",
		Meta:   infra.NewMeta(),
		Conn:   conn,
		closed: make(chan struct{}),
		Groups: map[string]Any{},
	}

	module.startSessionLoop(session)
	time.Sleep(25 * time.Millisecond)
	module.shutdownSession(session)

	if len(conn.writeTypes) == 0 || conn.writeTypes[0] != PingMessage {
		t.Fatalf("expected keepalive ping write, got %#v", conn.writeTypes)
	}
}

func TestEOFDoesNotCountAsReceiveFailed(t *testing.T) {
	resetModuleForTest()
	module.Open()

	if err := Accept(AcceptOptions{
		Conn: &testConn{},
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	if got := module.metrics().ReceiveFailed; got != 0 {
		t.Fatalf("expected eof close not counted as receive failure, got %d", got)
	}
}

func TestExportIncludesMessagesAndCommands(t *testing.T) {
	resetModuleForTest()
	module.RegisterMessage("demo.echo", Message{Name: "echo", Desc: "echo msg", Args: Vars{"text": {Type: "string", Required: true}}, Action: func(*Context) {}})
	module.RegisterCommand("demo.notice", Command{Name: "notice", Desc: "notice cmd", Args: Vars{"text": {Type: "string"}}})
	module.Open()

	doc := Export()
	schema := anyToMap(doc["schema"])
	if schema["name"] != "infrago.ws.export" || schema["version"] != exportSchemaVer {
		t.Fatalf("expected export schema metadata: %#v", schema)
	}
	if schema["generated"] == nil {
		t.Fatalf("expected schema generated timestamp: %#v", schema)
	}
	messages := anyToMap(anyToMap(doc["messages"])[infra.DEFAULT])
	commands := anyToMap(anyToMap(doc["commands"])[infra.DEFAULT])
	if _, ok := messages["demo.echo"]; !ok {
		t.Fatalf("expected message export")
	}
	if _, ok := commands["demo.notice"]; !ok {
		t.Fatalf("expected command export")
	}
	envelope := anyToMap(doc["envelope"])
	if anyToMap(anyToMap(envelope["response"])["default"])["name"] != "demo.command" {
		t.Fatalf("expected response envelope export: %#v", envelope)
	}
	sample := anyToMap(anyToMap(messages["demo.echo"])["sample"])
	if sample["name"] != "demo.echo" {
		t.Fatalf("expected message sample export: %#v", sample)
	}
	if _, ok := sample["space"]; ok {
		t.Fatalf("wire sample should not expose internal space: %#v", sample)
	}
	spaces, ok := doc["spaces"].([]Map)
	if !ok || len(spaces) != 1 || spaces[0]["space"] != infra.DEFAULT {
		t.Fatalf("expected spaces export: %#v", doc["spaces"])
	}
	if spaces[0]["message_count"] != 1 || spaces[0]["command_count"] != 1 {
		t.Fatalf("expected per-space counts: %#v", spaces[0])
	}
}

func TestAcceptDefaultsSpaceFromName(t *testing.T) {
	resetModuleForTest()

	hit := false
	module.RegisterMessage("demo.echo", Message{
		Space: "demo.socket",
		Action: func(ctx *Context) {
			hit = true
			if ctx.Space != "demo.socket" || ctx.Session.Space != "demo.socket" {
				t.Fatalf("unexpected space: ctx=%q session=%q", ctx.Space, ctx.Session.Space)
			}
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"name":"demo.echo","data":{}}`)},
	}
	if err := Accept(AcceptOptions{
		Conn: conn,
		Meta: infra.NewMeta(),
		Name: "demo.socket",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	if !hit {
		t.Fatalf("expected space-specific message to be called")
	}
}

func TestBroadcastIsolatedBySpace(t *testing.T) {
	resetModuleForTest()
	module.Open()

	conn1 := &testConn{}
	conn2 := &testConn{}
	session1 := &Session{ID: "s1", Space: "chat", Meta: infra.NewMeta(), Conn: conn1, Groups: map[string]Any{}, closed: make(chan struct{})}
	session2 := &Session{ID: "s2", Space: "notice", Meta: infra.NewMeta(), Conn: conn2, Groups: map[string]Any{}, closed: make(chan struct{})}
	module.registerSession(session1)
	module.registerSession(session2)

	result := module.deliverBroadcast(nil, "chat", "demo.notice", Map{"ok": true})
	if result.Hit != 1 || result.Success != 1 {
		t.Fatalf("unexpected broadcast result: %#v", result)
	}
	if len(conn1.writes) != 1 || len(conn2.writes) != 0 {
		t.Fatalf("expected only target space session to receive write")
	}
}

func TestFilterChainIncludesGlobalAndSpaceFilters(t *testing.T) {
	resetModuleForTest()

	order := make([]string, 0, 3)
	module.RegisterFilter("global", Filter{
		Message: func(ctx *Context) {
			order = append(order, "global")
			ctx.Next()
		},
	})
	module.RegisterFilter("room", Filter{
		Space: "room",
		Message: func(ctx *Context) {
			order = append(order, "room")
			ctx.Next()
		},
	})
	module.RegisterMessage("demo.echo", Message{
		Space: "room",
		Action: func(ctx *Context) {
			order = append(order, "action")
		},
	})
	module.Open()

	conn := &testConn{
		reads: [][]byte{[]byte(`{"name":"demo.echo","data":{}}`)},
	}
	if err := Accept(AcceptOptions{
		Conn:  conn,
		Meta:  infra.NewMeta(),
		Name:  "demo.socket",
		Space: "room",
	}); err != nil {
		t.Fatalf("accept failed: %v", err)
	}

	if len(order) != 3 || order[0] != "global" || order[1] != "room" || order[2] != "action" {
		t.Fatalf("unexpected filter order: %#v", order)
	}
}
