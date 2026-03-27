package ws

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/log"
)

const (
	opOpen    = "open"
	opReceive = "receive"
	opMessage = "message"
	opSend    = "send"
	opClose   = "close"

	dispatchPush      = "push"
	dispatchPushUser  = "push_user"
	dispatchBroadcast = "broadcast"
	dispatchGroupcast = "groupcast"

	internalDispatch = "_ws.dispatch"
)

func init() {
	registerInternalMessages()
}

var (
	module = &Module{
		config: Config{
			Format:         "text",
			Codec:          infra.JSON,
			MessageKey:     "name",
			PayloadKey:     "data",
			PingInterval:   time.Second * 30,
			ReadTimeout:    time.Second * 75,
			WriteTimeout:   time.Second * 10,
			MaxMessageSize: 4 << 20,
			QueueSize:      128,
			QueuePolicy:    "close",
		},
		messages: make(map[string]Message),
		commands: make(map[string]Command),
		filters:  make(map[string]Filter),
		handlers: make(map[string]Handler),
		hooks:    make(map[string]Hook),
		sessions: make(map[string]*Session),
		groups:   make(map[string]map[string]*Session),
		users:    make(map[string]map[string]*Session),
	}
	host           = infra.Mount(module)
	jsonBufferPool = sync.Pool{
		New: func() Any {
			return &bytes.Buffer{}
		},
	}
)

type (
	Module struct {
		mutex sync.RWMutex

		opened bool
		config Config

		messages map[string]Message
		commands map[string]Command
		filters  map[string]Filter
		handlers map[string]Handler
		hooks    map[string]Hook

		filterList  []Filter
		handlerList []Handler
		hookList    []Hook

		sessionMutex sync.RWMutex
		sessions     map[string]*Session
		groups       map[string]map[string]*Session
		users        map[string]map[string]*Session

		stats       wsStats
		observeStop chan struct{}
	}

	Hooks map[string]Hook
	Hook  struct {
		Name    string  `json:"name"`
		Desc    string  `json:"desc"`
		Open    ctxFunc `json:"-"`
		Close   ctxFunc `json:"-"`
		Receive ctxFunc `json:"-"`
		Send    ctxFunc `json:"-"`
	}
	Filters map[string]Filter
	Filter  struct {
		Name    string  `json:"name"`
		Desc    string  `json:"desc"`
		Message ctxFunc `json:"-"`
	}
	Handlers map[string]Handler
	Handler  struct {
		Name    string  `json:"name"`
		Desc    string  `json:"desc"`
		Error   ctxFunc `json:"-"`
		Invalid ctxFunc `json:"-"`
		Denied  ctxFunc `json:"-"`
	}
	Messages map[string]Message
	Message  struct {
		Name     string  `json:"name"`
		Desc     string  `json:"desc"`
		Nullable bool    `json:"nullable"`
		Args     Vars    `json:"args"`
		Setting  Map     `json:"-"`
		Action   ctxFunc `json:"-"`
	}
	Commands map[string]Command
	Command  struct {
		Name     string `json:"name"`
		Desc     string `json:"desc"`
		Nullable bool   `json:"nullable"`
		Args     Vars   `json:"args"`
		Setting  Map    `json:"-"`
	}

	Conn interface {
		ReadMessage() (int, []byte, error)
		WriteMessage(messageType int, data []byte) error
		Close() error
		Raw() Any
	}

	Config struct {
		Format          string        `json:"format"`
		Codec           string        `json:"codec"`
		MessageKey      string        `json:"message_key"`
		PayloadKey      string        `json:"payload_key"`
		PingInterval    time.Duration `json:"ping_interval"`
		ReadTimeout     time.Duration `json:"read_timeout"`
		WriteTimeout    time.Duration `json:"write_timeout"`
		MaxMessageSize  int64         `json:"max_message_size"`
		QueueSize       int           `json:"queue_size"`
		QueuePolicy     string        `json:"queue_policy"`
		Compression     bool          `json:"compression"`
		CompressLevel   int           `json:"compress_level"`
		ObserveInterval time.Duration `json:"observe_interval"`
		ObserveLog      bool          `json:"observe_log"`
		ObserveTrace    bool          `json:"observe_trace"`
		Setting         Map           `json:"-"`
	}

	Delivery struct {
		Hit        int    `json:"hit"`
		Success    int    `json:"success"`
		Failed     int    `json:"failed"`
		FirstError string `json:"first_error,omitempty"`
	}

	Stats struct {
		Connections      int64 `json:"connections"`
		Users            int64 `json:"users"`
		MessagesReceived int64 `json:"messages_received"`
		MessagesSent     int64 `json:"messages_sent"`
		ReceiveFailed    int64 `json:"receive_failed"`
		SendFailed       int64 `json:"send_failed"`
		BytesReceived    int64 `json:"bytes_received"`
		BytesSent        int64 `json:"bytes_sent"`
		AvgReceiveBytes  int64 `json:"avg_receive_bytes"`
		AvgSendBytes     int64 `json:"avg_send_bytes"`
		Queued           int64 `json:"queued"`
		Dropped          int64 `json:"dropped"`
	}

	AcceptOptions struct {
		Conn       Conn
		Meta       *infra.Meta
		Name       string
		Site       string
		Host       string
		Domain     string
		RootDomain string
		Path       string
		Uri        string
		Setting    Map
		Params     Map
		Query      Map
		Form       Map
		Value      Map
		Args       Map
		Locals     Map
	}

	Session struct {
		ID         string         `json:"id"`
		User       string         `json:"user,omitempty"`
		Name       string         `json:"name"`
		Site       string         `json:"site,omitempty"`
		Host       string         `json:"host,omitempty"`
		Domain     string         `json:"domain,omitempty"`
		RootDomain string         `json:"root_domain,omitempty"`
		Path       string         `json:"path,omitempty"`
		Uri        string         `json:"uri,omitempty"`
		Setting    Map            `json:"setting,omitempty"`
		Params     Map            `json:"params,omitempty"`
		Query      Map            `json:"query,omitempty"`
		Form       Map            `json:"form,omitempty"`
		Value      Map            `json:"value,omitempty"`
		Args       Map            `json:"args,omitempty"`
		Locals     Map            `json:"locals,omitempty"`
		Groups     map[string]Any `json:"groups,omitempty"`

		Meta *infra.Meta `json:"-"`
		Conn Conn        `json:"-"`

		writeMutex sync.Mutex
		closeOnce  sync.Once
		closed     chan struct{}
		sendQueue  chan preparedFrame
	}

	frameEnvelope struct {
		Msg  string `json:"msg"`
		Args Map    `json:"args,omitempty"`
		Data Map    `json:"data,omitempty"`
	}

	dispatchEnvelope struct {
		Op   string `json:"op"`
		Sid  string `json:"sid,omitempty"`
		Uid  string `json:"uid,omitempty"`
		Gid  string `json:"gid,omitempty"`
		Msg  string `json:"msg"`
		Args Map    `json:"args,omitempty"`
	}

	preparedFrame struct {
		Name     string
		Command  Command
		Raw      Map
		Args     Map
		Code     int
		Text     string
		Time     int64
		Priority string
		Policy   string
		Data     []byte
		HasCmd   bool
		Err      error
	}

	wsStats struct {
		received      atomic.Int64
		sent          atomic.Int64
		receiveFailed atomic.Int64
		sendFailed    atomic.Int64
		bytesReceived atomic.Int64
		bytesSent     atomic.Int64
		queued        atomic.Int64
		dropped       atomic.Int64
	}
)

func (m *Module) Register(name string, value Any) {
	switch v := value.(type) {
	case Hook:
		m.RegisterHook(name, v)
	case Hooks:
		m.RegisterHooks(v)
	case Filter:
		m.RegisterFilter(name, v)
	case Filters:
		m.RegisterFilters(v)
	case Handler:
		m.RegisterHandler(name, v)
	case Handlers:
		m.RegisterHandlers(v)
	case Message:
		m.RegisterMessage(name, v)
	case Messages:
		m.RegisterMessages(name, v)
	case Command:
		m.RegisterCommand(name, v)
	case Commands:
		m.RegisterCommands(name, v)
	}
}

func (m *Module) Config(global Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	cfg := m.config
	if cfg.Format == "" {
		cfg.Format = "text"
	}
	if cfg.Codec == "" {
		cfg.Codec = infra.JSON
	}
	if cfg.MessageKey == "" {
		cfg.MessageKey = "name"
	}
	if cfg.PayloadKey == "" {
		cfg.PayloadKey = "data"
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = time.Second * 30
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = time.Second * 75
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = time.Second * 10
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 4 << 20
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 128
	}
	if cfg.QueuePolicy == "" {
		cfg.QueuePolicy = "close"
	}

	if raw, ok := global["ws"].(Map); ok && raw != nil {
		if format, ok := raw["format"].(string); ok {
			cfg.Format = strings.TrimSpace(strings.ToLower(format))
		}
		if codec, ok := raw["codec"].(string); ok {
			cfg.Codec = strings.TrimSpace(strings.ToLower(codec))
		}
		if key, ok := raw["message_key"].(string); ok {
			cfg.MessageKey = strings.TrimSpace(strings.ToLower(key))
		}
		if key, ok := raw["payload_key"].(string); ok {
			cfg.PayloadKey = strings.TrimSpace(strings.ToLower(key))
		}
		if value, ok := parseDuration(raw["ping_interval"]); ok {
			cfg.PingInterval = value
		}
		if value, ok := parseDuration(raw["read_timeout"]); ok {
			cfg.ReadTimeout = value
		}
		if value, ok := parseDuration(raw["write_timeout"]); ok {
			cfg.WriteTimeout = value
		}
		if value, ok := parseInt64(raw["max_message_size"]); ok {
			cfg.MaxMessageSize = value
		}
		if value, ok := parseInt64(raw["queue_size"]); ok {
			cfg.QueueSize = int(value)
		}
		if value, ok := raw["queue_policy"].(string); ok {
			cfg.QueuePolicy = strings.TrimSpace(strings.ToLower(value))
		}
		if value, ok := raw["compression"].(bool); ok {
			cfg.Compression = value
		}
		if value, ok := parseInt64(raw["compress_level"]); ok {
			cfg.CompressLevel = int(value)
		}
		if value, ok := parseDuration(raw["observe_interval"]); ok {
			cfg.ObserveInterval = value
		}
		if value, ok := raw["observe_log"].(bool); ok {
			cfg.ObserveLog = value
		}
		if value, ok := raw["observe_trace"].(bool); ok {
			cfg.ObserveTrace = value
		}
		if setting, ok := raw["setting"].(Map); ok && setting != nil {
			cfg.Setting = cloneMap(setting)
		}
	}

	m.config = normalizeConfig(cfg)
}

func (m *Module) Setup() {}

func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.opened = true
	m.config = normalizeConfig(m.config)
	m.filterList = orderedValues(m.filters)
	m.handlerList = orderedValues(m.handlers)
	m.hookList = orderedValues(m.hooks)
}

func (m *Module) Start() {
	m.startObserve()
}

func (m *Module) Stop() {
	m.stopObserve()
}

func (m *Module) Close() {
	for _, session := range m.sessionsSnapshot() {
		_ = session.close()
	}
}

func (m *Module) RegisterHook(name string, hook Hook) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}

	name = normalizeName(name, hook.Name)
	if name == "" {
		return
	}
	if hook.Open == nil && hook.Close == nil && hook.Receive == nil && hook.Send == nil {
		return
	}
	storeNamed(m.hooks, name, hook)
}

func (m *Module) RegisterHooks(items Hooks) {
	for name, hook := range items {
		m.RegisterHook(name, hook)
	}
}

func (m *Module) RegisterFilter(name string, filter Filter) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}

	name = normalizeName(name, filter.Name)
	if name == "" || filter.Message == nil {
		return
	}
	storeNamed(m.filters, name, filter)
}

func (m *Module) RegisterFilters(items Filters) {
	for name, filter := range items {
		m.RegisterFilter(name, filter)
	}
}

func (m *Module) RegisterHandler(name string, handler Handler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}

	name = normalizeName(name, handler.Name)
	if name == "" {
		return
	}
	if handler.Error == nil && handler.Invalid == nil && handler.Denied == nil {
		return
	}
	storeNamed(m.handlers, name, handler)
}

func (m *Module) RegisterHandlers(items Handlers) {
	for name, handler := range items {
		m.RegisterHandler(name, handler)
	}
}

func (m *Module) RegisterMessage(name string, message Message) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}

	name = normalizeName(name, message.Name)
	if name == "" || message.Action == nil {
		return
	}
	message.Name = pickString(message.Name, name)
	storeNamed(m.messages, name, message)
}

func (m *Module) RegisterMessages(prefix string, items Messages) {
	for name, message := range items {
		target := name
		if prefix != "" {
			target = prefix + "." + name
		}
		m.RegisterMessage(target, message)
	}
}

func (m *Module) RegisterCommand(name string, command Command) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}

	name = normalizeName(name, command.Name)
	if name == "" {
		return
	}
	command.Name = pickString(command.Name, name)
	storeNamed(m.commands, name, command)
}

func (m *Module) RegisterCommands(prefix string, items Commands) {
	for name, command := range items {
		target := name
		if prefix != "" {
			target = prefix + "." + name
		}
		m.RegisterCommand(target, command)
	}
}

func Accept(opts AcceptOptions) error {
	if opts.Conn == nil {
		return errors.New("invalid websocket connection")
	}

	session := &Session{
		ID:         infra.GenerateTokenID(),
		User:       inferUserID(opts.Meta, opts.Args, opts.Value),
		Name:       strings.TrimSpace(strings.ToLower(opts.Name)),
		Site:       strings.TrimSpace(strings.ToLower(opts.Site)),
		Host:       opts.Host,
		Domain:     opts.Domain,
		RootDomain: opts.RootDomain,
		Path:       opts.Path,
		Uri:        opts.Uri,
		Setting:    cloneMap(opts.Setting),
		Params:     cloneMap(opts.Params),
		Query:      cloneMap(opts.Query),
		Form:       cloneMap(opts.Form),
		Value:      cloneMap(opts.Value),
		Args:       cloneMap(opts.Args),
		Locals:     cloneMap(opts.Locals),
		Groups:     make(map[string]Any),
		Meta:       cloneMeta(opts.Meta),
		Conn:       opts.Conn,
		closed:     make(chan struct{}),
	}
	cfg := module.configSnapshot()
	if cfg.QueueSize > 0 {
		session.sendQueue = make(chan preparedFrame, cfg.QueueSize)
	}

	module.registerSession(session)
	defer module.unregisterSession(session)
	defer func() { _ = session.close() }()
	module.configureSession(session)
	module.startWriter(session)
	module.startKeepalive(session)

	openCtx := module.newContext(session, opOpen)
	module.callHooks(openCtx, func(h Hook) ctxFunc { return h.Open })

	for {
		messageType, payload, err := session.Conn.ReadMessage()
		if err != nil {
			module.stats.receiveFailed.Add(1)
			closeCtx := module.newContext(session, opClose)
			closeCtx.Input = nil
			closeCtx.Type = messageType
			closeCtx.Error(infra.Fail.With(err.Error()))
			module.callHooks(closeCtx, func(h Hook) ctxFunc { return h.Close })
			return nil
		}

		receiveCtx := module.newContext(session, opReceive)
		receiveCtx.Type = messageType
		receiveCtx.Input = append([]byte(nil), payload...)
		module.stats.received.Add(1)
		module.stats.bytesReceived.Add(int64(len(payload)))
		module.callHooks(receiveCtx, func(h Hook) ctxFunc { return h.Receive })

		if messageType == CloseMessage {
			closeCtx := module.newContext(session, opClose)
			closeCtx.Type = messageType
			closeCtx.Input = append([]byte(nil), payload...)
			module.callHooks(closeCtx, func(h Hook) ctxFunc { return h.Close })
			return nil
		}
		if messageType == PingMessage || messageType == PongMessage {
			continue
		}

		module.handlePayload(session, messageType, payload)
	}
}

func Push(sid, msg string, values ...Map) error {
	return module.push(nil, sid, msg, pickMap(values...))
}

func PushResult(sid, msg string, values ...Map) Delivery {
	return module.deliverSession(nil, sid, msg, pickMap(values...))
}

func PushUser(uid, msg string, values ...Map) error {
	return module.pushUser(nil, uid, msg, pickMap(values...))
}

func PushUserResult(uid, msg string, values ...Map) Delivery {
	return module.deliverUser(nil, uid, msg, pickMap(values...))
}

func Broadcast(msg string, values ...Map) error {
	return module.broadcast(nil, msg, pickMap(values...))
}

func BroadcastResult(msg string, values ...Map) Delivery {
	return module.deliverBroadcast(nil, msg, pickMap(values...))
}

func Groupcast(gid, msg string, values ...Map) error {
	return module.groupcast(nil, gid, msg, pickMap(values...))
}

func GroupcastResult(gid, msg string, values ...Map) Delivery {
	return module.deliverGroup(nil, gid, msg, pickMap(values...))
}

func SessionByID(id string) *Session {
	return module.sessionByID(id)
}

func Sessions() []*Session {
	return module.sessionsSnapshot()
}

func SessionsByUser(uid string) []*Session {
	return module.userSessions(uid)
}

func Export() Map {
	return module.export()
}

func Metrics() Stats {
	return module.metrics()
}

func CompressionEnabled() bool {
	return module.configSnapshot().Compression
}

func CompressionLevel() int {
	return module.configSnapshot().CompressLevel
}

func (m *Module) push(meta *infra.Meta, sid, msg string, value Map) error {
	sid = strings.TrimSpace(sid)
	msg = strings.TrimSpace(strings.ToLower(msg))
	if sid == "" || msg == "" {
		return errors.New("invalid ws push target")
	}
	if meta != nil {
		return meta.Broadcast(internalDispatch, Map{
			"op":   dispatchPush,
			"sid":  sid,
			"msg":  msg,
			"args": cloneMap(value),
		})
	}
	return infra.Broadcast(internalDispatch, Map{
		"op":   dispatchPush,
		"sid":  sid,
		"msg":  msg,
		"args": cloneMap(value),
	})
}

func (m *Module) pushUser(meta *infra.Meta, uid, msg string, value Map) error {
	uid = strings.TrimSpace(uid)
	msg = strings.TrimSpace(strings.ToLower(msg))
	if uid == "" || msg == "" {
		return errors.New("invalid ws push user target")
	}
	if meta != nil {
		return meta.Broadcast(internalDispatch, Map{
			"op":   dispatchPushUser,
			"uid":  uid,
			"msg":  msg,
			"args": cloneMap(value),
		})
	}
	return infra.Broadcast(internalDispatch, Map{
		"op":   dispatchPushUser,
		"uid":  uid,
		"msg":  msg,
		"args": cloneMap(value),
	})
}

func (m *Module) broadcast(meta *infra.Meta, msg string, value Map) error {
	msg = strings.TrimSpace(strings.ToLower(msg))
	if msg == "" {
		return errors.New("invalid ws broadcast message")
	}
	if meta != nil {
		return meta.Broadcast(internalDispatch, Map{
			"op":   dispatchBroadcast,
			"msg":  msg,
			"args": cloneMap(value),
		})
	}
	return infra.Broadcast(internalDispatch, Map{
		"op":   dispatchBroadcast,
		"msg":  msg,
		"args": cloneMap(value),
	})
}

func (m *Module) groupcast(meta *infra.Meta, gid, msg string, value Map) error {
	gid = strings.TrimSpace(gid)
	msg = strings.TrimSpace(strings.ToLower(msg))
	if gid == "" || msg == "" {
		return errors.New("invalid ws groupcast target")
	}
	if meta != nil {
		return meta.Broadcast(internalDispatch, Map{
			"op":   dispatchGroupcast,
			"gid":  gid,
			"msg":  msg,
			"args": cloneMap(value),
		})
	}
	return infra.Broadcast(internalDispatch, Map{
		"op":   dispatchGroupcast,
		"gid":  gid,
		"msg":  msg,
		"args": cloneMap(value),
	})
}

func (m *Module) newContext(session *Session, op string) *Context {
	meta := cloneMeta(session.Meta)
	return &Context{
		Meta:    meta,
		Session: session,
		Conn:    session.Conn,
		Op:      op,
		Setting: cloneMap(session.Setting),
		Value:   cloneMap(session.Value),
		Args:    cloneMap(session.Args),
		Locals:  cloneMap(session.Locals),
	}
}

func (m *Module) handlePayload(session *Session, messageType int, payload []byte) {
	frame, err := m.unmarshalFrameMap(payload)
	if err != nil {
		ctx := m.newContext(session, opMessage)
		ctx.Type = messageType
		ctx.Input = append([]byte(nil), payload...)
		ctx.Invalid(infra.Invalid.With(err.Error()))
		m.handle(ctx)
		return
	}

	name, raw := m.parseIncomingFrame(frame)
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		ctx := m.newContext(session, opMessage)
		ctx.Type = messageType
		ctx.Input = append([]byte(nil), payload...)
		ctx.Invalid(infra.Invalid.With("invalid ws message"))
		m.handle(ctx)
		return
	}
	if raw == nil {
		raw = Map{}
	}

	cfg, ok := m.message(name)
	if !ok {
		ctx := m.newContext(session, opMessage)
		ctx.Name = name
		ctx.Type = messageType
		ctx.Input = append([]byte(nil), payload...)
		ctx.Value = raw
		ctx.Invalid(infra.Invalid.With("unknown ws message: %s", name))
		m.handle(ctx)
		return
	}

	ctx := m.newContext(session, opMessage)
	ctx.Name = name
	ctx.Message = cfg
	ctx.Type = messageType
	ctx.Input = append([]byte(nil), payload...)
	ctx.Value = raw

	if cfg.Args != nil {
		args := Map{}
		res := infra.Mapping(cfg.Args, raw, args, cfg.Nullable, false, ctx.Timezone())
		if res != nil && res.Fail() {
			ctx.Invalid(res)
			m.handle(ctx)
			return
		}
		ctx.Args = args
	}

	ctx.next(m.filterChain()...)
	ctx.next(cfg.Action)

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				ctx.Error(infra.Fail.With("ws panic: %v", rec))
			}
		}()
		ctx.Next()
	}()

	if ctx.handling != "" {
		m.handle(ctx)
	}
}

func (m *Module) handle(ctx *Context) {
	if ctx == nil || ctx.handling == "" {
		return
	}

	for _, handler := range m.handlerChain() {
		switch ctx.handling {
		case "invalid":
			if handler.Invalid != nil {
				handler.Invalid(ctx)
			}
		case "denied":
			if handler.Denied != nil {
				handler.Denied(ctx)
			}
		default:
			if handler.Error != nil {
				handler.Error(ctx)
			}
		}
	}
}

func (m *Module) sendLocal(meta *infra.Meta, session *Session, msg string, value Map) error {
	result := m.deliverPrepared(session, m.prepareFrame(meta, session, msg, value, nil))
	if result.FirstError != "" {
		return errors.New(result.FirstError)
	}
	return nil
}

func (m *Module) deliverSession(meta *infra.Meta, sid, msg string, value Map) Delivery {
	session := m.sessionByID(sid)
	return m.deliverPrepared(session, m.prepareFrame(meta, session, msg, value, nil))
}

func (m *Module) deliverUser(meta *infra.Meta, uid, msg string, value Map) Delivery {
	result := Delivery{}
	for _, session := range m.userSessions(uid) {
		result.merge(m.deliverPrepared(session, m.prepareFrame(meta, session, msg, value, nil)))
	}
	return result
}

func (m *Module) deliverBroadcast(meta *infra.Meta, msg string, value Map) Delivery {
	return m.deliverMany(meta, m.sessionsSnapshot(), msg, value)
}

func (m *Module) deliverGroup(meta *infra.Meta, gid, msg string, value Map) Delivery {
	return m.deliverMany(meta, m.groupSessions(gid), msg, value)
}

func (m *Module) deliverMany(meta *infra.Meta, sessions []*Session, msg string, value Map) Delivery {
	result := Delivery{}
	if len(sessions) == 0 {
		return result
	}

	prepared, ok := m.prepareSharedFrame(meta, sessions, msg, value)
	for _, session := range sessions {
		if ok {
			result.merge(m.deliverPrepared(session, prepared))
			continue
		}
		result.merge(m.deliverPrepared(session, m.prepareFrame(meta, session, msg, value, nil)))
	}
	return result
}

func (m *Module) prepareSharedFrame(meta *infra.Meta, sessions []*Session, msg string, value Map) (preparedFrame, bool) {
	if len(sessions) == 0 {
		return preparedFrame{}, false
	}

	name := strings.TrimSpace(strings.ToLower(msg))
	if name == "" {
		return preparedFrame{Name: ""}, true
	}

	command, ok := m.command(name)
	if !ok || command.Args == nil || meta != nil {
		first := sessions[0]
		return m.prepareFrame(meta, first, msg, value, nil), true
	}
	return preparedFrame{}, false
}

func (m *Module) prepareFrame(meta *infra.Meta, session *Session, msg string, value Map, res Res) preparedFrame {
	if session == nil {
		return preparedFrame{Err: errors.New("invalid ws session")}
	}

	name := strings.TrimSpace(strings.ToLower(msg))
	if name == "" {
		return preparedFrame{Err: errors.New("invalid ws command")}
	}

	raw := cloneMap(value)
	if raw == nil {
		raw = Map{}
	}

	args := raw
	command, ok := m.command(name)
	priority := "normal"
	policy := ""
	if ok {
		if value, ok := command.Setting["priority"].(string); ok {
			priority = strings.TrimSpace(strings.ToLower(value))
		}
		if value, ok := command.Setting["queue_policy"].(string); ok {
			policy = strings.TrimSpace(strings.ToLower(value))
		}
	}
	if ok && command.Args != nil {
		mapped := Map{}
		useMeta := cloneMeta(meta)
		if useMeta == nil {
			useMeta = cloneMeta(session.Meta)
		}
		if useMeta == nil {
			useMeta = infra.NewMeta()
		}
		res := infra.Mapping(command.Args, raw, mapped, command.Nullable, false, useMeta.Timezone())
		if res != nil && res.Fail() {
			return preparedFrame{Name: name, Raw: raw, HasCmd: ok, Command: command, Err: errors.New(res.Error())}
		}
		args = mapped
	}

	code := 0
	text := ""
	if res != nil {
		code = res.Code()
		useMeta := cloneMeta(meta)
		if useMeta == nil {
			useMeta = cloneMeta(session.Meta)
		}
		if useMeta != nil {
			text = useMeta.String(res.Status(), res.Args()...)
		} else {
			text = res.Error()
		}
	}
	data, err := m.marshalFrame(m.buildOutgoingFrame(name, args, code, text))
	if err != nil {
		return preparedFrame{Name: name, Raw: raw, Args: args, Code: code, Text: text, HasCmd: ok, Command: command, Err: err}
	}

	return preparedFrame{
		Name:     name,
		Command:  command,
		Raw:      raw,
		Args:     args,
		Code:     code,
		Text:     text,
		Time:     time.Now().Unix(),
		Priority: priority,
		Policy:   policy,
		Data:     data,
		HasCmd:   ok,
	}
}

func (m *Module) deliverPrepared(session *Session, prepared preparedFrame) Delivery {
	result := Delivery{}
	if session == nil {
		result.Failed = 1
		result.FirstError = "invalid ws session"
		return result
	}

	result.Hit = 1
	if prepared.Err != nil {
		result.Failed = 1
		result.FirstError = prepared.Err.Error()
		return result
	}

	if session.sendQueue == nil {
		if err := m.writePrepared(session, prepared); err != nil {
			result.Failed = 1
			result.FirstError = err.Error()
			m.unregisterSession(session)
			_ = session.close()
			return result
		}
		result.Success = 1
		return result
	}

	if err := m.enqueuePrepared(session, prepared); err != nil {
		result.Failed = 1
		result.FirstError = err.Error()
		return result
	}

	result.Success = 1
	return result
}

func (m *Module) enqueuePrepared(session *Session, prepared preparedFrame) error {
	if session == nil {
		return errors.New("invalid ws session")
	}
	if session.sendQueue == nil {
		return m.writePrepared(session, prepared)
	}

	cfg := m.configSnapshot()
	policy := resolveQueuePolicy(cfg, prepared)
	if policy == "drop" && shouldDropLowPriority(session, prepared) {
		m.stats.dropped.Add(1)
		m.stats.sendFailed.Add(1)
		return errors.New("ws send queue degraded")
	}
	switch policy {
	case "block":
		select {
		case <-session.closed:
			m.stats.sendFailed.Add(1)
			return errors.New("ws session closed")
		case session.sendQueue <- prepared:
			m.stats.queued.Add(1)
			return nil
		}
	case "drop":
		select {
		case <-session.closed:
			m.stats.sendFailed.Add(1)
			return errors.New("ws session closed")
		case session.sendQueue <- prepared:
			m.stats.queued.Add(1)
			return nil
		default:
			m.stats.dropped.Add(1)
			m.stats.sendFailed.Add(1)
			return errors.New("ws send queue full")
		}
	default:
		select {
		case <-session.closed:
			m.stats.sendFailed.Add(1)
			return errors.New("ws session closed")
		case session.sendQueue <- prepared:
			m.stats.queued.Add(1)
			return nil
		default:
			m.stats.dropped.Add(1)
			m.stats.sendFailed.Add(1)
			m.unregisterSession(session)
			_ = session.close()
			return errors.New("ws send queue full")
		}
	}
}

func resolveQueuePolicy(cfg Config, prepared preparedFrame) string {
	if prepared.Policy != "" {
		return prepared.Policy
	}
	switch prepared.Priority {
	case "high", "critical":
		return "block"
	case "low":
		return "drop"
	default:
		return cfg.QueuePolicy
	}
}

func shouldDropLowPriority(session *Session, prepared preparedFrame) bool {
	if session == nil || session.sendQueue == nil {
		return false
	}
	if prepared.Priority != "low" {
		return false
	}
	capacity := cap(session.sendQueue)
	if capacity <= 0 {
		return false
	}
	return len(session.sendQueue)*100/capacity >= 75
}

func (m *Module) startWriter(session *Session) {
	if session == nil || session.sendQueue == nil {
		return
	}

	go func() {
		for {
			select {
			case <-session.closed:
				return
			case prepared, ok := <-session.sendQueue:
				if !ok {
					return
				}
				if err := m.writePrepared(session, prepared); err != nil {
					m.unregisterSession(session)
					_ = session.close()
					return
				}
			}
		}
	}()
}

func (m *Module) writePrepared(session *Session, prepared preparedFrame) error {
	if session == nil {
		return errors.New("invalid ws session")
	}
	if prepared.Err != nil {
		return prepared.Err
	}

	ctx := m.newContext(session, opSend)
	ctx.Name = prepared.Name
	if prepared.HasCmd {
		ctx.Command = prepared.Command
	}
	ctx.Value = cloneMap(prepared.Raw)
	ctx.Args = cloneMap(prepared.Args)
	ctx.Output = append([]byte(nil), prepared.Data...)
	m.callHooks(ctx, func(h Hook) ctxFunc { return h.Send })

	cfg := m.configSnapshot()
	if err := m.writeFrame(session, m.frameType(), prepared.Data, cfg.WriteTimeout); err != nil {
		m.stats.sendFailed.Add(1)
		return err
	}
	m.stats.sent.Add(1)
	m.stats.bytesSent.Add(int64(len(prepared.Data)))
	return nil
}

func (m *Module) registerSession(session *Session) {
	m.sessionMutex.Lock()
	defer m.sessionMutex.Unlock()
	m.sessions[session.ID] = session
	if uid := strings.TrimSpace(session.User); uid != "" {
		if _, ok := m.users[uid]; !ok {
			m.users[uid] = make(map[string]*Session)
		}
		m.users[uid][session.ID] = session
	}
}

func (m *Module) unregisterSession(session *Session) {
	m.leaveAll(session)

	m.sessionMutex.Lock()
	if uid := strings.TrimSpace(session.User); uid != "" {
		if members, ok := m.users[uid]; ok {
			delete(members, session.ID)
			if len(members) == 0 {
				delete(m.users, uid)
			}
		}
	}
	delete(m.sessions, session.ID)
	m.sessionMutex.Unlock()
}

func (m *Module) leaveAll(session *Session) {
	if session == nil {
		return
	}

	m.sessionMutex.Lock()
	defer m.sessionMutex.Unlock()

	for gid := range session.Groups {
		if members, ok := m.groups[gid]; ok {
			delete(members, session.ID)
			if len(members) == 0 {
				delete(m.groups, gid)
			}
		}
		delete(session.Groups, gid)
	}
}

func (m *Module) join(session *Session, gid string) {
	if session == nil {
		return
	}

	gid = normalizeGroup(gid)
	if gid == "" {
		return
	}

	m.sessionMutex.Lock()
	defer m.sessionMutex.Unlock()

	if _, ok := m.groups[gid]; !ok {
		m.groups[gid] = make(map[string]*Session)
	}
	m.groups[gid][session.ID] = session
	session.Groups[gid] = true
}

func (m *Module) leave(session *Session, gid string) {
	if session == nil {
		return
	}

	gid = normalizeGroup(gid)
	if gid == "" {
		return
	}

	m.sessionMutex.Lock()
	defer m.sessionMutex.Unlock()

	if members, ok := m.groups[gid]; ok {
		delete(members, session.ID)
		if len(members) == 0 {
			delete(m.groups, gid)
		}
	}
	delete(session.Groups, gid)
}

func (m *Module) bindUser(session *Session, uid string) {
	if session == nil {
		return
	}

	uid = strings.TrimSpace(uid)

	m.sessionMutex.Lock()
	defer m.sessionMutex.Unlock()

	if current := strings.TrimSpace(session.User); current != "" {
		if members, ok := m.users[current]; ok {
			delete(members, session.ID)
			if len(members) == 0 {
				delete(m.users, current)
			}
		}
	}

	session.User = uid
	if uid == "" {
		return
	}
	if _, ok := m.users[uid]; !ok {
		m.users[uid] = make(map[string]*Session)
	}
	m.users[uid][session.ID] = session
}

func (m *Module) sessionGroups(session *Session) []string {
	if session == nil {
		return nil
	}

	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()

	out := make([]string, 0, len(session.Groups))
	for gid := range session.Groups {
		out = append(out, gid)
	}
	sort.Strings(out)
	return out
}

func (m *Module) inGroup(session *Session, gid string) bool {
	if session == nil {
		return false
	}

	gid = normalizeGroup(gid)
	if gid == "" {
		return false
	}

	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()
	_, ok := session.Groups[gid]
	return ok
}

func (m *Module) sessionByID(id string) *Session {
	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()
	return m.sessions[id]
}

func (m *Module) userSessions(uid string) []*Session {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil
	}

	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()

	members := m.users[uid]
	out := make([]*Session, 0, len(members))
	for _, session := range members {
		out = append(out, session)
	}
	return out
}

func (m *Module) userSnapshot() []string {
	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()

	out := make([]string, 0, len(m.users))
	for uid := range m.users {
		out = append(out, uid)
	}
	return out
}

func (m *Module) sessionsSnapshot() []*Session {
	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()

	out := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		out = append(out, session)
	}
	return out
}

func (m *Module) groupSessions(gid string) []*Session {
	gid = normalizeGroup(gid)
	if gid == "" {
		return nil
	}

	m.sessionMutex.RLock()
	defer m.sessionMutex.RUnlock()

	members := m.groups[gid]
	out := make([]*Session, 0, len(members))
	for _, session := range members {
		out = append(out, session)
	}
	return out
}

func (m *Module) filterChain() []ctxFunc {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	out := make([]ctxFunc, 0, len(m.filterList))
	for _, filter := range m.filterList {
		if filter.Message != nil {
			out = append(out, filter.Message)
		}
	}
	return out
}

func (m *Module) handlerChain() []Handler {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	out := make([]Handler, len(m.handlerList))
	copy(out, m.handlerList)
	return out
}

func (m *Module) callHooks(ctx *Context, pick func(Hook) ctxFunc) {
	m.mutex.RLock()
	hooks := make([]Hook, len(m.hookList))
	copy(hooks, m.hookList)
	m.mutex.RUnlock()

	for _, hook := range hooks {
		if fn := pick(hook); fn != nil {
			fn(ctx)
		}
	}
}

func (m *Module) message(name string) (Message, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	cfg, ok := m.messages[strings.TrimSpace(strings.ToLower(name))]
	return cfg, ok
}

func (m *Module) command(name string) (Command, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	cfg, ok := m.commands[strings.TrimSpace(strings.ToLower(name))]
	return cfg, ok
}

func (m *Module) export() Map {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	messages := Map{}
	for name, item := range m.messages {
		messages[name] = Map{
			"name":     item.Name,
			"desc":     item.Desc,
			"nullable": item.Nullable,
			"args":     exportVars(item.Args),
			"sample": Map{
				m.frameMessageKey(): name,
				m.framePayloadKey(): sampleVars(item.Args),
			},
			"setting": cloneMap(item.Setting),
		}
	}

	commands := Map{}
	for name, item := range m.commands {
		commands[name] = Map{
			"name":     item.Name,
			"desc":     item.Desc,
			"nullable": item.Nullable,
			"args":     exportVars(item.Args),
			"sample": Map{
				"code":              0,
				m.frameMessageKey(): name,
				m.framePayloadKey(): sampleVars(item.Args),
				"time":              0,
			},
			"setting": cloneMap(item.Setting),
		}
	}

	cfg := m.config
	return Map{
		"config": Map{
			"format":           cfg.Format,
			"codec":            cfg.Codec,
			"message_key":      m.frameMessageKey(),
			"payload_key":      m.framePayloadKey(),
			"queue_size":       cfg.QueueSize,
			"queue_policy":     cfg.QueuePolicy,
			"compression":      cfg.Compression,
			"compress_level":   cfg.CompressLevel,
			"ping_interval":    cfg.PingInterval.String(),
			"read_timeout":     cfg.ReadTimeout.String(),
			"write_timeout":    cfg.WriteTimeout.String(),
			"max_message_size": cfg.MaxMessageSize,
		},
		"envelope": Map{
			"request": Map{
				"compat_message_keys": []string{"name", "msg"},
				"compat_payload_keys": []string{"data", "args"},
				"default": Map{
					m.frameMessageKey(): "demo.message",
					m.framePayloadKey(): Map{"field": "value"},
				},
			},
			"response": Map{
				"default": Map{
					"code":              0,
					m.frameMessageKey(): "demo.command",
					m.framePayloadKey(): Map{"field": "value"},
					"text":              "失败时才返回",
					"time":              0,
				},
				"fields": Map{
					"code": "0 表示成功，其它表示失败",
					"text": "失败文案，成功时通常为空",
					"time": "服务器时间戳",
				},
			},
		},
		"errors":   wsErrorDocs(),
		"messages": messages,
		"commands": commands,
	}
}

func (m *Module) metrics() Stats {
	received := m.stats.received.Load()
	sent := m.stats.sent.Load()
	bytesReceived := m.stats.bytesReceived.Load()
	bytesSent := m.stats.bytesSent.Load()
	stats := Stats{
		Connections:      int64(len(m.sessionsSnapshot())),
		Users:            int64(len(m.userSnapshot())),
		MessagesReceived: received,
		MessagesSent:     sent,
		ReceiveFailed:    m.stats.receiveFailed.Load(),
		SendFailed:       m.stats.sendFailed.Load(),
		BytesReceived:    bytesReceived,
		BytesSent:        bytesSent,
		Queued:           m.stats.queued.Load(),
		Dropped:          m.stats.dropped.Load(),
	}
	if received > 0 {
		stats.AvgReceiveBytes = bytesReceived / received
	}
	if sent > 0 {
		stats.AvgSendBytes = bytesSent / sent
	}
	return stats
}

func (m *Module) startObserve() {
	cfg := m.configSnapshot()
	if cfg.ObserveInterval <= 0 || (!cfg.ObserveLog && !cfg.ObserveTrace) {
		return
	}

	m.mutex.Lock()
	if m.observeStop != nil {
		m.mutex.Unlock()
		return
	}
	stop := make(chan struct{})
	m.observeStop = stop
	m.mutex.Unlock()

	go func() {
		ticker := time.NewTicker(cfg.ObserveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m.reportMetrics()
			}
		}
	}()
}

func (m *Module) stopObserve() {
	m.mutex.Lock()
	stop := m.observeStop
	m.observeStop = nil
	m.mutex.Unlock()

	if stop != nil {
		close(stop)
	}
}

func (m *Module) reportMetrics() {
	stats := m.metrics()
	fields := Map{
		"component":         "ws",
		"connections":       stats.Connections,
		"users":             stats.Users,
		"messages_received": stats.MessagesReceived,
		"messages_sent":     stats.MessagesSent,
		"receive_failed":    stats.ReceiveFailed,
		"send_failed":       stats.SendFailed,
		"bytes_received":    stats.BytesReceived,
		"bytes_sent":        stats.BytesSent,
		"avg_receive_bytes": stats.AvgReceiveBytes,
		"avg_send_bytes":    stats.AvgSendBytes,
		"queued":            stats.Queued,
		"dropped":           stats.Dropped,
	}

	cfg := m.configSnapshot()
	if cfg.ObserveLog {
		log.Infow("ws.metrics", fields)
	}
	if cfg.ObserveTrace {
		_ = infra.NewMeta().Trace("ws.metrics", fields)
	}
}

func registerInternalMessages() {
	host.RegisterLocal(internalDispatch, infra.Message{
		Name: "ws dispatch",
		Desc: "内部消息：ws 节点间分发",
		Action: func(ctx *infra.Context) Res {
			op, _ := ctx.Value["op"].(string)
			sid, _ := ctx.Value["sid"].(string)
			uid, _ := ctx.Value["uid"].(string)
			gid, _ := ctx.Value["gid"].(string)
			msg, _ := ctx.Value["msg"].(string)
			args := toMap(ctx.Value["args"])
			result := Delivery{}
			switch strings.TrimSpace(strings.ToLower(op)) {
			case dispatchPush:
				result = module.deliverSession(ctx.Meta, sid, msg, args)
			case dispatchPushUser:
				result = module.deliverUser(ctx.Meta, uid, msg, args)
			case dispatchGroupcast:
				result = module.deliverGroup(ctx.Meta, gid, msg, args)
			default:
				result = module.deliverBroadcast(ctx.Meta, msg, args)
			}
			if result.FirstError != "" {
				return infra.Fail.With(result.FirstError)
			}
			return infra.OK
		},
	})
}

func orderedValues[T any](items map[string]T) []T {
	if len(items) == 0 {
		return nil
	}

	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]T, 0, len(keys))
	for _, key := range keys {
		out = append(out, items[key])
	}
	return out
}

func (d *Delivery) merge(other Delivery) {
	d.Hit += other.Hit
	d.Success += other.Success
	d.Failed += other.Failed
	if d.FirstError == "" {
		d.FirstError = other.FirstError
	}
}

func normalizeConfig(cfg Config) Config {
	cfg.Format = strings.TrimSpace(strings.ToLower(cfg.Format))
	switch cfg.Format {
	case "binary":
	default:
		cfg.Format = "text"
	}
	cfg.Codec = strings.TrimSpace(strings.ToLower(cfg.Codec))
	if cfg.Codec == "" {
		cfg.Codec = infra.JSON
	}
	cfg.MessageKey = strings.TrimSpace(strings.ToLower(cfg.MessageKey))
	if cfg.MessageKey == "" {
		cfg.MessageKey = "name"
	}
	cfg.PayloadKey = strings.TrimSpace(strings.ToLower(cfg.PayloadKey))
	if cfg.PayloadKey == "" {
		cfg.PayloadKey = "data"
	}
	if cfg.PingInterval < 0 {
		cfg.PingInterval = 0
	}
	if cfg.ReadTimeout < 0 {
		cfg.ReadTimeout = 0
	}
	if cfg.WriteTimeout < 0 {
		cfg.WriteTimeout = 0
	}
	if cfg.MaxMessageSize < 0 {
		cfg.MaxMessageSize = 0
	}
	if cfg.ObserveInterval < 0 {
		cfg.ObserveInterval = 0
	}
	if cfg.PingInterval == 0 {
		cfg.ReadTimeout = 0
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = time.Second * 10
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 4 << 20
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 128
	}
	cfg.QueuePolicy = strings.TrimSpace(strings.ToLower(cfg.QueuePolicy))
	switch cfg.QueuePolicy {
	case "block", "drop", "close":
	default:
		cfg.QueuePolicy = "close"
	}
	if cfg.Setting == nil {
		cfg.Setting = Map{}
	}
	return cfg
}

func normalizeName(name, fallback string) string {
	name = pickString(name, fallback)
	return strings.TrimSpace(strings.ToLower(name))
}

func normalizeGroup(group string) string {
	return strings.TrimSpace(group)
}

func pickString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func pickMap(values ...Map) Map {
	if len(values) == 0 || values[0] == nil {
		return Map{}
	}
	return cloneMap(values[0])
}

func cloneMap(src Map) Map {
	if len(src) == 0 {
		return Map{}
	}
	dst := make(Map, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneMeta(src *infra.Meta) *infra.Meta {
	if src == nil {
		return infra.NewMeta()
	}
	dst := infra.NewMeta()
	dst.Metadata(src.Metadata())
	return dst
}

func (m *Module) configSnapshot() Config {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.config
}

func (m *Module) frameType() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if strings.TrimSpace(strings.ToLower(m.config.Format)) == "binary" {
		return BinaryMessage
	}
	return TextMessage
}

func (m *Module) frameCodec() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	codec := strings.TrimSpace(strings.ToLower(m.config.Codec))
	if codec == "" {
		return infra.JSON
	}
	return codec
}

func (m *Module) marshalFrame(value Any) ([]byte, error) {
	codec := m.frameCodec()
	if codec == infra.JSON {
		buf := jsonBufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer jsonBufferPool.Put(buf)

		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(value); err != nil {
			return nil, err
		}
		out := append([]byte(nil), buf.Bytes()...)
		if len(out) > 0 && out[len(out)-1] == '\n' {
			out = out[:len(out)-1]
		}
		return out, nil
	}
	return infra.Marshal(codec, value)
}

func (m *Module) unmarshalFrame(data []byte, value Any) error {
	codec := m.frameCodec()
	if codec == infra.JSON {
		return json.Unmarshal(data, value)
	}
	return infra.Unmarshal(codec, data, value)
}

func (m *Module) unmarshalFrameMap(data []byte) (Map, error) {
	codec := m.frameCodec()
	if codec == infra.JSON {
		raw := Map{}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		return raw, nil
	}

	raw := Map{}
	out, err := infra.Decode(codec, data, &raw)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		return raw, nil
	}
	return anyToMap(out), nil
}

func (m *Module) buildOutgoingFrame(name string, args Map, code int, text string) Map {
	frame := Map{}
	frame["code"] = code
	frame["time"] = time.Now().Unix()
	frame[m.frameMessageKey()] = name
	if len(args) > 0 {
		frame[m.framePayloadKey()] = args
	}
	if text != "" {
		frame["text"] = text
	}
	return frame
}

func (m *Module) parseIncomingFrame(frame Map) (string, Map) {
	if len(frame) == 0 {
		return "", Map{}
	}

	messageKeys := []string{m.frameMessageKey(), "msg", "name"}
	payloadKeys := []string{m.framePayloadKey(), "args", "data"}

	name := ""
	for _, key := range messageKeys {
		if key == "" {
			continue
		}
		if value, ok := frame[key].(string); ok && strings.TrimSpace(value) != "" {
			name = value
			break
		}
	}

	used := map[string]struct{}{}
	for _, key := range messageKeys {
		if key != "" {
			used[key] = struct{}{}
		}
	}
	for _, key := range payloadKeys {
		if key != "" {
			used[key] = struct{}{}
		}
	}

	for _, key := range payloadKeys {
		if key == "" {
			continue
		}
		if value, ok := frame[key]; ok {
			if mapped := anyToMap(value); len(mapped) > 0 {
				return name, mapped
			}
			return name, Map{key: value}
		}
	}

	args := Map{}
	for key, value := range frame {
		if _, ok := used[key]; ok {
			continue
		}
		args[key] = value
	}
	return name, args
}

func (m *Module) frameMessageKey() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if key := strings.TrimSpace(strings.ToLower(m.config.MessageKey)); key != "" {
		return key
	}
	return "name"
}

func (m *Module) framePayloadKey() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if key := strings.TrimSpace(strings.ToLower(m.config.PayloadKey)); key != "" {
		return key
	}
	return "data"
}

func toMap(value Any) Map {
	switch vv := value.(type) {
	case nil:
		return Map{}
	case Map:
		return cloneMap(vv)
	default:
		return Map{}
	}
}

func anyToMap(value Any) Map {
	switch vv := value.(type) {
	case nil:
		return Map{}
	case Map:
		return cloneMap(vv)
	}

	data, err := json.Marshal(value)
	if err != nil {
		return Map{}
	}
	out := Map{}
	if err := json.Unmarshal(data, &out); err != nil {
		return Map{}
	}
	return out
}

func exportVars(vars Vars) Map {
	if len(vars) == 0 {
		return Map{}
	}
	out := Map{}
	for key, item := range vars {
		out[key] = exportVar(item)
	}
	return out
}

func exportVar(item Var) Map {
	out := Map{
		"type":      item.Type,
		"required":  item.Required,
		"nullable":  item.Nullable,
		"name":      item.Name,
		"text":      item.Text,
		"default":   item.Default,
		"unique":    item.Unique,
		"check":     item.Check,
		"collation": item.Collation,
		"comment":   item.Comment,
		"encode":    item.Encode,
		"decode":    item.Decode,
		"setting":   cloneMap(item.Setting),
		"options":   cloneMap(item.Options),
	}
	if len(item.Children) > 0 {
		out["children"] = exportVars(item.Children)
	}
	return out
}

func sampleVars(vars Vars) Map {
	if len(vars) == 0 {
		return Map{}
	}
	out := Map{}
	for key, item := range vars {
		out[key] = sampleVar(item)
	}
	return out
}

func sampleVar(item Var) Any {
	if item.Default != nil {
		return item.Default
	}
	if len(item.Children) > 0 {
		return sampleVars(item.Children)
	}
	switch strings.TrimSpace(strings.ToLower(item.Type)) {
	case "bool", "boolean":
		return false
	case "int", "int32", "int64", "uint", "uint32", "uint64", "digit", "digits":
		return 0
	case "float", "float32", "float64", "number":
		return 0
	case "array", "slice", "list":
		return []Any{}
	case "json", "map", "object":
		return Map{}
	case "datetime", "date", "time":
		return time.Now().Unix()
	default:
		return ""
	}
}

func wsErrorDocs() []Map {
	base := []Res{infra.OK, infra.Fail, infra.Invalid, infra.Denied, infra.Unsigned, infra.Unauthed}
	out := make([]Map, 0, len(base))
	for _, item := range base {
		if item == nil {
			continue
		}
		out = append(out, Map{
			"code":   item.Code(),
			"status": item.Status(),
			"text":   item.Error(),
		})
	}
	return out
}

func parseDuration(value Any) (time.Duration, bool) {
	switch vv := value.(type) {
	case nil:
		return 0, false
	case time.Duration:
		return vv, true
	case int:
		return time.Second * time.Duration(vv), true
	case int32:
		return time.Second * time.Duration(vv), true
	case int64:
		return time.Second * time.Duration(vv), true
	case float32:
		return time.Second * time.Duration(vv), true
	case float64:
		return time.Second * time.Duration(vv), true
	case string:
		vv = strings.TrimSpace(vv)
		if vv == "" {
			return 0, false
		}
		if d, err := time.ParseDuration(vv); err == nil {
			return d, true
		}
	}
	return 0, false
}

func parseInt64(value Any) (int64, bool) {
	switch vv := value.(type) {
	case nil:
		return 0, false
	case int:
		return int64(vv), true
	case int32:
		return int64(vv), true
	case int64:
		return vv, true
	case float32:
		return int64(vv), true
	case float64:
		return int64(vv), true
	case string:
		vv = strings.TrimSpace(vv)
		if vv == "" {
			return 0, false
		}
		var out int64
		_, err := fmt.Sscan(vv, &out)
		return out, err == nil
	}
	return 0, false
}

func inferUserID(meta *infra.Meta, values ...Map) string {
	keys := []string{"uid", "user_id", "userid", "userId", "user"}
	if meta != nil {
		if payload := meta.Payload(); len(payload) > 0 {
			for _, key := range keys {
				if uid := anyToString(payload[key]); uid != "" {
					return uid
				}
			}
		}
	}
	for _, value := range values {
		for _, key := range keys {
			if uid := anyToString(value[key]); uid != "" {
				return uid
			}
		}
	}
	return ""
}

func anyToString(value Any) string {
	switch vv := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(vv)
	default:
		text := strings.TrimSpace(fmt.Sprint(vv))
		if text == "<nil>" {
			return ""
		}
		return text
	}
}

func storeNamed[T any](target map[string]T, name string, value T) {
	if infra.Override() {
		target[name] = value
		return
	}
	if _, ok := target[name]; !ok {
		target[name] = value
	}
}

func (m *Module) configureSession(session *Session) {
	if session == nil || session.Conn == nil {
		return
	}

	cfg := m.configSnapshot()
	if conn, ok := session.Conn.(readLimitConn); ok && cfg.MaxMessageSize > 0 {
		conn.SetReadLimit(cfg.MaxMessageSize)
	}
	if conn, ok := session.Conn.(readDeadlineConn); ok && cfg.ReadTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
	}
	if conn, ok := session.Conn.(pongHandlerConn); ok && cfg.ReadTimeout > 0 {
		conn.SetPongHandler(func(string) error {
			if use, ok := session.Conn.(readDeadlineConn); ok {
				return use.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
			}
			return nil
		})
	}
	if conn, ok := session.Conn.(compressionConn); ok && cfg.Compression {
		conn.EnableWriteCompression(true)
		if cfg.CompressLevel != 0 {
			_ = conn.SetCompressionLevel(cfg.CompressLevel)
		}
	}
}

func (m *Module) startKeepalive(session *Session) {
	if session == nil || session.Conn == nil {
		return
	}

	cfg := m.configSnapshot()
	if cfg.PingInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(cfg.PingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-session.closed:
				return
			case <-ticker.C:
				if err := m.writeFrame(session, PingMessage, nil, cfg.WriteTimeout); err != nil {
					m.unregisterSession(session)
					_ = session.close()
					return
				}
			}
		}
	}()
}

func (m *Module) writeFrame(session *Session, messageType int, data []byte, timeout time.Duration) error {
	if session == nil || session.Conn == nil {
		return errors.New("invalid ws session")
	}

	session.writeMutex.Lock()
	defer session.writeMutex.Unlock()

	if conn, ok := session.Conn.(writeDeadlineConn); ok {
		if timeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		} else {
			_ = conn.SetWriteDeadline(time.Time{})
		}
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	}

	return session.Conn.WriteMessage(messageType, data)
}

func (s *Session) close() error {
	if s == nil || s.Conn == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if s.closed != nil {
			close(s.closed)
		}
		err = s.Conn.Close()
	})
	return err
}
