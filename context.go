package ws

import (
	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Context struct {
		*infra.Meta
		Session *Session
		Conn    Conn

		Op      string
		Type    int
		Name    string
		Message Message
		Command Command
		Setting Map
		Value   Map
		Args    Map
		Locals  Map
		Input   []byte
		Output  []byte

		index    int
		nexts    []ctxFunc
		handling string
	}

	ctxFunc func(*Context)
)

func (ctx *Context) clear() {
	ctx.index = 0
	ctx.nexts = make([]ctxFunc, 0)
}

func (ctx *Context) next(nexts ...ctxFunc) {
	ctx.nexts = append(ctx.nexts, nexts...)
}

func (ctx *Context) Next() {
	if len(ctx.nexts) <= ctx.index {
		return
	}
	next := ctx.nexts[ctx.index]
	ctx.index++
	if next != nil {
		next(ctx)
		return
	}
	ctx.Next()
}

func (ctx *Context) abort() {
	ctx.index = len(ctx.nexts)
}

func (ctx *Context) Error(args ...Res) {
	res := infra.Fail
	if len(args) > 0 && args[0] != nil {
		res = args[0]
	}
	ctx.Result(res)
	ctx.handling = "error"
	ctx.abort()
}

func (ctx *Context) Fail(args ...Res) {
	ctx.Error(args...)
}

func (ctx *Context) Invalid(args ...Res) {
	res := infra.Invalid
	if len(args) > 0 && args[0] != nil {
		res = args[0]
	}
	ctx.Result(res)
	ctx.handling = "invalid"
	ctx.abort()
}

func (ctx *Context) Deny(args ...Res) {
	res := infra.Denied
	if len(args) > 0 && args[0] != nil {
		res = args[0]
	}
	ctx.Result(res)
	ctx.handling = "denied"
	ctx.abort()
}

func (ctx *Context) Reply(msg string, values ...Map) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	return module.sendLocal(ctx.Meta, ctx.Session, msg, pickMap(values...))
}

func (ctx *Context) Answer(msg string, data Map, results ...Res) error {
	result := ctx.AnswerResult(msg, data, results...)
	if result.FirstError != "" {
		return infra.Fail.With(result.FirstError)
	}
	return nil
}

func (ctx *Context) ReplyResult(msg string, values ...Map) Delivery {
	if ctx == nil || ctx.Session == nil {
		return Delivery{}
	}
	return module.deliverPrepared(ctx.Session, module.prepareFrame(ctx.Meta, ctx.Session, msg, pickMap(values...), nil))
}

func (ctx *Context) AnswerResult(msg string, data Map, results ...Res) Delivery {
	if ctx == nil || ctx.Session == nil {
		return Delivery{}
	}
	var res Res
	if len(results) > 0 {
		res = results[0]
	}
	return module.deliverPrepared(ctx.Session, module.prepareFrame(ctx.Meta, ctx.Session, msg, data, res))
}

func (ctx *Context) Push(sid, msg string, values ...Map) error {
	if ctx == nil {
		return Push(sid, msg, values...)
	}
	return module.push(ctx.Meta, sid, msg, pickMap(values...))
}

func (ctx *Context) PushResult(sid, msg string, values ...Map) Delivery {
	if ctx == nil {
		return PushResult(sid, msg, values...)
	}
	return module.deliverSession(ctx.Meta, sid, msg, pickMap(values...))
}

func (ctx *Context) PushUser(uid, msg string, values ...Map) error {
	if ctx == nil {
		return PushUser(uid, msg, values...)
	}
	return module.pushUser(ctx.Meta, uid, msg, pickMap(values...))
}

func (ctx *Context) PushUserResult(uid, msg string, values ...Map) Delivery {
	if ctx == nil {
		return PushUserResult(uid, msg, values...)
	}
	return module.deliverUser(ctx.Meta, uid, msg, pickMap(values...))
}

func (ctx *Context) Broadcast(msg string, values ...Map) error {
	if ctx == nil {
		return Broadcast(msg, values...)
	}
	return module.broadcast(ctx.Meta, msg, pickMap(values...))
}

func (ctx *Context) BroadcastResult(msg string, values ...Map) Delivery {
	if ctx == nil {
		return BroadcastResult(msg, values...)
	}
	return module.deliverBroadcast(ctx.Meta, msg, pickMap(values...))
}

func (ctx *Context) Groupcast(gid, msg string, values ...Map) error {
	if ctx == nil {
		return Groupcast(gid, msg, values...)
	}
	return module.groupcast(ctx.Meta, gid, msg, pickMap(values...))
}

func (ctx *Context) GroupcastResult(gid, msg string, values ...Map) Delivery {
	if ctx == nil {
		return GroupcastResult(gid, msg, values...)
	}
	return module.deliverGroup(ctx.Meta, gid, msg, pickMap(values...))
}

func (ctx *Context) Join(groups ...string) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	for _, group := range groups {
		module.join(ctx.Session, group)
	}
}

func (ctx *Context) Leave(groups ...string) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	for _, group := range groups {
		module.leave(ctx.Session, group)
	}
}

func (ctx *Context) LeaveAll() {
	if ctx == nil || ctx.Session == nil {
		return
	}
	module.leaveAll(ctx.Session)
}

func (ctx *Context) InGroup(group string) bool {
	if ctx == nil || ctx.Session == nil {
		return false
	}
	return module.inGroup(ctx.Session, group)
}

func (ctx *Context) Groups() []string {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	return module.sessionGroups(ctx.Session)
}

func (ctx *Context) BindUser(uid string) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	module.bindUser(ctx.Session, uid)
}
