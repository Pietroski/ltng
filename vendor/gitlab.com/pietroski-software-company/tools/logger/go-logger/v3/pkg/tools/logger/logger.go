package go_logger

import (
	"context"
	"fmt"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"
	"log"
	"runtime"
	"strings"
	"sync"

	colour_models "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/models/colours"
	prefix_models "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/models/prefix"
	"gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger/dyer"
	"gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger/encapsulator"
	go_publisher "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/publisher"
)

// TODO: feature-flag debug publishing

type (
	Logger interface {
		Errorf(string, ...Field)
		Warningf(string, ...Field)
		Infof(string, ...Field)
		Debugf(string, ...Field)

		FromCtx(ctx context.Context) Logger
	}

	GoLogger struct {
		ctx       context.Context
		mtx       *sync.RWMutex
		ctxTracer go_tracer.Tracer
		isFromCtx bool

		encapsulator encapsulator.Encapsulator
		dyer         dyer.Dyer
		beautifier   go_serializer.Beautifier

		publishers *Publishers

		options *Opts
	}

	Publishers map[string]go_publisher.Publisher
	Field      map[string]interface{}

	Opts struct {
		Debug   bool
		Publish bool
	}
)

func NewGoLogger(
	ctx context.Context,
	publishers *Publishers,
	options *Opts,
) Logger {
	gl := &GoLogger{
		ctx:       ctx,
		mtx:       &sync.RWMutex{},
		ctxTracer: go_tracer.NewCtxTracer(),

		encapsulator: encapsulator.NewEncapsulator(),
		dyer:         dyer.NewDyer(),
		beautifier:   go_serializer.NewJsonBeautifier(),

		publishers: publishers,

		options: options,
	}

	return gl
}

func NewDefaultOpts() *Opts {
	return &Opts{
		Debug:   true,
		Publish: true,
	}
}

func FromCtx(ctx context.Context) Logger {
	gl := &GoLogger{
		ctx:       ctx,
		mtx:       &sync.RWMutex{},
		ctxTracer: go_tracer.NewCtxTracer(),
		isFromCtx: true,

		encapsulator: encapsulator.NewEncapsulator(),
		dyer:         dyer.NewDyer(),
		beautifier:   go_serializer.NewJsonBeautifier(),

		options: NewDefaultOpts(),
	}

	return gl
}

func (gl *GoLogger) FromCtx(ctx context.Context) Logger {
	newGl := &GoLogger{
		ctx:       ctx,
		mtx:       gl.mtx,
		ctxTracer: gl.ctxTracer,
		isFromCtx: true,

		encapsulator: gl.encapsulator,
		dyer:         gl.dyer,
		beautifier:   gl.beautifier,

		publishers: gl.publishers,

		options: gl.options,
	}

	return newGl
}

func (gl *GoLogger) Errorf(s string, ff ...Field) {
	formattedStr := gl.fmtStr(colour_models.CL.Red, prefix_models.ErrPrefix, s, ff...)
	log.Println(formattedStr)
}

func (gl *GoLogger) Warningf(s string, ff ...Field) {
	formattedStr := gl.fmtStr(colour_models.CL.Yellow, prefix_models.WarnPrefix, s, ff...)
	log.Println(formattedStr)
}

func (gl *GoLogger) Infof(s string, ff ...Field) {
	formattedStr := gl.fmtStr(colour_models.CL.Reset, prefix_models.InfoPrefix, s, ff...)
	log.Println(formattedStr)
}

func (gl *GoLogger) Debugf(s string, ff ...Field) {
	if !gl.options.Debug {
		return
	}

	formattedStr := gl.fmtStr(colour_models.CL.Blue, prefix_models.DebugPrefix, s, ff...)
	log.Println(formattedStr)
}

func (gl *GoLogger) fmtStr(
	colour colour_models.Colour,
	prefix prefix_models.Prefix,
	s string, ff ...Field,
) (str string) {
	fields := gl.buildFromFields(ff...)
	formattedStr := fmt.Sprintf(s)
	encapsulatedStr := gl.encapsulator.Encapsule(prefix, formattedStr)
	colouredStr := gl.dyer.Dye(colour, encapsulatedStr)
	str = colouredStr
	beautifiedField, _ := gl.beautifyPayload(fields)
	str, _ = gl.buildStr(colouredStr, beautifiedField)

	// TODO: do call to outgoing channel

	return
}

func (gl *GoLogger) buildStr(
	colouredStr, beautifiedField string,
) (str string, err error) {
	var ctx context.Context
	isFromCtx := gl.fromCtx()
	if isFromCtx {
		ctx, str, err = gl.addTracing(gl.ctx, colouredStr, beautifiedField)
	} else {
		ctx, str, err = gl.addTracing(context.Background(), colouredStr, beautifiedField)
	}

	gl.setCtx(ctx)

	return
}

func (gl *GoLogger) addTracing(
	ctx context.Context,
	s, f string,
) (context.Context, string, error) {
	var err error
	ctx, err = gl.ctxTracer.Trace(ctx)
	if err != nil {
		//
	}

	var str string
	str, err = gl.ctxTracer.Wrap(ctx, s, f)
	if err != nil {
		err = fmt.Errorf("error wrapping: %v", err)
		gl.Errorf(
			"error to trace when beautifying payload",
			Field{"error": err.Error()},
		)

		return ctx, str, err
	}

	return ctx, str, nil
}

func (gl *GoLogger) beautifyPayload(payload interface{}) (str string, err error) {
	beautifiedField, err := gl.beautifier.Beautify(payload, "", "  ")
	if err != nil {
		traceInfo, ok := gl.ctxTracer.GetTraceInfo(gl.ctx)
		if !ok {
			gl.Warningf("no previous trace detected; creating a trace")
			var tracingErr error
			var ctx context.Context
			ctx, tracingErr = gl.ctxTracer.Trace(gl.ctx)
			if tracingErr != nil {
				tracingErr = fmt.Errorf("error tracing: %v", err)
				gl.Errorf(
					"error to trace when beautifying payload",
					Field{"error": err.Error()},
				)

				return str, tracingErr
			}

			gl.setCtx(ctx)

			traceInfo, _ = gl.ctxTracer.GetTraceInfo(gl.ctx)
		}

		gl.Errorf(
			"failed to marshal indent payload to be logged",
			Mapper("err", err.Error()),
			Mapper("message-trace-id", traceInfo.ID),
		)
	}
	str = string(beautifiedField)

	return
}

func (gl *GoLogger) buildFromFields(ff ...Field) Field {
	fields := make(Field)
	for _, f := range ff {
		for fk, fv := range f {
			fields[fk] = fv
		}
	}

	return fields
}

func Mapper(key string, value interface{}) Field {
	return map[string]interface{}{key: value}
}

// publish call the publish method of each publisher interface.
// TODO: implement publish retrial?
func (gl *GoLogger) publish(payload interface{}) {
	if gl.publishers == nil {
		return
	}

	for publisherName, publisher := range *gl.publishers {
		log.Printf("calling publisher - %v", publisherName)
		_ = publisher.Publish(payload)
	}
}

const (
	DataFieldConstraint = "fields:\n"
)

// Splitter splits the logged string into two,
// the logged pre-fields and the logged post-fields.
func Splitter(str string) string {
	splitStr := strings.Split(str, DataFieldConstraint)
	if len(splitStr) != 2 {
		return ""
	}

	return splitStr[1]
}

func (gl *GoLogger) setCtx(ctx context.Context) {
	for !gl.mtx.TryLock() {
		runtime.Gosched()
	}
	//gl.mtx.Lock()
	gl.ctx = ctx
	gl.mtx.Unlock()

	//gl.mtx.Lock()
	//gl.ctx = ctx
	//gl.mtx.Unlock()
}

func (gl *GoLogger) fromCtx() bool {
	gl.mtx.RLock()
	isFromCtx := gl.isFromCtx
	gl.mtx.RUnlock()

	return isFromCtx
}
