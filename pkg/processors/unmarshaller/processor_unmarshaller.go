package unmarshaller

import (
	"bytes"
	"context"
	"reflect"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type UnmarshallerProcessor struct {
	Codec   Codec
	Metakey string
	Type    reflect.Type
}

func unmarshallerConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField(FIELD_UNMARSHALLER_ENCODING).Description("encoding of input bytes").Default("json")).
		Field(service.NewStringField(FIELD_UNMARSHALLER_METAKEY).Description("metadata key for the deserialized object")).
		Field(service.NewStringField(FIELD_UNMARSHALLER_TYPE).Description("type of deserialized object, must match a corresponding call to UnmarshallerTypeRegistryAdd()"))
}

func UnmarshallerTypeRegistryAdd(name string, typ reflect.Type) {
	unmarshallerTypeRegistry[name] = typ
}

func newUnmarshaller(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	enc, err := conf.FieldString(FIELD_UNMARSHALLER_ENCODING)
	if err != nil {
		return nil, err
	}
	codec, allowed := UNMARSHALLER_ALLOWED_ENCODINGS[enc]
	if !allowed {
		return nil, ErrUnmarshallerUnsupportedEncoding
	}

	metakey, err := conf.FieldString(FIELD_UNMARSHALLER_METAKEY)
	if err != nil {
		return nil, err
	}

	typeName, err := conf.FieldString(FIELD_UNMARSHALLER_TYPE)
	if err != nil {
		return nil, err
	}
	typ, exists := unmarshallerTypeRegistry[typeName]
	if !exists {
		return nil, ErrUnmarshallerTypeExistence
	}

	return &UnmarshallerProcessor{
		Codec:   codec,
		Metakey: metakey,
		Type:    typ,
	}, nil
}

func init() {
	err := service.RegisterProcessor(UNMARSHALLER_PROCESSOR_NAME, unmarshallerConfigSpec(), newUnmarshaller)
	if err != nil {
		panic(err)
	}
}

func (p *UnmarshallerProcessor) Process(_ context.Context, m *service.Message) (service.MessageBatch, error) {
	encodedBytes, err := m.AsBytes()
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(encodedBytes)
	value := reflect.New(p.Type).Interface()
	err = p.Codec.Decoder(r).Decode(value)
	if err != nil {
		return nil, err
	}
	m.MetaSetMut(p.Metakey, value)
	return service.MessageBatch{m}, nil
}

func (p *UnmarshallerProcessor) Close(_ context.Context) error {
	return nil
}
