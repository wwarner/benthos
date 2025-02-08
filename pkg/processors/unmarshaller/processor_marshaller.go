package unmarshaller

import (
	"bytes"
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type MarshallerProcessor struct {
	Codec   Codec
	Metakey string
}

func marshallerConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField(FIELD_UNMARSHALLER_ENCODING).Description("encoding of input bytes").Default("json")).
		Field(service.NewStringField(FIELD_UNMARSHALLER_METAKEY).Description("metadata key for deserialized object"))
}

func newMarshaller(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	metakey, err := conf.FieldString(FIELD_UNMARSHALLER_METAKEY)
	if err != nil {
		return nil, err
	}
	enc, err := conf.FieldString(FIELD_UNMARSHALLER_ENCODING)
	if err != nil {
		return nil, err
	}
	codec, allowed := UNMARSHALLER_ALLOWED_ENCODINGS[enc]
	if !allowed {
		return nil, ErrUnmarshallerUnsupportedEncoding
	}
	return &MarshallerProcessor{
		Codec:   codec,
		Metakey: metakey,
	}, nil
}

func init() {
	err := service.RegisterProcessor(MARSHALLER_PROCESSOR_NAME, marshallerConfigSpec(), newMarshaller)
	if err != nil {
		panic(err)
	}
}

func (p *MarshallerProcessor) Process(_ context.Context, m *service.Message) (service.MessageBatch, error) {
	v, exists := m.MetaGetMut(p.Metakey)
	if !exists {
		return nil, ErrMarshallerObjectNotFoundAtMetakey
	}
	var encoded bytes.Buffer
	err := p.Codec.Encoder(&encoded).Encode(v)
	if err != nil {
		return nil, err
	}
	m.SetBytes(encoded.Bytes())
	return service.MessageBatch{m}, nil
}

func (p *MarshallerProcessor) Close(_ context.Context) error {
	return nil
}
