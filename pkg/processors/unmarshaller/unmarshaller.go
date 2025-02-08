package unmarshaller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"gopkg.in/yaml.v3"
)

var (
	UNMARSHALLER_ALLOWED_ENCODINGS = map[string]Codec{
		"json": jsonCodec,
		"yaml": yamlCodec,
	}
	ErrMarshallerObjectNotFoundAtMetakey = fmt.Errorf("No object found at metakey")
	ErrUnmarshallerTypeExistence         = fmt.Errorf("Unrecognized type cannot be unmarshalled")
	ErrUnmarshallerUnsupportedEncoding   = fmt.Errorf("Unsupported encoding")
	unmarshallerTypeRegistry             = map[string]reflect.Type{
		"int":    reflect.TypeOf(0),
		"string": reflect.TypeOf(""),
	}
)

const (
	FIELD_UNMARSHALLER_ENCODING = "encoding"
	FIELD_UNMARSHALLER_METAKEY  = "metakey"
	FIELD_UNMARSHALLER_TYPE     = "type"
	MARSHALLER_PROCESSOR_NAME   = "marshal"
	UNMARSHALLER_PROCESSOR_NAME = "unmarshal"
)

func RegisterCodec(key string, codec Codec) {
	UNMARSHALLER_ALLOWED_ENCODINGS[key] = codec
}

type (
	Decoder interface {
		Decode(any) error
	}
	Encoder interface {
		Encode(any) error
	}
	Codec interface {
		Decoder(io.Reader) Decoder
		Encoder(io.Writer) Encoder
	}
)

type JsonCodec struct{}

func (c *JsonCodec) Decoder(r io.Reader) Decoder {
	return json.NewDecoder(r)
}

func (c *JsonCodec) Encoder(w io.Writer) Encoder {
	return json.NewEncoder(w)
}

var jsonCodec = &JsonCodec{}

type YamlCodec struct{}

func (c *YamlCodec) Decoder(r io.Reader) Decoder {
	return yaml.NewDecoder(r)
}

func (c *YamlCodec) Encoder(w io.Writer) Encoder {
	return yaml.NewEncoder(w)
}

var yamlCodec = &YamlCodec{}

type ParquetCodec struct {
	Schema *parquet.Schema
}

func NewParquetCodec(v any) *ParquetCodec {
	return &ParquetCodec{
		Schema: parquet.SchemaOf(v),
	}
}

type ParquetDecoder struct {
	r *parquet.Reader
}

func (d *ParquetDecoder) Decode(v any) error {
	return d.r.Read(v)
}

type ParquetEncoder struct {
	w *parquet.Writer
}

func (e *ParquetEncoder) Encode(v any) error {
	defer e.w.Close()
	return e.w.Write(v)
}

func (c *ParquetCodec) Decoder(r io.Reader) Decoder {
	buf, err := io.ReadAll(r)
	if err != nil {
		// gracefully degrade
		buf = []byte{}
	}
	ra := bytes.NewReader(buf)
	return &ParquetDecoder{
		r: parquet.NewReader(ra, c.Schema),
	}
}

func (c *ParquetCodec) Encoder(w io.Writer) Encoder {
	return &ParquetEncoder{
		w: parquet.NewWriter(w, c.Schema),
	}
}
