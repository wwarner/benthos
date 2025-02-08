package unmarshaller

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnmarshaler(t *testing.T) {
	type S struct {
		G string `json:"g_j" yaml:"g_y" parquet:"g_p"`
	}
	type ATestType struct {
		I int `json:"i_j" yaml:"i_y" parquet:"i_p"`
		S *S  `json: "s_j,omitempty" yaml: "s_y" parquet: "s_p,omitempty"`
	}
	for name, tst := range map[string]*ATestType{
		"frst": {100, &S{"100"}},
		"scnd": {200, &S{"200"}},
		"thrd": {300, nil},
		"frth": {},
	} {
		t.Run(name, func(t *testing.T) {
			// json
			var jsonEncoded bytes.Buffer
			err := jsonCodec.Encoder(&jsonEncoded).Encode(tst)
			require.NoError(t, err)
			jsonDecoded := &ATestType{}
			err = jsonCodec.Decoder(&jsonEncoded).Decode(jsonDecoded)
			require.NoError(t, err)
			require.Equal(t, tst, jsonDecoded)

			// yaml
			var yamlEncoded bytes.Buffer
			err = yamlCodec.Encoder(&yamlEncoded).Encode(tst)
			require.NoError(t, err)
			yamlDecoded := &ATestType{}
			err = yamlCodec.Decoder(&yamlEncoded).Decode(yamlDecoded)
			require.NoError(t, err)
			require.Equal(t, tst, yamlDecoded)

			// parquet
			parquetEncoded := new(bytes.Buffer)
			parquetCodec := NewParquetCodec(tst)
			err = parquetCodec.Encoder(parquetEncoded).Encode(tst)
			require.NoError(t, err)
			require.Greater(t, parquetEncoded.Len(), 0)
			parquetDecoded := &ATestType{}
			err = parquetCodec.Decoder(parquetEncoded).Decode(parquetDecoded)
			require.NoError(t, err)
			require.Equal(t, tst, parquetDecoded)
		})
	}
}
