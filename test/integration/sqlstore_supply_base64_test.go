//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
)

func newBase64TestProvider(t *testing.T) (*gorm.DB, *sqlstore.Provider) {
	t.Helper()
	dsn := requireMySQL(t)
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return db, sqlstore.New(db)
}

// TestSQLStoreSupplyContentIsBase64AfterUpdate verifies the map-based update
// path persists opaque supply bytes as base64 text. The provider deliberately
// has no static encryption configured, so decoding the raw column must yield
// the updated plaintext exactly.
func TestSQLStoreSupplyContentIsBase64AfterUpdate(t *testing.T) {
	db, p := newBase64TestProvider(t)
	ctx := context.Background()
	assertContentColumnsAreLongtextBin(t, ctx, db)

	runID := time.Now().UnixNano()
	namespace := fmt.Sprintf("supply-b64-%d", runID)
	name := fmt.Sprintf("non-utf8-%d", runID)
	first := []byte{0xff, 0xfe, 0x00, 0x80, 'c', 'r', 'e', 'a', 't', 'e'}
	second := []byte{0x00, 0x81, 0xff, 'u', 'p', 'd', 'a', 't', 'e'}

	created, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: namespace,
		Name:      name,
		Content:   first,
	}, nil)
	if err != nil {
		t.Fatalf("PutSupply create: %v", err)
	}

	ifMatch := created.Revision
	if _, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: namespace,
		Name:      name,
		Content:   second,
	}, &ifMatch); err != nil {
		t.Fatalf("PutSupply update: %v", err)
	}

	got, err := p.GetSupply(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetSupply after update: %v", err)
	}
	if !bytes.Equal(got.Content, second) {
		t.Fatalf("GetSupply content = %x, want updated plaintext %x", got.Content, second)
	}

	var encoded string
	if err := p.DB().WithContext(ctx).Raw(
		"SELECT content FROM xflow_supplies WHERE namespace = ? AND name = ?",
		namespace, name,
	).Row().Scan(&encoded); err != nil {
		t.Fatalf("read raw xflow_supplies.content: %v", err)
	}
	stored, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("raw xflow_supplies.content is not valid base64: %v", err)
	}
	if !bytes.Equal(stored, second) {
		t.Fatalf("decoded raw content = %x, want updated plaintext %x", stored, second)
	}
}

// TestSQLStoreArtifactContentIsBase64RoundTrip verifies the other migrated
// content column keeps arbitrary artifact bytes as base64 text at rest and
// restores those bytes through the object-store interface.
func TestSQLStoreArtifactContentIsBase64RoundTrip(t *testing.T) {
	db, p := newBase64TestProvider(t)
	ctx := context.Background()
	assertContentColumnsAreLongtextBin(t, ctx, db)

	content := append([]byte{0xff, 0xfe, 0x00, 0x80}, []byte(fmt.Sprintf("artifact-%d", time.Now().UnixNano()))...)
	digest := store.ContentHash(content)
	key := store.ObjectKeyForDigest(digest)
	if _, err := p.ArtifactObjects().PutObject(ctx, key, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	var encoded string
	if err := db.WithContext(ctx).Raw(
		"SELECT content FROM xflow_artifact_blobs WHERE content_hash = ?", digest,
	).Row().Scan(&encoded); err != nil {
		t.Fatalf("read raw xflow_artifact_blobs.content: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString(content); encoded != want {
		t.Fatalf("raw artifact content = %q, want base64 %q", encoded, want)
	}

	body, object, err := p.ArtifactObjects().GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read artifact body: %v", err)
	}
	if object.Size != int64(len(content)) {
		t.Fatalf("GetObject size = %d, want %d", object.Size, len(content))
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("GetObject content = %x, want %x", got, content)
	}
}

// TestSQLStoreArtifactAtCapacityBase64RoundTrip proves the documented 32 MiB
// max_allowed_packet requirement is sufficient for the largest supported raw
// artifact. The 16 MiB input becomes 22,369,624 bytes of base64 text, so a
// small-artifact round trip cannot detect a packet-size regression.
func TestSQLStoreArtifactAtCapacityBase64RoundTrip(t *testing.T) {
	db, p := newBase64TestProvider(t)
	ctx := context.Background()
	assertContentColumnsAreLongtextBin(t, ctx, db)

	var maxAllowedPacket int64
	if err := db.WithContext(ctx).Raw("SELECT @@max_allowed_packet").Row().Scan(&maxAllowedPacket); err != nil {
		t.Fatalf("read @@max_allowed_packet: %v", err)
	}
	const requiredPacketBytes = 32 << 20
	if maxAllowedPacket < requiredPacketBytes {
		t.Fatalf("@@max_allowed_packet = %d, want at least %d for a %d-byte base64 artifact", maxAllowedPacket, requiredPacketBytes, base64.StdEncoding.EncodedLen(store.MaxArtifactBytes))
	}

	content := make([]byte, store.MaxArtifactBytes)
	for i := range content {
		// Include every byte value so this exercises opaque binary content rather
		// than an ASCII-only special case.
		content[i] = byte(i)
	}
	// xflow_artifact_blobs is content-addressed and immutable, so a fixed payload
	// makes every run after the first collide on the primary key. Stamp a unique
	// prefix instead of trimming the payload: the length must stay exactly
	// MaxArtifactBytes for this test to bound the encoded packet size.
	binary.BigEndian.PutUint64(content[:8], uint64(time.Now().UnixNano()))
	digest := store.ContentHash(content)
	key := store.ObjectKeyForDigest(digest)
	t.Cleanup(func() {
		// Each run stores 22 MiB of base64; without this the shared test database
		// grows by that much per invocation.
		db.Exec("DELETE FROM xflow_artifact_blobs WHERE content_hash = ?", digest)
	})
	if _, err := p.ArtifactObjects().PutObject(ctx, key, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject at MaxArtifactBytes: %v", err)
	}

	var storedLength int64
	if err := db.WithContext(ctx).Raw(
		"SELECT OCTET_LENGTH(content) FROM xflow_artifact_blobs WHERE content_hash = ?", digest,
	).Row().Scan(&storedLength); err != nil {
		t.Fatalf("read encoded artifact length: %v", err)
	}
	if want := int64(base64.StdEncoding.EncodedLen(len(content))); storedLength != want {
		t.Fatalf("stored base64 artifact length = %d, want %d", storedLength, want)
	}

	body, object, err := p.ArtifactObjects().GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject at MaxArtifactBytes: %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read at-capacity artifact body: %v", err)
	}
	if object.Size != int64(len(content)) {
		t.Fatalf("GetObject size = %d, want %d", object.Size, len(content))
	}
	if !bytes.Equal(got, content) {
		t.Fatal("GetObject content differs from the at-capacity input")
	}
}

// TestSQLStoreProviderParameterizesGORMLogs ensures a caller-provided GORM
// logger cannot interpolate base64 supply content into a SQL trace. This also
// guards Debug(), which switches loggers through LogMode.
func TestSQLStoreProviderParameterizesGORMLogs(t *testing.T) {
	dsn := requireMySQL(t)
	var logs bytes.Buffer
	unsafeLogger := gormlogger.New(
		log.New(&logs, "", 0),
		gormlogger.Config{
			LogLevel:             gormlogger.Info,
			Colorful:             false,
			ParameterizedQueries: false,
		},
	)
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{
		TranslateError: true,
		Logger:         unsafeLogger,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	logs.Reset()

	p := sqlstore.New(db)
	if _, ok := p.DB().Logger.(gorm.ParamsFilter); !ok {
		t.Fatal("sqlstore provider logger does not filter bound SQL parameters")
	}
	if _, ok := p.DB().Debug().Logger.(gorm.ParamsFilter); !ok {
		t.Fatal("sqlstore provider debug logger does not filter bound SQL parameters")
	}

	secret := fmt.Sprintf("SQLSTORE_LOG_SECRET_%d", time.Now().UnixNano())
	if _, err := p.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: fmt.Sprintf("log-redaction-%d", time.Now().UnixNano()),
		Name:      "content",
		Content:   []byte(secret),
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}
	if err := p.DB().Debug().Exec("SELECT ?", secret).Error; err != nil {
		t.Fatalf("Debug SELECT: %v", err)
	}

	output := logs.String()
	for _, leaked := range []string{secret, base64.StdEncoding.EncodeToString([]byte(secret))} {
		if strings.Contains(output, leaked) {
			t.Fatal("GORM SQL log leaked bound content")
		}
	}
	if !strings.Contains(output, "?") {
		t.Fatal("GORM SQL log did not retain parameter placeholders")
	}
}

// assertContentColumnsAreLongtextBin checks the live MySQL schema rather than
// relying on GORM tags, whose collation support varies by dialect and version.
func assertContentColumnsAreLongtextBin(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	type column struct {
		TableName     string `gorm:"column:table_name"`
		DataType      string `gorm:"column:data_type"`
		CollationName string `gorm:"column:collation_name"`
		IsNullable    string `gorm:"column:is_nullable"`
	}

	var columns []column
	if err := db.WithContext(ctx).Raw(`
SELECT TABLE_NAME AS table_name, DATA_TYPE AS data_type,
       COLLATION_NAME AS collation_name, IS_NULLABLE AS is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('xflow_supplies', 'xflow_artifact_blobs')
  AND COLUMN_NAME = 'content'
`).Scan(&columns).Error; err != nil {
		t.Fatalf("inspect content column schema: %v", err)
	}

	byTable := make(map[string]column, len(columns))
	for _, column := range columns {
		byTable[column.TableName] = column
	}
	for _, table := range []string{"xflow_supplies", "xflow_artifact_blobs"} {
		column, ok := byTable[table]
		if !ok {
			t.Fatalf("INFORMATION_SCHEMA has no %s.content column", table)
		}
		if column.DataType != "longtext" || column.CollationName != "utf8mb4_bin" || column.IsNullable != "NO" {
			t.Fatalf("%s.content schema = DATA_TYPE %q, COLLATION_NAME %q, IS_NULLABLE %q; want longtext, utf8mb4_bin, NO",
				table, column.DataType, column.CollationName, column.IsNullable)
		}
	}
}
