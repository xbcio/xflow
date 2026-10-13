package memstore_test

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/storetest"
)

func TestScheduleOverrideContract(t *testing.T) {
	storetest.ScheduleOverrideContract(t, memstore.New(), "memsched"+time.Now().Format("150405"))
}
