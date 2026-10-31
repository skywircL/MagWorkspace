package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

// Restore bypasses the model display budget and must hydrate all 64 complete
// parents before a resumed/recovered Harness execution can validate citations.
func TestToolTransportRestoresMaximumParentEvidence(t *testing.T) {
	for _, kind := range []string{"unicode", "html"} {
		t.Run(kind, func(t *testing.T) {
			request := retrievalRequest()
			content := strings.Repeat("🙂", domain.ParentChunkRunes)
			if kind == "html" {
				content = strings.Repeat("<>&", domain.ParentChunkRunes/3) + "<"
			}
			hash := sha256.Sum256([]byte(content))
			sourceHash := sha256.Sum256([]byte(strings.Repeat(content, 64)))
			items := make([]domain.Evidence, 64)
			for i := range items {
				items[i] = domain.Evidence{
					ID: fmt.Sprintf("%032x", i+1), RunID: request.RunID, WorkspaceID: request.WorkspaceID,
					DocumentID: strings.Repeat("6", 32), VersionID: strings.Repeat("7", 32), IndexID: strings.Repeat("8", 32),
					Generation: strings.Repeat("9", 32), ChunkID: fmt.Sprintf("%032x", i+100),
					SourceSHA256: hex.EncodeToString(sourceHash[:]), SHA256: hex.EncodeToString(hash[:]),
					Title: strings.Repeat("<", 255), Content: content, StartByte: i * len(content), EndByte: (i + 1) * len(content),
					CreatedAt: time.Date(2026, 10, 21, 1, 2, 3, 999999999, time.UTC),
				}
			}
			response := map[string]any{"evidence": items}
			encoded, err := json.Marshal(response)
			if err != nil || len(encoded) <= 1<<20 || len(encoded) > maxToolResponseBytes {
				t.Fatalf("maximum legal parent fixture does not exercise the expanded budget: bytes=%d err=%v", len(encoded), err)
			}
			t.Logf("full-parent callback response bytes: %d", len(encoded))
			var calls atomic.Int32
			request.ToolHandler = testToolCapabilities(func(ctx context.Context, name string, args json.RawMessage) (any, error) {
				calls.Add(1)
				if ctx.Err() != nil || name != "restore_evidence" || string(args) != "{}" {
					t.Error("unexpected parent evidence restoration request")
				}
				return response, nil
			})
			config := harnessConfig(t)
			bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
			if err != nil {
				t.Fatal(err)
			}
			harnessDir, err := filepath.Abs(filepath.Join(config.HarnessPath, "src"))
			if err != nil {
				t.Fatal(err)
			}
			script := fmt.Sprintf(`import asyncio,hashlib,json,sys
sys.path[:0]=[%q,%q]
from retrieval import Backend,MAX_REQUEST,MAX_RESPONSE
from workspace_tools import WorkspaceTools
from magent_harness.identity import Caller,ResourceScope
from magent_harness.tools import ToolContext
assert MAX_REQUEST==256<<10 and MAX_RESPONSE==2<<20
r=json.load(sys.stdin)
ctx=ToolContext(Caller(r["workspace_id"],r["user_id"],r["role"]),ResourceScope(r["workspace_id"]),r["session_id"],r["run_id"],"")
content=chr(0x1f642)*4096 if %q=="unicode" else "<>&"*1365+"<"
async def check():
 async with Backend(r["retrieval"]) as backend:
  tools=WorkspaceTools(backend)
  await tools.restore(ctx)
  restored=tools.registry.citations(ctx," ".join("[E:"+format(i,"032x")+"]" for i in range(1,65)))
  assert len(restored)==64
  for i,item in enumerate(restored):
   assert item.content==content and item.title=="<"*255
   assert item.start_byte==i*len(content.encode()) and item.end_byte==(i+1)*len(content.encode())
   assert item.sha256==hashlib.sha256(content.encode()).hexdigest()
asyncio.run(check())
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`, bridgeDir, harnessDir, kind)
			config.Script = filepath.Join(t.TempDir(), "restore-parents.py")
			if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
				t.Fatal(err)
			}
			result, err := mustClient(t, config).Run(context.Background(), request)
			if err != nil || result.Answer != "ok" || calls.Load() != 1 {
				t.Fatalf("maximum parent restoration failed: calls=%d result=%+v error=%v", calls.Load(), result, err)
			}
		})
	}
}
