package agent

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestHarnessCitationModelViewPreservesCurrentEvidenceAndDurableInput(t *testing.T) {
	client := mustClient(t, harnessConfig(t))
	script := `
import sys, asyncio, json
sys.path.insert(0, sys.argv[1])
from model_context import CitationModelView, HISTORICAL_SOURCE
from magent_harness.messages import Message
from magent_harness.retrieval import EvidenceError
old, current = 'a'*32, 'b'*32
class Registry:
    def require(self, ctx, eid):
        assert ctx == 'authenticated context'
        if eid != current: raise EvidenceError('not current evidence')
        return object()
class Model:
    async def complete(self, messages, tools, *, max_tokens=None):
        assert tools == ['tools'] and max_tokens == 11
        return messages
    async def complete_stream(self, messages, tools, *, on_delta, max_tokens=None):
        on_delta('unchanged delta')
        return await self.complete(messages, tools, max_tokens=max_tokens)
model = Model()
view = CitationModelView(model, Registry(), 'authenticated context')
historical = Message(role='assistant', content='历史原文 [E:'+old+']')
tool = Message(role='tool', content=json.dumps({'old':'[E:'+old+']', 'current':'[E:'+current+']'}), tool_call_id='call')
question = Message(role='user', content='请解释字面编号 [E:'+old+']')
checkpoint = Message(role='user', content='摘要 [E:'+old+']', kind='compaction_summary')
conversation = Message(role='tool', content='[historical evidence:'+old+']', tool_call_id='conversation')
messages = [historical, tool, question, checkpoint, conversation]
before = [m.to_dict() for m in messages]
async def check():
    result = await view.complete(messages, ['tools'], max_tokens=11)
    assert result[0].content == '历史原文 '+HISTORICAL_SOURCE
    assert old not in result[1].content and '[E:'+current+']' in result[1].content
    assert result[1].tool_call_id == 'call' and json.loads(result[1].content)
    assert result[2].content == question.content
    assert old not in result[3].content and old not in result[4].content
    deltas = []
    streamed = await view.complete_stream(messages, ['tools'], on_delta=deltas.append, max_tokens=11)
    assert streamed == result and deltas == ['unchanged delta']
    assert before == [m.to_dict() for m in messages]
    summary = CitationModelView(model, Registry(), 'authenticated context', summary=True)
    payload = Message(role='user', content=json.dumps({'conversation':before}))
    summarized = await summary.complete([payload], ['tools'], max_tokens=11)
    assert old not in summarized[0].content and current in summarized[0].content
    assert json.loads(summarized[0].content)
    assert old in payload.content
asyncio.run(check())
`
	cmd := exec.Command(client.config.Python, "-S", "-B", "-c", script, filepath.Dir(client.config.Script))
	cmd.Env = client.env
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("model citation projection: %v %s", err, output)
	}
}
