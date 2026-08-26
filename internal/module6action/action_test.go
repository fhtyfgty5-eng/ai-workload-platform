package module6action

import "testing"

func TestRunNormalizesLiteralText(t *testing.T) {
	output, err := Run("document.normalize", map[string]any{"source": "  hello   world\n"})
	if err != nil || output != "hello world" {
		t.Fatalf("Run() = %q, %v; want normalized text", output, err)
	}
}

func TestRunSummarizesWithWordLimit(t *testing.T) {
	output, err := Run("document.summarize", map[string]any{"source": "one two three four", "max_words": float64(2)})
	if err != nil || output != "one two" {
		t.Fatalf("Run() = %q, %v; want two words", output, err)
	}
}

func TestRunRejectsUnknownAction(t *testing.T) {
	if _, err := Run("sh -c id", nil); err == nil {
		t.Fatal("Run() error = nil, want unknown action rejection")
	}
}
