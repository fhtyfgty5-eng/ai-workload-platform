package agentapp

import (
	"context"
	"testing"
)

func TestNewServiceDefaultsToMockModel(t *testing.T) {
	service, err := NewService("", func(string) string { return "" })
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	draft, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatalf("GenerateDraft() error = %v", err)
	}
	if draft.Definition.ID == "" || draft.DraftID == "" {
		t.Fatalf("draft = %#v, want generated IDs", draft)
	}
}

func TestNewServiceRejectsUnknownModel(t *testing.T) {
	if _, err := NewService("unknown", func(string) string { return "" }); err == nil {
		t.Fatal("NewService() error = nil, want unsupported model error")
	}
}
