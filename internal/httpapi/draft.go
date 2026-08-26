package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/auth"
)

type createDraftRequest struct {
	Goal string `json:"goal"`
}

type draftRequest struct {
	Draft agent.WorkflowDraft `json:"draft"`
}

type confirmDraftRequest struct {
	Draft       agent.WorkflowDraft `json:"draft"`
	ContentHash string              `json:"content_hash"`
}

// dispatchDraft 处理 Agent 草稿的 HTTP 边界。草稿只在请求体中往返，
// 不写入数据库；服务端每次校验和确认都重新计算内容哈希。
func dispatchDraft(ctx context.Context, w http.ResponseWriter, r *http.Request, deps Dependencies, role auth.Role, requestID string, parts []string) error {
	if len(parts) == 4 && parts[3] == "drafts" {
		if r.Method != http.MethodPost {
			return errMethodNotAllowed
		}
		if role != auth.OperatorRole {
			writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
			return nil
		}
		if deps.Drafts == nil {
			return fmt.Errorf("draft service is unavailable")
		}
		var request createDraftRequest
		if err := decodeJSON(w, r, &request, deps.MaxBodyBytes, true); err != nil {
			return errBadRequest(err)
		}
		if strings.TrimSpace(request.Goal) == "" {
			return errBadRequest(fmt.Errorf("goal is required"))
		}
		draft, err := deps.Drafts.GenerateDraft(ctx, request.Goal)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusCreated, draft)
		return nil
	}
	if len(parts) != 6 || parts[3] != "drafts" || (parts[5] != "validate" && parts[5] != "confirm") {
		return errNotFound
	}
	if r.Method != http.MethodPost {
		return errMethodNotAllowed
	}
	if role != auth.OperatorRole {
		writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
		return nil
	}
	if deps.Drafts == nil {
		return fmt.Errorf("draft service is unavailable")
	}
	draftID, err := url.PathUnescape(parts[4])
	if err != nil || strings.TrimSpace(draftID) == "" {
		return errBadRequest(fmt.Errorf("invalid draft ID"))
	}
	if parts[5] == "validate" {
		var request draftRequest
		if err := decodeJSON(w, r, &request, deps.MaxBodyBytes, true); err != nil {
			return errBadRequest(err)
		}
		if request.Draft.DraftID != draftID {
			return errBadRequest(fmt.Errorf("draft ID does not match path"))
		}
		validated, err := deps.Drafts.ValidateDraft(ctx, request.Draft)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, validated)
		return nil
	}
	var request confirmDraftRequest
	if err := decodeJSON(w, r, &request, deps.MaxBodyBytes, true); err != nil {
		return errBadRequest(err)
	}
	if request.Draft.DraftID != draftID {
		return errBadRequest(fmt.Errorf("draft ID does not match path"))
	}
	definition, err := deps.Drafts.ConfirmDraft(ctx, request.Draft, request.ContentHash)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, definition)
	return nil
}

var _ DraftService = (*agent.Service)(nil)
