package httpapi

import "net/http"

type knowledgeBaseInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (s *server) knowledgeBases(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.KnowledgeBases(r.Context(), userID(r), r.PathValue("workspaceID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) knowledgeBase(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.KnowledgeBase(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("knowledgeBaseID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) createKnowledgeBase(w http.ResponseWriter, r *http.Request) {
	var in knowledgeBaseInput
	if !decode(w, r, &in) {
		return
	}
	data, err := s.db.CreateKnowledgeBase(r.Context(), userID(r), r.PathValue("workspaceID"), in.Name, in.Description)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}

func (s *server) updateKnowledgeBase(w http.ResponseWriter, r *http.Request) {
	var in knowledgeBaseInput
	if !decode(w, r, &in) {
		return
	}
	data, err := s.db.UpdateKnowledgeBase(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("knowledgeBaseID"), in.Name, in.Description)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) deleteKnowledgeBase(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteKnowledgeBase(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("knowledgeBaseID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) knowledgeBaseDocuments(w http.ResponseWriter, r *http.Request) {
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	data, total, err := s.db.KnowledgeBaseDocuments(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("knowledgeBaseID"), n, (p-1)*n)
	if err != nil {
		fail(w, err)
		return
	}
	page(w, data, total, p, n)
}

func (s *server) createKnowledgeBaseDocument(w http.ResponseWriter, r *http.Request) {
	in, ok := readDocumentUpload(w, r, false, s.db.UploadLimitBytes())
	if !ok {
		return
	}
	data, err := s.db.CreateKnowledgeBaseDocument(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("knowledgeBaseID"), in.title, in.filename, in.content)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}
