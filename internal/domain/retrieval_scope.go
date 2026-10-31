package domain

// RetrievalScope is a backend-owned, immutable upper bound for one Run.
// Each nil dimension is unrestricted; each non-nil empty dimension selects none.
// Knowledge-base and document selections intersect, rather than widening one another.
type RetrievalScope struct {
	DocumentIDs      []string
	KnowledgeBaseIDs []string
}
