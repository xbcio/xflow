package apiserver

import (
	"net/http"
)

// currentUserResponse is the caller's own verified identity.
//
// Every field is server-issued: the subject the authenticator verified, the
// namespace that credential is bound to, and the scopes it was issued. Nothing
// here is read from the request, so a caller cannot use this endpoint to
// discover or claim another principal's identity — there is no parameter that
// names one.
//
// The field names are the server's vocabulary (subject/namespace/scopes), not
// a UI's. A console that wants different names maps them on its side, so the
// contract does not have to be renamed the next time a surface does.
type currentUserResponse struct {
	Subject   string   `json:"subject"`
	Namespace string   `json:"namespace"`
	Scopes    []string `json:"scopes"`
}

// handleCurrentUser serves GET /v1/current-user: who the presented credential
// is, as the server resolved it.
//
// It exists because a console has to know what it may show before it can render
// anything, and the alternative — probing routes and inferring capability from
// which ones 403 — both guesses wrong on routes that 404 and spends an audited
// mutation-shaped round trip per guess on the ones that do not.
//
// Registered on the principal-auth path only: the response is a VERIFIED
// subject, and a server configured without a PrincipalAuthenticator has none to
// report. Returning an invented one there would be worse than not answering.
func (m *workflowControlModule) handleCurrentUser(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || principal.Subject == "" {
		// Reached when the wrapper admitted a request but no principal was
		// attached — a custom PrincipalAuthenticator that returned an empty
		// subject. Reporting an empty identity would be read by a caller as
		// "authenticated as nobody"; refuse instead, like every other
		// unauthenticated answer on this API.
		writeFail(w, r, http.StatusUnauthorized, "unauthenticated", "unauthorized")
		return
	}
	scopes := principal.Scopes
	if scopes == nil {
		// JSON null would reach a client as "no scope list at all" rather than
		// "no scopes"; the empty list is the truthful encoding and the one a
		// consumer can iterate without a null guard.
		scopes = []string{}
	}
	writeData(w, r, http.StatusOK, currentUserResponse{
		Subject:   principal.Subject,
		Namespace: principal.Namespace,
		Scopes:    scopes,
	})
}
