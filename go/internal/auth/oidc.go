package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Provider — OIDC-провайдер с готовым верификатором токенов.
type Provider struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// NewProvider делает OIDC discovery и создаёт верификатор.
// audience — ожидаемое значение в claim "aud".
func NewProvider(ctx context.Context, issuerURL, audience string) (*Provider, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	p, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery %q: %w", issuerURL, err)
	}

	verifier := p.Verifier(&oidc.Config{
		ClientID: audience,
	})

	return &Provider{provider: p, verifier: verifier}, nil
}

// Verify проверяет подпись, exp, iss, aud токена и возвращает IDToken.
func (p *Provider) Verify(ctx context.Context, rawIDToken string) (*oidc.IDToken, error) {
	return p.verifier.Verify(ctx, rawIDToken)
}
