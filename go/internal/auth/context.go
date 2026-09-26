package auth

import "context"

type userCtxKey struct{}

// WithUser кладёт пользователя в контекст.
func WithUser(ctx context.Context, u *User) context.Context {
	return context.WithValue(ctx, userCtxKey{}, u)
}

// UserFromContext достаёт пользователя из контекста.
func UserFromContext(ctx context.Context) (*User, bool) {
	u, ok := ctx.Value(userCtxKey{}).(*User)
	return u, ok
}

// MustUserFromContext возвращает пользователя или nil.
// Используется в местах, где middleware гарантировал аутентификацию.
func MustUserFromContext(ctx context.Context) *User {
	u, _ := UserFromContext(ctx)
	return u
}
